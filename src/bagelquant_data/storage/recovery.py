"""Immutable Parquet segment evidence, audits and explicit backup recovery."""
from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, cast

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from bagelquant_data.core.schema import concat_compatible_frames
from bagelquant_data.storage.data_meta import DataMetaStore
from bagelquant_data.storage.paths import LakePaths

if TYPE_CHECKING:
    from bagelquant_data.storage.parquet import ParquetStore


def _file_hash(path: Path, check_canceled: Callable[[], None] | None = None) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while piece := stream.read(1024 * 1024):
            if check_canceled is not None:
                check_canceled()
            digest.update(piece)
    return digest.hexdigest()


def register_segment(data_meta: DataMetaStore, partition: str, seq: int, frame: pl.DataFrame,
                     path: Path, manifest: dict, *, changed_fields: dict | None = None) -> dict:
    """Register one prepared original file, never a second body or row index."""
    from bagelquant_data import input_index
    data_meta.ensure_writable()
    from bagelquant_data.core.hashing import frame_content_hash
    from bagelquant_data.pipeline.versions import VERSION_FIELDS
    fields = sorted(set(frame.columns)-VERSION_FIELDS-{"time","year","month","source_time","asset_id","_dependency_digest"})
    changes = {}
    file_hash = _file_hash(path)
    shared = {"lower":None if "source_time" not in frame.columns or not frame.height else str(frame["source_time"].min()),
        "upper":None if "source_time" not in frame.columns or not frame.height else str(frame["source_time"].max()),
        "asset_lower":None if "asset_id" not in frame.columns or not frame.height else frame["asset_id"].min(),
        "asset_upper":None if "asset_id" not in frame.columns or not frame.height else frame["asset_id"].max()}
    general = "source_time" not in frame.columns
    if general:
        fields = sorted(set(frame.columns)-VERSION_FIELDS-{"year","month"})
    general_keys = [name for name in ("time","asset_id") if name in frame.columns]
    general_unique = frame.height <= 1 or bool(general_keys) and frame.select(general_keys).n_unique() == frame.height
    if changed_fields is None and not general:
        changes["$initial"] = {**shared,"fields":fields,"identity":file_hash,"timing_identity":file_hash}
    for field in fields:
        if general:
            projection = list(dict.fromkeys([*general_keys,field]))
            changes[field] = {**shared,"identity":frame_content_hash(frame.select(projection)) if general_unique else file_hash,
                "projection":projection if general_unique else None,"timing_identity":None}
            continue
        if changed_fields is None or field not in changed_fields:
            continue
        value_bounds,timing_bounds = changed_fields[field]["value"],changed_fields[field]["timing"]
        lower,upper = value_bounds or timing_bounds
        changes[field] = {**shared,"lower":lower,"upper":upper,
            "identity":None if value_bounds is None else file_hash,
            "timing_identity":None if timing_bounds is None else file_hash}
    definition = data_meta.get_dataset(manifest["source"],manifest["dataset"])
    key_fields = ["source_time","asset_id",*json.loads(definition["spec_json"]).get("primary_key_extra",())] if definition is not None else ["source_time","asset_id"]
    key_identity = frame_content_hash(frame.select([key for key in key_fields if key in frame.columns])) if "source_time" in frame.columns else ""
    batch = {"key_identity":key_identity,"field_changes_json":json.dumps(changes,sort_keys=True),"partition_path": partition, "content_hash": file_hash, "row_count": frame.height,
        "schema_ipc": frame.to_arrow().schema.serialize().to_pybytes(),
        "generation_path": manifest["generation_path"], "file_size_bytes": path.stat().st_size,
        "bounds_json": json.dumps({**input_index.bounds(frame),"keys":key_fields}, sort_keys=True),
        **{key: (str(getattr(frame[axis], op)()) if axis in frame.columns and frame.height else None)
           for key,axis,op in (("min_available","time","min"),("max_available","time","max"),
                              ("min_observation","source_time","min"),("max_observation","source_time","max"))}}
    with data_meta.connect() as db:
        db.execute("begin immediate")
        commit = db.execute("select status from version_commits where seq=?", (seq,)).fetchone()
        if commit is None or commit[0] != "prepared":
            raise RuntimeError("Segment evidence requires a prepared commit")
        from .immutable_metadata import save_schema
        batch["schema_ref"] = save_schema(db, batch["schema_ipc"])
        fields = ("partition_path","content_hash","row_count","schema_ref","generation_path", "file_size_bytes", "bounds_json", "field_changes_json", "key_identity",
                  "min_available","max_available","min_observation","max_observation")
        db.execute("insert into version_segments(commit_seq," + ",".join(fields) + ") values(" + ",".join("?" for _ in range(14)) + ")",
            (seq, *(batch[field] for field in fields)))
    return batch


def _registered(data_meta: DataMetaStore, partition: str, seq: int, digest: str) -> tuple[dict, Path]:
    rows = data_meta._rows("select b.*,c.source,c.dataset from version_segments b join version_commits c on c.seq=b.commit_seq "
        "where b.commit_seq=? and b.partition_path=? and c.status='committed'", (seq,partition))
    if not rows or rows[0]["content_hash"] != digest:
        raise RuntimeError(f"Original Parquet segment {seq} is missing or its registered checksum differs")
    location = data_meta._rows("select value from data_meta_state where key='lake_location'")[0]["value"]
    paths = LakePaths.open(data_meta_path=data_meta.data_meta_path, lake_path=data_meta.data_meta_path.parent / location)
    row = rows[0]
    return row, paths.generation_path(row["source"], row["dataset"], partition, row["generation_path"])


def read_batch(data_meta: DataMetaStore, partition: str, seq: int, digest: str, *,
               check_canceled: Callable[[], None] | None = None, verify: bool = False,
               observation_start: date | None = None, observation_end: date | None = None) -> pl.DataFrame:
    """Consume exact original Parquet; explicit audit verifies every original byte."""
    row,path = _registered(data_meta,partition,seq,digest)
    if check_canceled is not None:
        check_canceled()
    if verify and _file_hash(path,check_canceled) != digest:
        raise RuntimeError(f"Original Parquet segment {seq} checksum mismatch; restore a trusted backup")
    schema = pl.Schema(pa.ipc.read_schema(pa.BufferReader(row["schema_ipc"])))
    bounded = "source_time" in schema and (observation_start is not None or observation_end is not None)
    if bounded:
        # Validate the complete original's physical authority before selecting
        # observations. Its footer row count is not the selected row count.
        with pq.ParquetFile(path) as reader:
            if pl.Schema(reader.schema_arrow) != schema or reader.metadata.num_rows != row["row_count"]:
                raise RuntimeError(f"Original Parquet segment {seq} typed schema/row count mismatch")
        scan = pl.scan_parquet(path, hive_partitioning=False)
        if observation_start is not None:
            scan = scan.filter(pl.col("source_time") >= observation_start)
        if observation_end is not None:
            scan = scan.filter(pl.col("source_time") <= observation_end)
        frame = scan.collect()
    else:
        frame = pl.read_parquet(path, hive_partitioning=False)
    if frame.schema != schema or not bounded and frame.height != row["row_count"]:
        raise RuntimeError(f"Original Parquet segment {seq} typed schema/row count mismatch")
    return with_record_key(frame,keys=json.loads(row["bounds_json"])["keys"])


def verify_batch(data_meta: DataMetaStore, partition: str, seq: int, digest: str, *,
                 buffer_bytes: int = 1024 * 1024, check_canceled: Callable[[], None] | None = None) -> None:
    project_batch(data_meta,partition,seq,digest,columns=(), consume=lambda _: None,
        buffer_bytes=buffer_bytes,check_canceled=check_canceled)
    row,path = _registered(data_meta,partition,seq,digest)
    changes = json.loads(row["field_changes_json"])
    from bagelquant_data.core.hashing import frame_content_hash
    for field,change in changes.items():
        if change.get("projection") is None:
            if any(change.get(name) is not None and change[name] != digest for name in ("identity","timing_identity")):
                raise RuntimeError("Segment field change identity differs from original file evidence")
            continue
        pieces,retained = [],0
        with pq.ParquetFile(path) as reader:
            for batch in reader.iter_batches(batch_size=max(1,min(65536,buffer_bytes//256)),columns=change["projection"]):
                if check_canceled is not None:
                    check_canceled()
                piece = cast(pl.DataFrame,pl.from_arrow(batch,rechunk=False))
                retained += piece.estimated_size()
                if retained*8 > buffer_bytes:
                    raise MemoryError("General field change audit exceeds max_buffer_bytes")
                pieces.append(piece)
        original = pl.concat(pieces) if pieces else pl.read_parquet(path,columns=change["projection"],hive_partitioning=False)
        if frame_content_hash(original) != change["identity"]:
            raise RuntimeError(f"Segment field change identity for {field} differs from original evidence")


def project_batch(data_meta: DataMetaStore, partition: str, seq: int, digest: str, *,
                  columns: tuple[str,...], consume: Callable[[pl.DataFrame],None],
                  buffer_bytes: int, check_canceled: Callable[[],None] | None = None) -> None:
    """Explicit audit/projection visits bounded typed chunks of the sole original."""
    row,path = _registered(data_meta,partition,seq,digest)
    if _file_hash(path,check_canceled) != digest:
        raise RuntimeError(f"Original Parquet segment {seq} checksum mismatch; restore a trusted backup")
    from bagelquant_data import input_index
    expected_bounds = json.loads(row["bounds_json"])
    audited_bounds: dict = {"record":False,"lower":None,"upper":None,"available":None,"max_availability_delay_days":None}
    with pq.ParquetFile(path) as reader:
        schema = pa.ipc.read_schema(pa.BufferReader(row["schema_ipc"]))
        if pl.Schema(reader.schema_arrow) != pl.Schema(schema) or reader.metadata.num_rows != row["row_count"]:
            raise RuntimeError(f"Original Parquet segment {seq} typed schema/row count mismatch")
        # Validate full original columns even when callers consume a narrow projection.
        rows = max(1,min(65536,buffer_bytes // max(256,len(schema)*32)))
        for batch in reader.iter_batches(batch_size=rows):
            if check_canceled is not None:
                check_canceled()
            batch.validate(full=True)
            original = cast(pl.DataFrame,pl.from_arrow(batch,rechunk=False))
            actual = input_index.bounds(original)
            audited_bounds["record"] = actual["record"]
            for field,reduce in (("lower",min),("upper",max),("available",min),("max_availability_delay_days",max)):
                if actual[field] is not None:
                    previous = audited_bounds[field]
                    audited_bounds[field] = actual[field] if previous is None else reduce(previous,actual[field])
            original = with_record_key(original,keys=expected_bounds["keys"])
            if "_payload_hash" in columns and "source_time" in original.columns:
                from bagelquant_data.pipeline.versions import _payload_hashes, VERSION_FIELDS
                fields = sorted(set(original.columns)-VERSION_FIELDS-{"time","year","month","_dependency_digest"})
                original = original.with_columns(_payload_hashes(original,fields))
            projected = original.select([name for name in columns if name in original.columns])
            if projected.width:
                consume(projected)
        if not reader.metadata.num_rows:
            audited_bounds["record"] = "source_time" in schema.names
    if any(audited_bounds[field] != expected_bounds[field] for field in audited_bounds):
        raise RuntimeError(f"Original Parquet segment {seq} bounds differ from original evidence")

def registered_batches(parquet: ParquetStore, source: str, dataset: str, partition: str) -> list[dict]:
    return parquet.metadata._rows("select b.* from version_segments b join version_commits c on c.seq=b.commit_seq "
        "where c.source=? and c.dataset=? and c.status='committed' and b.partition_path=? order by b.commit_seq", (source,dataset,partition))


def reconstruct(parquet: ParquetStore, source: str, dataset: str, partition: str) -> pl.DataFrame:
    """Read retained history; no secondary copy exists for repairing lost originals."""
    batches = registered_batches(parquet,source,dataset,partition)
    if not batches:
        raise RuntimeError("No registered original segments; restore a trusted backup")
    return sort_versions(concat_compatible_frames([read_batch(parquet.metadata,partition,int(batch["commit_seq"]),
        batch["content_hash"],verify=True) for batch in batches]))


def sort_versions(frame: pl.DataFrame) -> pl.DataFrame:
    fields = [name for name in ("time","source_time","asset_id","_record_id","_commit_seq") if name in frame.columns]
    return frame.sort(fields) if fields else frame


def repair_partition(parquet: ParquetStore, source: str, dataset: str, partition: str) -> dict:
    parquet.metadata.ensure_writable()
    reconstruct(parquet,source,dataset,partition)
    return {"partition":partition,"rows":sum(batch["row_count"] for batch in registered_batches(parquet,source,dataset,partition)),
            "method":"original_segment_audit","content_changed":False}


def inspect_partition(parquet: ParquetStore, source: str, dataset: str, partition: str, *, deep: bool = False) -> dict:
    batches = registered_batches(parquet,source,dataset,partition)
    result = {"partition":partition,"registered_batches":len(batches),"recovery_state":"backup_required","error":None}
    try:
        for batch in batches:
            _, path = _registered(parquet.metadata,partition,int(batch["commit_seq"]),batch["content_hash"])
            if not path.is_file():
                raise RuntimeError("Original segment is missing; restore a trusted backup")
            if deep:
                verify_batch(parquet.metadata,partition,int(batch["commit_seq"]),batch["content_hash"])
        result["recovery_state"] = "originals_present"
    except (OSError,RuntimeError,pl.exceptions.PolarsError) as error:
        result["error"] = str(error)
    return result


def verify_visibility(data_meta: DataMetaStore, event: dict, *,
                      buffer_bytes: int, check_canceled: Callable[[],None] | None = None) -> None:
    """Audit the exact retained first-visibility stream independently of content."""
    location = data_meta._rows("select value from data_meta_state where key='lake_location'")[0]["value"]
    paths = LakePaths.open(data_meta_path=data_meta.data_meta_path,lake_path=data_meta.data_meta_path.parent / location)
    base = paths.dataset_root(event["source"],event["dataset"])
    path = (base / event["generation_path"]).resolve()
    if not path.is_relative_to(base) or _file_hash(path,check_canceled) != event["content_hash"]:
        raise RuntimeError("Original visibility event checksum mismatch; restore a trusted backup")
    with pq.ParquetFile(path) as reader:
        if reader.metadata.num_rows != event["row_count"]:
            raise RuntimeError("Original visibility event row count mismatch")
        for batch in reader.iter_batches(batch_size=max(1,min(65536,buffer_bytes // 256))):
            if check_canceled is not None:
                check_canceled()
            batch.validate(full=True)

def with_record_key(frame: pl.DataFrame, *, keys: list[str] | None = None) -> pl.DataFrame:
    """Expose a virtual natural observation key without storing a row proof hash."""
    if "source_time" not in frame.columns or "_record_id" in frame.columns:
        return frame
    # Raw extra keys remain present in the original schema. The canonical
    # producer/query caller selects the exact declared compound key when needed.
    from bagelquant_data.pipeline.versions import _payload_hashes
    return frame.with_columns(_payload_hashes(frame,keys or ["source_time","asset_id"]).rename("_record_id"))
