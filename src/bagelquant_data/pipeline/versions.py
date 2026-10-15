"""Commit availability-dated records and their durable recovery evidence."""

from __future__ import annotations

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast
from typing import Callable, ParamSpec, TypeVar
from functools import wraps
from zoneinfo import ZoneInfo

import polars as pl
from polars.datatypes import DataTypeClass

from bagelquant_data.core.dataset import DatasetSpec, record_key
from bagelquant_data.core.types import DateLike
from bagelquant_data.core.schema import (
    align_frame,
    compatible_schema,
)
from bagelquant_data.storage.parquet import (
    ParquetStore,
    rollback_partition_writes,
    partition_write_context,
)
from bagelquant_data.storage.recovery import sort_versions
from bagelquant_data.storage.data_meta import _insert_version_check


VERSION_FIELDS = frozenset(
    {
        "ingested_at",
        "_commit_seq",
        "_record_id",
        "_payload_hash",
        "_snapshot_id",
        "snapshot_date",
        "_baseline",
        "_attestation_id",
    }
)

def _payload_value(value: object) -> object:
    """Encode logical scalars without JSON's null/nonfinite conflation."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return {"float": value.hex()}
    if isinstance(value, Decimal):
        return {"decimal": str(value)}
    if isinstance(value, datetime):
        return {"datetime": value.isoformat()}
    if isinstance(value, date):
        return {"date": value.isoformat()}
    if isinstance(value, time):
        return {"time": value.isoformat()}
    if isinstance(value, timedelta):
        return {"duration_us": (value.days * 86_400 + value.seconds) * 1_000_000 + value.microseconds}
    if isinstance(value, bytes):
        return {"binary": value.hex()}
    if isinstance(value, (list, tuple)):
        return [_payload_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _payload_value(item) for key, item in sorted(value.items())}
    raise TypeError(f"Unsupported persisted payload scalar: {type(value).__name__}")


def _payload_expression(expression: pl.Expr, dtype: pl.DataType | DataTypeClass) -> pl.Expr:
    """Preserve native temporal precision before crossing into Python rows."""
    if dtype.is_temporal():
        return expression.cast(pl.Int64)
    if isinstance(dtype, pl.List):
        return expression.list.eval(_payload_expression(pl.element(), dtype.inner))
    if isinstance(dtype, pl.Array):
        return expression.arr.to_list().list.eval(_payload_expression(pl.element(), dtype.inner))
    if isinstance(dtype, pl.Struct):
        fields = [_payload_expression(expression.struct.field(field.name), field.dtype).alias(field.name)
                  for field in dtype.fields]
        return pl.when(expression.is_null()).then(None).otherwise(pl.struct(fields))
    return expression


def _payload_encoder(dtype: pl.DataType | DataTypeClass,
                     cache_budget: list[int]) -> Callable[[object], bytes]:
    """Encode one scalar, sharing a conservative bounded string-token reserve."""
    def generic(value: object) -> bytes:
        return json.dumps(_payload_value(value), sort_keys=True,
                          separators=(",", ":")).encode()

    if dtype == pl.String:
        tokens: dict[object, bytes] = {}
        column_bytes = 0

        def string_token(value: object) -> bytes:
            nonlocal column_bytes
            cached = tokens.get(value)
            if cached is not None:
                return cached
            encoded = generic(value)
            # Reserve dictionary, Python key/value and encoded-token storage.
            # Build lazily from the bounded row iterator: never collect uniques.
            reserve = 512 + 2 * len(encoded)
            if reserve <= min(1024**2 - column_bytes, cache_budget[0]):
                tokens[value] = encoded
                column_bytes += reserve
                cache_budget[0] -= reserve
            return encoded

        return string_token
    if dtype.is_integer():
        return lambda value: b"null" if value is None else str(value).encode()
    if dtype.is_float():
        return lambda value: (b"null" if value is None else
                              b'{"float":"' + cast(float, value).hex().encode() + b'"}')
    if dtype == pl.Boolean:
        return lambda value: b"null" if value is None else b"true" if value else b"false"
    if dtype == pl.Null:
        return lambda value: b"null"
    return generic


def _payload_hashes(frame: pl.DataFrame, fields: list[str]) -> pl.Series:
    """Hash the unchanged typed-row-v1 bytes with bounded scalar token reuse.

    Schema and temporal precision remain part of identity; floating hex retains
    signed zero, infinities and NaN/null distinction. Nested values retain the
    recursive JSON representation. String caches share a conservative 4 MiB
    reserve (at most 1 MiB per column), further bounded by input-frame bytes,
    with no unique-value materialization.
    """
    selected = frame.select(fields)
    header = json.dumps([(name, str(dtype)) for name, dtype in selected.schema.items()],
                        separators=(",", ":")).encode()
    selected = selected.select(_payload_expression(pl.col(name), dtype).alias(name)
                               for name, dtype in selected.schema.items())
    cache_budget = [min(4 * 1024**2, int(selected.estimated_size()))]
    encoders = [_payload_encoder(dtype, cache_budget) for dtype in selected.schema.values()]
    base = hashlib.sha256(b"typed-row-v1\0" + header + b"\0")
    hashes = []
    for row in selected.iter_rows():
        digest = base.copy()
        digest.update(b"[" + b",".join(encode(value) for encode, value in zip(encoders, row)) + b"]")
        hashes.append(digest.hexdigest())
    return pl.Series("_payload_hash", hashes, dtype=pl.String)


_P = ParamSpec("_P")
_R = TypeVar("_R")


def _leased(function: Callable[_P, _R]) -> Callable[_P, _R]:
    @wraps(function)
    def guarded(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        spec = cast(DatasetSpec, args[0])
        parquet = cast(ParquetStore, args[2])
        with parquet.metadata.dataset_writer(spec.source, spec.name, str(kwargs["run_id"])):
            return function(*args, **kwargs)
    return guarded


@_leased
def commit_versions(
    spec: DatasetSpec,
    frame: pl.DataFrame,
    parquet: ParquetStore,
    *,
    run_id: str,
    mode: str = "incremental",
    ingested_at: datetime | None = None,
    requests: list[dict] | None = None,
    writer_executor: ThreadPoolExecutor | None = None,
    available_date: DateLike | None = None,
    preserve_available: bool = False,
    preserve_semantic_timing: bool = False,
    historical_baseline: bool | None = None,
    scope_transitions: list[dict] | None = None,
    partition_workers: int = 1,
    input_receipt_id: str | None = None,
    _on_unchanged_check: Callable | None = None,
):
    from bagelquant_data.pipeline.commit import CommitResult

    parquet.metadata.ensure_writable()
    parquet.metadata.assert_definition(spec)
    if isinstance(partition_workers, bool) or not isinstance(partition_workers, int) or partition_workers < 1:
        raise ValueError("partition_workers must be a positive integer")
    if mode not in {"initialize", "incremental", "refresh"}:
        raise ValueError("mode must be initialize, incremental, or refresh")
    now = ingested_at or datetime.now(UTC)
    if now.tzinfo is None:
        raise ValueError("ingested_at must be timezone-aware")
    now = now.astimezone(UTC)
    local = now.astimezone(ZoneInfo(spec.availability_timezone))
    # The boundary is exclusive: receipt at/after the decision cutoff belongs
    # to the next date. A negative offset without a cutoff has no causal meaning.
    if spec.availability_day_offset < 0 and spec.availability_cutoff_time is None:
        raise ValueError("negative availability_day_offset requires an explicit cutoff time")
    rollover = (
        spec.availability_cutoff_time is not None
        and local.time() >= time.fromisoformat(spec.availability_cutoff_time)
    )
    available = local.date() + timedelta(
        days=spec.availability_day_offset + int(rollover)
    )
    if available_date is not None:
        from bagelquant_data.query.raw import _date_value
        available = _date_value(available_date)
    baseline = mode == "initialize" if historical_baseline is None else historical_baseline
    root = parquet.paths.dataset_root(spec.source, spec.name)
    manifests = parquet.metadata.manifest(spec.source, spec.name)
    parent = {str(row["generation_path"]): str(row["generation_path"]) for row in manifests}
    definition_hash = parquet.metadata.dataset_spec_hash(spec.source, spec.name)
    if mode == "initialize":
        initialization = parquet.metadata._rows(
            "select status from dataset_initializations where source=? and dataset=?",
            (spec.source, spec.name),
        )
        if initialization and initialization[0]["status"] != "running":
            raise ValueError("Historical initialization is already complete")
        previous = parquet.metadata._rows(
            "select mode,run_id from version_commits where source=? and dataset=? and status='committed'",
            (spec.source, spec.name),
        )
        if previous and not initialization:
            raise ValueError("Historical initialization requires a new dataset")
        if any(row["mode"] != "initialize" for row in previous):
            raise ValueError(
                "Historical initialization cannot backdate an existing incremental dataset"
            )
    reserved = VERSION_FIELDS.intersection(frame.columns)
    if reserved:
        raise ValueError(
            f"Provider input contains reserved version columns: {sorted(reserved)}"
        )
    source_times = (
        frozenset(str(v) for v in frame["source_time"].unique())
        if "source_time" in frame.columns
        else frozenset()
    )
    attestations = pl.DataFrame()
    content_scopes = []
    checked_rows = frame.height
    changed_fields = None
    if spec.update_type == "by_date":
        keys = list(record_key(spec))
        frame = frame.with_columns(_payload_hashes(frame,keys).rename("_record_id"))
        payload = sorted(set(frame.columns) - {"time", "year", "month", "_record_id", "_dependency_digest"})
        if preserve_semantic_timing:
            payload.append("time")
        frame = frame.with_columns(_payload_hashes(frame, payload))
        conflicts = (
            frame.group_by("_record_id")
            .agg(pl.col("_payload_hash").n_unique())
            .filter(pl.col("_payload_hash") > 1)
        )
        if conflicts.height:
            conflict_id = str(
                conflicts.sort("_record_id")["_record_id"].item(0)
            )
            conflict_rows = frame.filter(pl.col("_record_id") == conflict_id)
            differing_fields = [
                name
                for name in payload
                if conflict_rows[name].n_unique() > 1
            ]
            raise ValueError(
                "Conflicting rows for a single observation in one provider response: "
                f"record_id={conflict_id}; row_count={conflict_rows.height}; "
                f"differing_fields={differing_fields}"
            )
        frame = frame.unique("_record_id", maintain_order=True)
        frame = frame.with_columns(
            (
                pl.max_horizontal(pl.col("source_time"), pl.col("time"))
                if preserve_available
                else pl.col("source_time") if mode == "initialize"
                else pl.max_horizontal(pl.col("source_time"), pl.lit(available))
            ).alias("time")
        )
        if manifests and frame.height:
            from bagelquant_data.query.raw import RawQueryService

            observation_start = cast(DateLike, frame["source_time"].min())
            observation_end = cast(DateLike, frame["source_time"].max())
            old = (
                RawQueryService(parquet, parquet.metadata)
                .query(
                    spec.name,
                    source=spec.source,
                    observation_start=observation_start,
                    observation_end=observation_end,
                    fields=None,
                )
                .collect()
            )
            if old.height:
                old = old.drop("_payload_hash") if "_payload_hash" in old.columns else old
                common_schema = compatible_schema([frame.select(payload).schema,
                    old.select([field for field in payload if field in old.columns]).schema])
                old = old.with_columns((pl.col(field).cast(common_schema[field]) if field in old.columns
                    else pl.lit(None,dtype=common_schema[field])).alias(field) for field in payload)
                frame = frame.with_columns(pl.col(field).cast(common_schema[field]) for field in payload)
                frame = frame.with_columns(_payload_hashes(frame,payload))
                old = old.with_columns(_payload_hashes(old,payload))
                if spec.source == "items" and input_receipt_id is not None:
                    from bagelquant_data.core.hashing import frame_content_hash
                    matched = frame.join(old.select("_record_id","_payload_hash",pl.col("_commit_seq").alias("_checked_commit")),
                        on=["_record_id","_payload_hash"],how="inner")
                    for (checked_commit,),checked in matched.partition_by("_checked_commit",as_dict=True).items():
                        content_scopes.append({"commit_seq":checked_commit,"key_identity":frame_content_hash(checked.select(keys)),"row_count":checked.height})
                changed_fields = {}
                joined = frame.join(old.select("_record_id", pl.col("time").alias("_previous_availability"), *[pl.col(name).alias("_old_"+name) for name in payload]).with_columns(pl.lit(True).alias("_old_exists")),
                    on="_record_id",how="left")
                timing_changed = joined.filter(pl.col("_old_exists").is_null() | ~pl.col("time").eq_missing(pl.col("_previous_availability")))
                for field in payload:
                    equal = pl.col(field).eq_missing(pl.col("_old_"+field))
                    if common_schema[field].is_float():
                        equal = equal & pl.col(field).cast(pl.String).eq_missing(pl.col("_old_"+field).cast(pl.String))
                    changed = joined.filter(pl.col("_old_exists").is_null() | ~equal)
                    if changed.height or timing_changed.height:
                        changed_fields[field] = {"value":None if not changed.height else
                            (str(changed["source_time"].min()),str(changed["source_time"].max())),
                            "timing":None if not timing_changed.height else
                            (str(timing_changed["source_time"].min()),str(timing_changed["source_time"].max()))}
                if joined["_old_exists"].null_count() == joined.height:
                    changed_fields = None  # Shared initial/all-new coordinate evidence.
                # Unchanged values retain their content version. An immutable
                # check witnesses that exact version at this collection date.
                # Match only typed identities to the original creator. Joining
                # the full old schema invents suffixed coordinate/payload fields
                # and collides with legitimate channels such as time_right.
                attestation_fields = list(dict.fromkeys(["_record_id", "_payload_hash", "time", *keys]))
                attestations = (
                    frame.select(attestation_fields)
                    .join(
                        old.filter(pl.col("_baseline")).select("_record_id", "_payload_hash", "_commit_seq"),
                        on=["_record_id", "_payload_hash"], how="inner",
                    )
                    .select("_commit_seq", "time", *keys)
                )
                frame = frame.join(old.select("_record_id", "_payload_hash"), on=["_record_id", "_payload_hash"], how="anti")
        if frame.is_empty():
            # Schema-only acquisitions and typed empty acquisitions retain the
            # declared channels without duplicating identical historical rows.
            typed = frame.drop([name for name in ("_record_id","_payload_hash") if name in frame.columns]).with_columns(
                pl.lit(now, dtype=pl.Datetime("us", "UTC")).alias("ingested_at"),
                pl.lit(None, dtype=pl.Int64).alias("_commit_seq"),
                pl.lit(baseline).alias("_baseline"),
            )
            stored_schema = parquet.canonical_schema(spec.source, spec.name)
            canonical = compatible_schema([*([stored_schema] if stored_schema is not None else []), typed.schema])
            empty_schema = partition_write_context(canonical)
            visibility_path = None
            try:
                with parquet.metadata.connect() as db:
                    db.execute("begin immediate")
                    parquet.metadata.assert_dataset_parent(db, spec.source, spec.name, parent, definition_hash, run_id)
                    check_id = _record_check(db, spec, run_id, now, available, baseline, requests, checked_rows, attestations, input_receipt_id=input_receipt_id,content_scopes=content_scopes)
                    event = db.execute("select generation_path from visibility_segments where check_id=?", (check_id,)).fetchone()
                    if event is not None:
                        visibility_path = root / event[0]
                    parquet.metadata._write_dataset_schema(db, spec.source, spec.name,
                        empty_schema.schema_ipc, empty_schema.schema_hash, now.isoformat())
                    if scope_transitions:
                        parquet.metadata._transition_scopes(db, scope_transitions, run_id=run_id)
                    if _on_unchanged_check is not None:
                        _on_unchanged_check(db, check_id)
            except BaseException:
                if visibility_path is not None:
                    visibility_path.unlink(missing_ok=True)
                raise
            return CommitResult(0, 0, len({value[:7] for value in source_times}), 0, present_times=source_times)
    else:
        frame = frame.unique(maintain_order=True)
        if manifests:
            from bagelquant_data.core.hashing import frame_content_hash
            from bagelquant_data.query.raw import RawQueryService

            current_spec_hash = parquet.metadata.dataset_spec_hash(
                spec.source, spec.name
            )
            latest_commit = parquet.metadata._rows(
                "select spec_hash from version_commits "
                "where source=? and dataset=? and status='committed' "
                "order by seq desc limit 1",
                (spec.source, spec.name),
            )
            definition_is_current = bool(
                latest_commit
                and latest_commit[0]["spec_hash"] == current_spec_hash
            )

            current = (
                RawQueryService(parquet, parquet.metadata)
                .query_general(spec.name, source=spec.source)
                .collect()
            )
            current = current.drop(
                [name for name in VERSION_FIELDS if name in current.columns]
            )
            same_schema = set(current.columns) == set(frame.columns) and all(
                current.schema[name] == frame.schema[name] for name in frame.columns
            )
            if (
                definition_is_current
                and same_schema
                and frame_content_hash(current.select(frame.columns))
                == frame_content_hash(frame)
            ):
                with parquet.metadata.connect() as db:
                    db.execute("begin immediate")
                    parquet.metadata.assert_dataset_parent(db, spec.source, spec.name, parent, definition_hash, run_id)
                    _record_check(db, spec, run_id, now, available, baseline, requests, frame.height, input_receipt_id=input_receipt_id)
                    if scope_transitions:
                        parquet.metadata._transition_scopes(db, scope_transitions, run_id=run_id)
                return CommitResult(0, 0, 1, 0)
        frame = frame.with_columns(pl.lit(available).alias("snapshot_date"))
    if _on_unchanged_check is not None:
        raise RuntimeError("Timing attestation cannot publish changed numerical content")
    with parquet.metadata.connect() as db:
        from bagelquant_data.storage.immutable_metadata import encode_requests
        request_record = encode_requests(db, requests or [])
        seq = db.execute(
            "insert into version_commits(source,dataset,run_id,ingested_at,pit_date,mode,status,spec_hash,request_json,input_receipt_id,baseline) values(?,?,?,?,?,?,'prepared',?,?,?,?)",
            (
                spec.source,
                spec.name,
                run_id,
                now.isoformat(),
                available.isoformat(),
                mode,
                parquet.metadata.dataset_spec_hash(spec.source, spec.name),
                request_record,
                input_receipt_id,
                int(baseline),
            ),
        ).lastrowid
    if seq is None:
        raise RuntimeError("Failed to allocate an ingestion commit sequence")
    frame = frame.with_columns(
        pl.lit(now, dtype=pl.Datetime("us", "UTC")).alias("ingested_at"),
        pl.lit(seq, dtype=pl.Int64).alias("_commit_seq"),
        pl.lit(baseline).alias("_baseline"),
    )
    if spec.update_type == "general":
        frame = frame.with_columns(pl.lit(str(seq)).alias("_snapshot_id"))
    partition_field = "snapshot_date" if spec.update_type == "general" else "time"
    groups = frame.with_columns(
        pl.col(partition_field).dt.strftime("%Y-%m").alias("_partition")
    ).partition_by("_partition", as_dict=True, maintain_order=True)
    if not groups:
        groups = {
            (available.strftime("%Y-%m"),): frame.with_columns(
                pl.lit(None, dtype=pl.String).alias("_partition")
            )
        }
    writes = []
    batches = []
    stored_schema = parquet.canonical_schema(spec.source, spec.name)
    schemas = [frame.drop([name for name in ("_record_id","_payload_hash") if name in frame.columns]).schema, *([stored_schema] if stored_schema is not None else [])]
    if spec.update_type == "by_date":
        frame = align_frame(frame, compatible_schema(schemas))
        groups = frame.with_columns(pl.col(partition_field).dt.strftime("%Y-%m").alias("_partition")).partition_by(
            "_partition", as_dict=True, maintain_order=True
        )
    manifest_by_partition = {m["partition_path"]: m for m in manifests}
    pending = iter(groups.items())
    bytes_read = 0
    peak_partition_in_flight = 0
    try:
        while True:
            # Bound both queued tasks and concurrent old+delta partition frames.
            tasks = []
            failure = None
            try:
                for _ in range(partition_workers if writer_executor else 1):
                    item = next(pending, None)
                    if item is None:
                        break
                    (month,), group = item
                    partition = f"year={month[:4]}/month={month[5:]}/data.parquet"
                    args = (spec, group.drop("_partition"), parquet, root, partition,
                            int(seq), manifest_by_partition.get(partition),changed_fields)
                    tasks.append(
                        writer_executor.submit(_write_version_partition, *args)
                        if writer_executor else _write_version_partition(*args)
                    )
            except BaseException as error:
                failure = error
            if not tasks:
                if failure is not None:
                    raise failure
                break
            peak_partition_in_flight = max(peak_partition_in_flight, len(tasks))
            # Settle every submitted writer before rollback; later completion
            # must never republish a partition after its rollback.
            for task in tasks:
                try:
                    write, batch, schema, read_bytes = (
                        task.result() if writer_executor else task
                    )
                    writes.append(write)
                    batches.append(batch)
                    schemas.append(schema)
                    bytes_read += read_bytes
                except BaseException as error:
                    if failure is None:
                        failure = error
            if failure is not None:
                raise failure
        canonical = compatible_schema(schemas)
        parquet.commit_metadata(
            spec,
            canonical,
            [w.manifest for w in writes],
            version_commit={
                "seq": seq, "batches": batches,
                "parent": parent, "definition_hash": definition_hash,
                "check": _check_evidence(spec, run_id, now, available, baseline, requests, checked_rows, attestations, input_receipt_id=input_receipt_id,content_scopes=content_scopes)
                if attestations.height or content_scopes else None,
            },
            scope_transitions=scope_transitions,
            run_id=run_id,
            committed_rows=frame.height,
        )
    except BaseException:
        rollback_partition_writes(writes)
        raise
    return CommitResult(
        frame.height,
        len(writes),
        0,
        sum(w.bytes_written for w in writes),
        present_times=source_times,
        bytes_read=bytes_read,
        peak_partition_in_flight=peak_partition_in_flight,
    )


def _check_evidence(spec, run_id, now, available, baseline, requests, checked_rows, records, *, input_receipt_id=None,content_scopes=()):
    return {
        "source": spec.source, "dataset": spec.name, "run_id": run_id,
        "checked_at": now.isoformat(), "pit_date": available.isoformat(), "baseline": baseline,
        "request_json": json.dumps(requests or [], sort_keys=True, default=str),
        "row_count": checked_rows,
        "records": records.select("_commit_seq", "time", *record_key(spec)).iter_rows()
        if records.height else iter(()),
        "record_schema": records.select("_commit_seq", "time", *record_key(spec)).to_arrow().schema.serialize().to_pybytes() if records.height else None,
        "input_receipt_id": input_receipt_id,
        "content_scopes_json":json.dumps(content_scopes,sort_keys=True),
    }


def _record_check(db, spec, run_id, now, available, baseline, requests, checked_rows, records=None, *, input_receipt_id=None,content_scopes=()):
    return _insert_version_check(db, **_check_evidence(
        spec, run_id, now, available, baseline, requests, checked_rows,
        pl.DataFrame() if records is None else records,
        input_receipt_id=input_receipt_id,content_scopes=content_scopes,
    ))


def _write_version_partition(spec, delta, parquet, root, partition, seq, manifest, changed_fields):
    """Write only this immutable delta; original segments are never read or copied."""
    from bagelquant_data.storage.recovery import register_segment
    delta = sort_versions(delta.drop([name for name in ("_record_id","_payload_hash") if name in delta.columns]))
    month = partition.split("/")
    values = {"year": int(month[0][5:]), "month": int(month[1][6:]), "versioned": True}
    if "source_time" in delta.columns and delta.height:
        values.update(min_source_time=str(delta["source_time"].min()), max_source_time=str(delta["source_time"].max()))
    write = parquet.write_partition_file_result(spec, delta, Path(partition), values,
        existing_manifest=None, write_context=partition_write_context(delta.schema))
    batch = register_segment(parquet.metadata, partition, seq, delta, write.path, write.manifest,changed_fields=changed_fields)
    return write, batch, delta.schema, 0
