"""Monthly, immutable ingestion batches; lake.db alone decides visibility."""

from __future__ import annotations

import hashlib
import io
import sqlite3
import zlib
from pathlib import Path
from typing import TYPE_CHECKING
from contextlib import closing

import polars as pl
import pyarrow as pa

from bagelquant_data.core.hashing import frame_content_hash, canonical_arrow_table
from bagelquant_data.core.schema import concat_compatible_frames
from bagelquant_data.storage.atomic import atomic_write_parquet

if TYPE_CHECKING:
    from bagelquant_data.storage.parquet import ParquetStore


def journal_path(root: Path, partition: str | Path) -> Path:
    path = (root / partition).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ValueError("Recovery partition escapes dataset root")
    return path.with_name("recovery.sqlite")


def _encode_batch(frame: pl.DataFrame) -> bytes:
    table = canonical_arrow_table(frame)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def append_batch(root: Path, partition: str, seq: int, frame: pl.DataFrame) -> dict:
    path = journal_path(root, partition)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = _encode_batch(frame)
    digest = hashlib.sha256(payload).hexdigest()
    db = sqlite3.connect(path)
    try:
        db.execute("pragma synchronous=FULL")
        db.execute(
            "create table if not exists batches (commit_seq integer primary key, digest text not null, payload blob not null)"
        )
        with db:
            existing = db.execute(
                "select digest from batches where commit_seq=?", (seq,)
            ).fetchone()
            if existing is not None and existing[0] != digest:
                raise RuntimeError("An immutable recovery batch cannot be replaced")
            db.execute(
                "insert or ignore into batches values(?,?,?)",
                (seq, digest, zlib.compress(payload, 3)),
            )
    finally:
        db.close()
    return {
        "partition_path": partition,
        "content_hash": digest,
        "row_count": frame.height,
        "schema_ipc": frame.to_arrow().schema.serialize().to_pybytes(),
        "min_available": str(frame["time"].min()) if "source_time" in frame.columns and frame.height else None,
        "max_available": str(frame["time"].max()) if "source_time" in frame.columns and frame.height else None,
        "min_observation": str(frame["source_time"].min()) if "source_time" in frame.columns and frame.height else None,
        "max_observation": str(frame["source_time"].max()) if "source_time" in frame.columns and frame.height else None,
    }


def read_batch(root: Path, partition: str, seq: int, digest: str) -> pl.DataFrame:
    path = journal_path(root, partition)
    db = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
    try:
        row = db.execute(
            "select digest,payload from batches where commit_seq=?", (seq,)
        ).fetchone()
    finally:
        db.close()
    if row is None:
        raise RuntimeError(f"Recovery batch {seq} is missing")
    payload = zlib.decompress(row[1])
    if row[0] != digest or hashlib.sha256(payload).hexdigest() != digest:
        raise RuntimeError(f"Recovery batch {seq} checksum mismatch")
    return pl.read_ipc(io.BytesIO(payload))


def registered_batches(
    parquet: ParquetStore, source: str, dataset: str, partition: str
) -> list[dict]:
    return parquet.metadata._rows(
        "select b.* from version_batches b join version_commits c on c.seq=b.commit_seq "
        "where c.source=? and c.dataset=? and c.status='committed' and b.partition_path=? order by b.commit_seq",
        (source, dataset, partition),
    )


def reconstruct(
    parquet: ParquetStore, source: str, dataset: str, partition: str
) -> pl.DataFrame:
    root = parquet.paths.dataset_root(source, dataset)
    batches = registered_batches(parquet, source, dataset, partition)
    if not batches:
        raise RuntimeError(
            "No registered recovery evidence; provider data cannot recreate historical versions"
        )
    frames = [
        read_batch(root, partition, int(b["commit_seq"]), b["content_hash"])
        for b in batches
    ]
    frame = concat_compatible_frames(frames)
    schema = parquet.canonical_schema(source, dataset)
    if schema is not None:
        from bagelquant_data.core.schema import align_frame

        # A partition retains its own committed schema, not later unrelated columns.
        existing = next(
            r
            for r in parquet.metadata.manifest(source, dataset)
            if r["partition_path"] == partition
        )
        if frames[-1].schema == schema:
            frame = align_frame(frame, schema)
        expected = existing["content_hash"]
    else:
        raise RuntimeError("Canonical schema evidence is missing")
    frame = sort_versions(frame)
    if frame_content_hash(frame) != expected:
        raise RuntimeError(
            "Recovery evidence does not reproduce the committed partition"
        )
    return frame


def sort_versions(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.sort(
        [
            name
            for name in ("time", "source_time", "asset_id", "_record_id", "_commit_seq")
            if name in frame.columns
        ]
    )


def repair_partition(
    parquet: ParquetStore, source: str, dataset: str, partition: str
) -> dict:
    """Rebuild bytes from registered evidence without changing logical generation."""
    try:
        frame = reconstruct(parquet, source, dataset, partition)
    except (RuntimeError, OSError, sqlite3.Error, zlib.error, pl.exceptions.PolarsError):
        restore_journal(parquet, source, dataset, partition)
        frame = reconstruct(parquet, source, dataset, partition)
    path = parquet.paths.dataset_root(source, dataset) / partition
    atomic_write_parquet(frame, path)
    # A physical repair never manufactures an ingestion time or advances coverage.
    return {
        "partition": partition,
        "rows": frame.height,
        "method": "replay_ingestion",
        "content_changed": False,
    }


def restore_journal(parquet: ParquetStore, source: str, dataset: str, partition: str) -> None:
    """Recover a journal only from a verified complete partition and batch evidence."""
    from bagelquant_data.core.schema import align_frame
    from bagelquant_data.storage.atomic import replace_with_retry
    from uuid import uuid4

    root = parquet.paths.dataset_root(source, dataset)
    path = root / partition
    manifest = next((m for m in parquet.metadata.manifest(source, dataset)
                     if m["partition_path"] == partition), None)
    if manifest is None:
        raise RuntimeError("No committed manifest; historical recovery is blocked")
    frame = pl.read_parquet(path, hive_partitioning=False)
    if frame_content_hash(frame) != manifest["content_hash"]:
        raise RuntimeError("Both recovery evidence and Parquet are damaged; historical recovery is blocked")
    batches = registered_batches(parquet, source, dataset, partition)
    if not batches:
        raise RuntimeError("No registered batches; historical recovery is blocked")
    payloads = []
    for batch in batches:
        schema = pl.Schema(pa.ipc.read_schema(pa.BufferReader(batch["schema_ipc"])))
        delta = align_frame(frame.filter(pl.col("_commit_seq") == int(batch["commit_seq"])), schema)
        payload = _encode_batch(delta)
        if delta.height != batch["row_count"] or hashlib.sha256(payload).hexdigest() != batch["content_hash"]:
            raise RuntimeError("Verified Parquet cannot reproduce the original batch; historical recovery is blocked")
        payloads.append((batch["commit_seq"], batch["content_hash"], zlib.compress(payload, 3)))
    journal = journal_path(root, partition)
    temporary = journal.with_name(f".recovery-{uuid4().hex}.sqlite")
    try:
        with closing(sqlite3.connect(temporary)) as db:
            with db:
                db.execute("pragma synchronous=FULL")
                db.execute("create table batches (commit_seq integer primary key, digest text not null, payload blob not null)")
                db.executemany("insert into batches values(?,?,?)", payloads)
        replace_with_retry(temporary, journal)
    finally:
        temporary.unlink(missing_ok=True)


def inspect_partition(
    parquet: ParquetStore,
    source: str,
    dataset: str,
    partition: str,
    *,
    deep: bool = False,
) -> dict:
    root = parquet.paths.dataset_root(source, dataset)
    batches = registered_batches(parquet, source, dataset, partition)
    result = {
        "partition": partition,
        "registered_batches": len(batches),
        "recovery_state": "ready",
        "error": None,
    }
    try:
        if not batches or not journal_path(root, partition).is_file():
            raise RuntimeError("Registered recovery batches are missing")
        if deep:
            reconstruct(parquet, source, dataset, partition)
    except (
        RuntimeError,
        OSError,
        sqlite3.Error,
        zlib.error,
        pl.exceptions.PolarsError,
    ) as error:
        result.update(recovery_state="corrupt", error=str(error))
    return result
