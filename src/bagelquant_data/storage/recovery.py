"""Immutable compressed Arrow evidence in the dedicated Data SQLite store."""

from __future__ import annotations

import hashlib
import io
import mmap
import sqlite3
import tempfile
import zlib
from contextlib import ExitStack
from typing import BinaryIO, cast
from typing import TYPE_CHECKING
from collections.abc import Callable

import polars as pl
import pyarrow as pa

from bagelquant_data.core.hashing import canonical_arrow_table, frame_content_hash
from bagelquant_data.core.schema import align_frame, concat_compatible_frames
from bagelquant_data.storage.atomic import atomic_write_parquet
from bagelquant_data.storage.data_meta import DataMetaStore

if TYPE_CHECKING:
    from bagelquant_data.storage.parquet import ParquetStore


def _encode_batch(frame: pl.DataFrame) -> bytes:
    table = canonical_arrow_table(frame)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def append_batch(data_meta: DataMetaStore, partition: str, seq: int, frame: pl.DataFrame) -> dict:
    """Durably prepare evidence; it remains invisible until its commit publishes."""
    data_meta.ensure_writable()
    payload = _encode_batch(frame)
    # Compression is pure preparation. Holding SQLite's writer lock here
    # would serialize all admitted partition workers on their CPU-heavy work.
    compressed = zlib.compress(payload, 3)
    batch = {
        "partition_path": partition,
        "content_hash": hashlib.sha256(payload).hexdigest(),
        "row_count": frame.height,
        "schema_ipc": frame.to_arrow().schema.serialize().to_pybytes(),
        "min_available": str(frame["time"].min()) if "time" in frame.columns and frame.height else None,
        "max_available": str(frame["time"].max()) if "time" in frame.columns and frame.height else None,
        "min_observation": str(frame["source_time"].min()) if "source_time" in frame.columns and frame.height else None,
        "max_observation": str(frame["source_time"].max()) if "source_time" in frame.columns and frame.height else None,
    }
    from bagelquant_data import input_index
    # Preserve the authoritative IPC's canonical row order, including ties.
    original = pa.ipc.open_file(pa.BufferReader(payload)).read_all()
    projected = cast(pl.DataFrame, pl.from_arrow(original.select(
        [name for name in input_index.COLUMNS if name in original.column_names])))
    index_payload = input_index.encode(projected, batch["content_hash"])
    with data_meta.connect() as db:
        db.execute("begin immediate")
        commit = db.execute("select status from version_commits where seq=?", (seq,)).fetchone()
        if commit is None or commit[0] != "prepared":
            raise RuntimeError("Recovery evidence requires a prepared commit")
        existing = db.execute(
            "select content_hash from version_batches where commit_seq=? and partition_path=?", (seq, partition)
        ).fetchone()
        if existing is not None and existing[0] != batch["content_hash"]:
            raise RuntimeError("An immutable recovery batch cannot be replaced")
        db.execute(
            "insert or ignore into version_batches(commit_seq,partition_path,content_hash,row_count,schema_ipc,payload,"
            "min_available,max_available,min_observation,max_observation) values(?,?,?,?,?,?,?,?,?,?)",
            (seq, partition, batch["content_hash"], frame.height, batch["schema_ipc"], compressed,
             batch["min_available"], batch["max_available"], batch["min_observation"], batch["max_observation"]),
        )
        input_index.initialize(db)
        input_index.save(db, partition, seq, batch["content_hash"], index_payload, input_index.bounds(frame))
    return batch


def read_batch(data_meta: DataMetaStore, partition: str, seq: int, digest: str, *,
               check_canceled: Callable[[], None] | None = None,
               verify: bool = False) -> pl.DataFrame:
    """Decode committed evidence; explicit audits additionally verify its bytes."""
    cancellation: BaseException | None = None
    def check() -> None:
        nonlocal cancellation
        if check_canceled is not None:
            try:
                check_canceled()
            except BaseException as error:
                cancellation = error
                raise
    if check_canceled is not None:
        check_canceled()
    rows = data_meta._rows(
        "select b.content_hash,b.payload,b.schema_ipc,b.row_count from version_batches b join version_commits c on c.seq=b.commit_seq "
        "where b.commit_seq=? and b.partition_path=? and c.status='committed'", (seq, partition)
    )
    if not rows:
        raise RuntimeError(f"Committed recovery batch {seq} is missing")
    try:
        decoder = zlib.decompressobj()
        compressed = rows[0]["payload"]
        with io.BytesIO() as buffer:
            for offset in range(0, len(compressed), 1024 * 1024):
                pending = compressed[offset:offset + 1024 * 1024]
                while pending:
                    check()
                    buffer.write(decoder.decompress(pending, 1024 * 1024))
                    pending = decoder.unconsumed_tail
            if not decoder.eof:
                raise zlib.error("incomplete compressed stream")
            payload = buffer.getvalue()
    except (zlib.error, TypeError) as error:
        if error is cancellation:
            raise
        raise RuntimeError(f"Recovery batch {seq} compression is damaged") from error
    if rows[0]["content_hash"] != digest or verify and hashlib.sha256(payload).hexdigest() != digest:
        raise RuntimeError(f"Recovery batch {seq} checksum mismatch")
    if check_canceled is not None:
        check_canceled()
    frame = pl.read_ipc(payload)
    schema = pl.Schema(pa.ipc.read_schema(pa.BufferReader(rows[0]["schema_ipc"])))
    if frame.schema != schema or frame.height != rows[0]["row_count"]:
        raise RuntimeError(f"Recovery batch {seq} typed schema/row count mismatch")
    return frame


def verify_batch(data_meta: DataMetaStore, partition: str, seq: int, digest: str,
                 *, buffer_bytes: int = 1024 * 1024,
                 check_canceled: Callable[[], None] | None = None) -> None:
    """Recheck original IPC bytes and structure with bounded decode buffers."""
    _inspect_batch(data_meta, partition, seq, digest, buffer_bytes=buffer_bytes,
                   check_canceled=check_canceled)


def project_batch(data_meta: DataMetaStore, partition: str, seq: int, digest: str, *,
                  columns: tuple[str, ...], consume: Callable[[pl.DataFrame], None],
                  buffer_bytes: int, check_canceled: Callable[[], None] | None = None) -> None:
    """Visit bounded projected chunks after verifying the entire original IPC.

    The consumer must not retain chunks. Original bytes, full Arrow structure and
    all original column types are checked; discarded values are never converted
    to Polars frames. This is an operation-local transport, not a frame cache.
    """
    _inspect_batch(data_meta, partition, seq, digest, buffer_bytes=buffer_bytes,
                   check_canceled=check_canceled, columns=columns, consume=consume)


def _inspect_batch(data_meta: DataMetaStore, partition: str, seq: int, digest: str, *,
                   buffer_bytes: int, check_canceled: Callable[[], None] | None,
                   columns: tuple[str, ...] | None = None,
                   consume: Callable[[pl.DataFrame], None] | None = None) -> None:
    cancellation: BaseException | None = None
    consumer_error: BaseException | None = None
    def check() -> None:
        nonlocal cancellation
        if check_canceled is not None:
            try:
                check_canceled()
            except BaseException as error:
                cancellation = error
                raise
    # Reserve at most a quarter for retained IPC, another quarter for compressed
    # and decoded chunks, and the remaining budget for Arrow/type validation.
    memory_bytes = min(64 * 1024 * 1024, max(0, buffer_bytes // 4))
    chunk_bytes = max(1, min(1024 * 1024, buffer_bytes // 8))
    if check_canceled is not None:
        check_canceled()
    with ExitStack() as cleanup:
        memory = io.BytesIO()
        cleanup.callback(memory.close)
        spool: BinaryIO | None = None
        with data_meta.connect() as db:
            if not db.in_transaction:
                db.execute("begin")
            row = db.execute(
                "select b.rowid as batch_rowid,b.content_hash from version_batches b "
                "join version_commits c on c.seq=b.commit_seq where b.commit_seq=? "
                "and b.partition_path=? and c.status='committed'", (seq, partition),
            ).fetchone()
            if row is None:
                raise RuntimeError(f"Committed recovery batch {seq} is missing")
            if row["content_hash"] != digest:
                raise RuntimeError(f"Recovery batch {seq} checksum mismatch")
            checksum = hashlib.sha256()
            decoder = zlib.decompressobj()
            try:
                with db.blobopen("version_batches", "payload", row["batch_rowid"], readonly=True) as blob:
                    while compressed := blob.read(chunk_bytes):
                        check()
                        pending = compressed
                        while pending:
                            check()
                            piece = decoder.decompress(pending, chunk_bytes)
                            if spool is None and memory.tell() + len(piece) > memory_bytes:
                                spool = cleanup.enter_context(tempfile.TemporaryFile())
                                with memory.getbuffer() as retained:
                                    spool.write(retained)
                                memory.close()
                            if spool is None:
                                memory.write(piece)
                            else:
                                spool.write(piece)
                            checksum.update(piece)
                            pending = decoder.unconsumed_tail
                    if not decoder.eof:
                        raise zlib.error("incomplete compressed stream")
            except (zlib.error, TypeError, sqlite3.Error) as error:
                if error is cancellation or error is consumer_error:
                    raise
                raise RuntimeError(f"Recovery batch {seq} compression is damaged") from error
            if checksum.hexdigest() != digest:
                raise RuntimeError(f"Recovery batch {seq} checksum mismatch")
        # The exact original SHA is necessary but not sufficient: retain IPC
        # structural/type rejection, including valid-SHA non-IPC corruption.
        try:
            mapping = None
            view = None
            if spool is None:
                view = memory.getbuffer()
                cleanup.callback(view.release)
                source = pa.BufferReader(view)
            else:
                spool.flush()
                mapping = mmap.mmap(spool.fileno(), 0, access=mmap.ACCESS_READ)
                cleanup.callback(mapping.close)
                source = pa.BufferReader(mapping)
            reader = None
            batch = None
            structural_error = None
            try:
                reader = pa.ipc.open_file(source)
                empty = pl.from_arrow(pa.Table.from_batches([], schema=reader.schema))
                del empty
                for index in range(reader.num_record_batches):
                    check()
                    batch = reader.get_batch(index)
                    batch.validate(full=True)
                    projected = batch if columns is None else batch.select(
                        [name for name in columns if name in batch.schema.names])
                    rows = max(1, chunk_bytes // max(1, projected.nbytes // max(1, projected.num_rows)))
                    for offset in range(0, projected.num_rows, rows):
                        check()
                        decoded = pl.from_arrow(projected.slice(offset, rows), rechunk=False)
                        if consume is not None:
                            try:
                                consume(cast(pl.DataFrame, decoded))
                            except BaseException as error:
                                # A consumer traceback may hold exported Arrow
                                # buffers; release it before closing the mmap.
                                consumer_error = error.with_traceback(None)
                                raise consumer_error
                        del decoded
                    projected = None
                    batch = None
            except (pa.ArrowException, pl.exceptions.PolarsError, ValueError, TypeError) as error:
                if error is cancellation or error is consumer_error:
                    raise
                # Do not retain the parser traceback's exported mmap buffers
                # while closing the temporary evidence view.
                structural_error = str(error)
            finally:
                projected = None
                decoded = None
                del decoded
                batch = None
                reader = None
                source.close()
                del source
                if mapping is not None:
                    mapping.close()
                if view is not None:
                    view.release()
            if structural_error is not None:
                raise RuntimeError(f"Recovery batch {seq} IPC is damaged: {structural_error}")
        except (pa.ArrowException, pl.exceptions.PolarsError, ValueError, TypeError) as error:
            if error is cancellation or error is consumer_error:
                raise
            raise RuntimeError(f"Recovery batch {seq} IPC is damaged") from error


def registered_batches(parquet: ParquetStore, source: str, dataset: str, partition: str) -> list[dict]:
    return parquet.metadata._rows(
        "select b.commit_seq,b.partition_path,b.content_hash,b.row_count,b.schema_ipc,"
        "b.min_available,b.max_available,b.min_observation,b.max_observation,length(b.payload) as payload_size "
        "from version_batches b join version_commits c on c.seq=b.commit_seq "
        "where c.source=? and c.dataset=? and c.status='committed' and b.partition_path=? order by b.commit_seq",
        (source, dataset, partition),
    )


def reconstruct(parquet: ParquetStore, source: str, dataset: str, partition: str) -> pl.DataFrame:
    batches = registered_batches(parquet, source, dataset, partition)
    if not batches:
        raise RuntimeError("No registered recovery evidence; historical recovery is blocked")
    frames = [read_batch(parquet.metadata, partition, int(b["commit_seq"]), b["content_hash"], verify=True) for b in batches]
    frame = sort_versions(concat_compatible_frames(frames))
    # A by-date delta was aligned to the full committed partition's schema.
    # Concatenation's provider-inference normalization can turn entirely null
    # String/scalar columns into Null; recovery must restore the retained types.
    schema = pl.Schema(pa.ipc.read_schema(pa.BufferReader(batches[-1]["schema_ipc"])))
    if "_record_id" in schema:
        frame = align_frame(frame, schema)
    manifest = next((row for row in parquet.metadata.manifest(source, dataset) if row["partition_path"] == partition), None)
    if manifest is None:
        raise RuntimeError("Committed manifest evidence is missing")
    if frame_content_hash(frame) != manifest["content_hash"]:
        raise RuntimeError("Recovery evidence does not reproduce the committed partition")
    return frame


def sort_versions(frame: pl.DataFrame) -> pl.DataFrame:
    fields = [name for name in ("time", "source_time", "asset_id", "_record_id", "_commit_seq") if name in frame.columns]
    return frame.sort(fields) if fields else frame


def repair_partition(parquet: ParquetStore, source: str, dataset: str, partition: str) -> dict:
    """Restore the registered generation without changing identity or coverage."""
    parquet.metadata.ensure_writable()
    try:
        frame = reconstruct(parquet, source, dataset, partition)
    except (RuntimeError, OSError, sqlite3.Error, zlib.error, pl.exceptions.PolarsError):
        restore_batches(parquet, source, dataset, partition)
        frame = reconstruct(parquet, source, dataset, partition)
    manifest = next(row for row in parquet.metadata.manifest(source, dataset) if row["partition_path"] == partition)
    path = parquet.paths.generation_path(source, dataset, manifest["partition_path"], manifest["generation_path"])
    atomic_write_parquet(frame, path)
    if frame_content_hash(pl.read_parquet(path, hive_partitioning=False)) != manifest["content_hash"]:
        raise RuntimeError("Repaired partition failed its committed checksum")
    return {"partition": partition, "rows": frame.height, "method": "replay_ingestion", "content_changed": False}


def restore_batches(parquet: ParquetStore, source: str, dataset: str, partition: str) -> None:
    """Recover payloads only when committed Parquet reproduces every stored digest."""
    parquet.metadata.ensure_writable()
    manifest = next((row for row in parquet.metadata.manifest(source, dataset) if row["partition_path"] == partition), None)
    if manifest is None:
        raise RuntimeError("No committed manifest; historical recovery is blocked")
    path = parquet.paths.generation_path(source, dataset, manifest["partition_path"], manifest["generation_path"])
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
        payloads.append((zlib.compress(payload, 3), batch["commit_seq"], partition, batch["content_hash"]))
    with parquet.metadata.connect() as db:
        db.execute("begin immediate")
        db.executemany("update version_batches set payload=? where commit_seq=? and partition_path=? and content_hash=?", payloads)


def inspect_partition(parquet: ParquetStore, source: str, dataset: str, partition: str, *, deep: bool = False) -> dict:
    batches = registered_batches(parquet, source, dataset, partition)
    result = {"partition": partition, "registered_batches": len(batches), "recovery_state": "ready", "error": None}
    try:
        if not batches or any(not batch.get("payload_size") for batch in batches):
            raise RuntimeError("Registered recovery batches are missing")
        if deep:
            reconstruct(parquet, source, dataset, partition)
    except (RuntimeError, OSError, sqlite3.Error, zlib.error, pl.exceptions.PolarsError) as error:
        result.update(recovery_state="corrupt", error=str(error))
    return result
