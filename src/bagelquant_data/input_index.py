"""Optional, rebuildable selection metadata; original batches remain authoritative."""
from __future__ import annotations

import io
from contextlib import contextmanager
import hashlib
import json
import zlib
from typing import Any

import polars as pl

from bagelquant_data.core.hashing import frame_content_hash

VERSION = "selection.v2"
COLUMNS = ("time", "source_time", "asset_id", "ingested_at", "_baseline",
           "_commit_seq", "_record_id", "_payload_hash", "_snapshot_id", "snapshot_date")
DDL = (
    "CREATE TABLE IF NOT EXISTS input_batch_index("
    "commit_seq INTEGER NOT NULL,partition_path TEXT NOT NULL,content_hash TEXT NOT NULL,"
    "version TEXT NOT NULL,payload BLOB NOT NULL,bounds_json TEXT NOT NULL,PRIMARY KEY(commit_seq,partition_path))",
    "CREATE TABLE IF NOT EXISTS input_selection_index("
    "receipt_digest TEXT NOT NULL,alias TEXT NOT NULL,request_key TEXT NOT NULL,version TEXT NOT NULL,"
    "identity TEXT NOT NULL,parents_json TEXT NOT NULL,has_rows INTEGER NOT NULL,"
    "PRIMARY KEY(receipt_digest,alias,request_key))",
)


def initialize(db) -> None:
    for statement in DDL:
        db.execute(statement)


def available(db) -> bool:
    return db.execute("SELECT 1 FROM sqlite_master WHERE name='input_batch_index'").fetchone() is not None


def encode(frame: pl.DataFrame, content_hash: str) -> bytes:
    """Keep no numerical payload, using its original canonical content token."""
    metadata = frame.select([name for name in COLUMNS if name in frame.columns])
    if "_payload_hash" not in metadata.columns:
        metadata = metadata.with_columns(pl.lit(content_hash).alias("_index_content"))
    buffer = io.BytesIO()
    metadata.write_ipc(buffer)
    raw = buffer.getvalue()
    return len(raw).to_bytes(8, "big") + zlib.compress(raw, 3)


def bounds(frame: pl.DataFrame) -> dict:
    return {"record": "_record_id" in frame.columns,
            **{name: None if axis not in frame.columns or not frame.height else str(getattr(frame[axis], operation)())
               for name, axis, operation in (("lower", "source_time", "min"),
                   ("upper", "source_time", "max"), ("available", "time", "min"))}}


def save(db, partition: str, seq: int, content_hash: str, payload: bytes, summary: dict) -> None:
    binding = {"partition": partition, "seq": seq, "content_hash": content_hash,
               "version": VERSION, "bounds": summary, "payload": hashlib.sha256(payload).hexdigest()}
    summary = {**summary, "binding": binding, "digest": _digest(binding)}
    db.execute("INSERT OR REPLACE INTO input_batch_index VALUES(?,?,?,?,?,?)",
               (seq, partition, content_hash, VERSION, payload, json.dumps(summary)))


def _digest(value: dict) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def read(db, batch: Any, *, start=None, end=None, cutoff=None, max_bytes=None) -> pl.DataFrame | None:
    if not available(db):
        return None
    row = db.execute("SELECT content_hash,version,bounds_json FROM input_batch_index "
                     "WHERE commit_seq=? AND partition_path=?",
                     (batch["commit_seq"], batch["partition_path"])).fetchone()
    if row is None or row[0] != batch["content_hash"] or row[1] != VERSION:
        return None
    summary = json.loads(row[2])
    binding = summary.get("binding")
    expected = {"partition": batch["partition_path"], "seq": batch["commit_seq"],
                "content_hash": batch["content_hash"], "version": VERSION,
                "bounds": {key: value for key, value in summary.items() if key not in {"binding", "digest"}},
                "payload": None if binding is None else binding.get("payload")}
    if not isinstance(binding, dict) or binding != expected or summary.get("digest") != _digest(expected):
        raise RuntimeError("Selection index metadata is damaged; run explicit index maintenance")
    if summary["record"] and (
        start is not None and summary["upper"] is not None and summary["upper"] < str(start)
        or end is not None and summary["lower"] is not None and summary["lower"] > str(end)
        or cutoff is not None and summary["available"] is not None and summary["available"] > str(cutoff)
    ):
        return pl.DataFrame()
    admission = db.execute("SELECT substr(payload,1,8),length(payload) FROM input_batch_index "
                           "WHERE commit_seq=? AND partition_path=?",
                           (batch["commit_seq"], batch["partition_path"])).fetchone()
    if max_bytes is not None and (admission[1] > max_bytes
            or int.from_bytes(admission[0], "big") > max_bytes):
        return None
    row = db.execute("SELECT payload FROM input_batch_index WHERE commit_seq=? AND partition_path=?",
                     (batch["commit_seq"], batch["partition_path"])).fetchone()
    payload = row[0]
    if hashlib.sha256(payload).hexdigest() != binding["payload"] or len(payload) < 8:
        raise RuntimeError("Selection index payload is damaged; run explicit index maintenance")
    size = int.from_bytes(payload[:8], "big")
    if max_bytes is not None and size > max_bytes:
        return None
    try:
        decoder = zlib.decompressobj()
        raw = decoder.decompress(payload[8:], size + 1)
        if len(raw) != size or not decoder.eof or decoder.unused_data:
            raise RuntimeError("Selection index size differs")
        return pl.read_ipc(raw)
    except (zlib.error, pl.exceptions.PolarsError) as error:
        raise RuntimeError("Selection index is damaged; run explicit index maintenance") from error


def identity(frame: pl.DataFrame) -> str:
    return frame_content_hash(frame)


def project_original(store, batch, *, max_bytes, check_canceled=None) -> pl.DataFrame:
    """Explicit audit/build only: bounded projection of immutable original IPC."""
    from bagelquant_data.storage.recovery import project_batch
    chunks, retained = [], 0
    def consume(frame):
        nonlocal retained
        buffer = io.BytesIO()
        frame.write_ipc(buffer)
        raw = buffer.getvalue()
        retained += len(raw)
        if retained > max_bytes // 4:
            raise MemoryError("Historical selection metadata exceeds max_buffer_bytes")
        chunks.append(raw)
    project_batch(store, batch["partition_path"], batch["commit_seq"], batch["content_hash"],
                  columns=COLUMNS, consume=consume, buffer_bytes=max_bytes // 2,
                  check_canceled=check_canceled)
    if chunks:
        return pl.concat([pl.read_ipc(io.BytesIO(chunk)) for chunk in chunks], how="diagonal_relaxed")
    # Empty IPC contains no chunks, but its typed schema is still evidence.
    import pyarrow as pa
    rows = store._rows("SELECT schema_ipc FROM version_batches WHERE commit_seq=? AND partition_path=?",
                       (batch["commit_seq"], batch["partition_path"]))
    schema = pa.ipc.read_schema(pa.BufferReader(rows[0]["schema_ipc"]))
    typed = pl.Schema(schema)
    return pl.DataFrame(schema={name: typed[name] for name in COLUMNS if name in typed})


@contextmanager
def _arrow_index_reader(transport, size, max_bytes):
    import mmap
    import pyarrow as pa
    mapping, source, failure = None, None, None
    holder = []
    try:
        if size > max_bytes // 4:
            mapping = mmap.mmap(transport.fileno(), 0, access=mmap.ACCESS_READ)
            source = pa.BufferReader(mapping)
        else:
            source = pa.BufferReader(transport.read())
        try:
            holder.append(pa.ipc.open_file(source))
        except (pa.ArrowException, ValueError, TypeError) as error:
            failure = str(error)
        if failure is None:
            yield holder
    finally:
        holder.clear()
        if source is not None:
            source.close()
        del source
        if mapping is not None:
            mapping.close()
    if failure is not None:
        raise RuntimeError(f"Selection index IPC is damaged: {failure}")


def audit(store, batch, *, max_bytes, check_canceled=None) -> None:
    """Compare indexed rows to original IPC with bounded, disposable transport."""
    import tempfile
    from bagelquant_data.storage.recovery import project_batch
    with store.connect() as db:
        if not available(db):
            return
        row = db.execute("SELECT substr(payload,1,8),bounds_json,version,content_hash,rowid FROM input_batch_index "
            "WHERE commit_seq=? AND partition_path=?", (batch["commit_seq"], batch["partition_path"])).fetchone()
        if row is None or row[2] != VERSION:
            return
        if row[3] != batch["content_hash"]:
            raise RuntimeError("Selection index original binding differs")
        header, saved, row_id = row[0], json.loads(row[1]), row[4]
        binding = saved.get("binding")
        expected_binding = {"partition": batch["partition_path"], "seq": batch["commit_seq"],
            "content_hash": batch["content_hash"], "version": VERSION,
            "bounds": {k: v for k, v in saved.items() if k not in {"binding", "digest"}},
            "payload": None if binding is None else binding.get("payload")}
        if not isinstance(binding, dict) or binding != expected_binding or saved.get("digest") != _digest(expected_binding):
            raise RuntimeError("Selection index metadata is damaged")
    size = int.from_bytes(header, "big")
    chunk = max(1, max_bytes // 16)
    # Large projected indexes spill rather than forcing a large Polars frame.
    with tempfile.SpooledTemporaryFile(max_size=max(1, max_bytes // 4)) as transport:
        decoder = zlib.decompressobj()
        checksum = hashlib.sha256()
        with store.connect() as db:
            with db.blobopen("input_batch_index", "payload", row_id, readonly=True) as blob:
                checksum.update(blob.read(8))
                while compressed := blob.read(chunk):
                    checksum.update(compressed)
                    pending = compressed
                    while pending:
                        if check_canceled is not None:
                            check_canceled()
                        transport.write(decoder.decompress(pending, chunk))
                        pending = decoder.unconsumed_tail
        if checksum.hexdigest() != binding["payload"]:
            raise RuntimeError("Selection index payload is damaged")
        if not decoder.eof or transport.tell() != size or decoder.unused_data:
            raise RuntimeError("Selection index size differs")
        transport.seek(0)
        # Arrow reads only each requested projected batch from the transport.
        with _arrow_index_reader(transport, size, max_bytes) as readers:
            import pyarrow as pa
            registered = store._rows("SELECT schema_ipc FROM version_batches WHERE commit_seq=? AND partition_path=?",
                (batch["commit_seq"], batch["partition_path"]))
            original_schema = pl.Schema(pa.ipc.read_schema(pa.BufferReader(registered[0]["schema_ipc"])))
            projected_schema = {name: original_schema[name] for name in COLUMNS if name in original_schema}
            if "_payload_hash" not in projected_schema:
                projected_schema["_index_content"] = pl.String()
            if pl.Schema(readers[0].schema) != pl.Schema(projected_schema):
                raise RuntimeError("Selection index schema differs from original evidence")
            position, offset, total = 0, 0, 0
            observed = {"record": "_record_id" in readers[0].schema.names,
                        "lower": None, "upper": None, "available": None}
            def consume(frame):
                nonlocal position, offset, total
                expected = frame
                if "_payload_hash" not in frame.columns:
                    expected = frame.with_columns(pl.lit(batch["content_hash"]).alias("_index_content"))
                begin = 0
                while begin < expected.height:
                    if position >= readers[0].num_record_batches:
                        raise RuntimeError("Selection index has missing rows")
                    indexed, actual = None, None
                    try:
                        indexed = readers[0].get_batch(position)
                        length = min(expected.height - begin, indexed.num_rows - offset)
                        if length == 0:
                            position, offset = position + 1, 0
                            continue
                        actual = pl.from_arrow(indexed.slice(offset, length))
                        matches = expected.slice(begin, length).equals(actual)
                    finally:
                        indexed, actual = None, None
                    if not matches:
                        raise RuntimeError("Selection index differs from original evidence")
                    begin, offset, total = begin + length, offset + length, total + length
                current = bounds(frame)
                for name in ("lower", "upper", "available"):
                    value = current[name]
                    if value is not None:
                        prior = observed[name]
                        observed[name] = value if prior is None else (max(prior, value) if name == "upper" else min(prior, value))
            project_batch(store, batch["partition_path"], batch["commit_seq"], batch["content_hash"],
                          columns=COLUMNS, consume=consume, buffer_bytes=max_bytes // 2,
                          check_canceled=check_canceled)
            if total != sum(readers[0].get_batch(index).num_rows for index in range(readers[0].num_record_batches)):
                raise RuntimeError("Selection index has extra rows")
            if observed != saved["binding"]["bounds"]:
                raise RuntimeError("Selection index bounds differ from original evidence")

def selection(db, digest: str, alias: str, key: str) -> dict | None:
    if not available(db):
        return None
    row = db.execute("SELECT identity,parents_json,has_rows,version FROM input_selection_index "
                     "WHERE receipt_digest=? AND alias=? AND request_key=?", (digest, alias, key)).fetchone()
    if row is None or row[3] != VERSION:
        return None
    encoded = json.loads(row[1])
    value = {"identity": row[0], "parents": encoded["parents"], "has_rows": bool(row[2])}
    binding = {"receipt": digest, "alias": alias, "request": key, "version": VERSION, **value}
    if encoded.get("digest") != _digest(binding):
        raise RuntimeError("Selection summary is damaged; run explicit index maintenance")
    return value


def save_selection(db, digest: str, alias: str, key: str, value: dict) -> None:
    binding = {"receipt": digest, "alias": alias, "request": key, "version": VERSION, **value}
    db.execute("INSERT OR REPLACE INTO input_selection_index VALUES(?,?,?,?,?,?,?)",
               (digest, alias, key, VERSION, value["identity"],
                json.dumps({"parents": value["parents"], "digest": _digest(binding)}, sort_keys=True),
                int(value["has_rows"])))
