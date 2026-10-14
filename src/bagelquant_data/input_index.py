"""Optional, rebuildable selection metadata; original batches remain authoritative."""
from __future__ import annotations

import io
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
    "CREATE TABLE IF NOT EXISTS input_selection_index("
    "receipt_digest TEXT NOT NULL,alias TEXT NOT NULL,request_key TEXT NOT NULL,version TEXT NOT NULL,"
    "identity TEXT NOT NULL,parents_json TEXT NOT NULL,has_rows INTEGER NOT NULL,"
    "PRIMARY KEY(receipt_digest,alias,request_key))",
    "CREATE TABLE IF NOT EXISTS input_projection_index("
    "evidence_key TEXT NOT NULL,request_key TEXT NOT NULL,version TEXT NOT NULL,"
    "identity TEXT NOT NULL,source_receipt_id TEXT NOT NULL,source_receipt_digest TEXT NOT NULL,"
    "alias TEXT NOT NULL,parents_json TEXT NOT NULL,has_rows INTEGER NOT NULL,digest TEXT NOT NULL,"
    "PRIMARY KEY(evidence_key,request_key))",
    "CREATE TABLE IF NOT EXISTS input_coordinate_index("
    "receipt_digest TEXT NOT NULL,alias TEXT NOT NULL,selection TEXT NOT NULL,"
    "dates_json TEXT NOT NULL,digest TEXT NOT NULL,PRIMARY KEY(receipt_digest,alias,selection))",

)


def initialize(db) -> None:
    for statement in DDL:
        db.execute(statement)


def available(db, *, values=False) -> bool:
    return db.execute("SELECT 1 FROM sqlite_master WHERE name='input_selection_index'").fetchone() is not None


def bounds(frame: pl.DataFrame) -> dict:
    return {"record": "source_time" in frame.columns,
            "max_availability_delay_days": int(frame.select((pl.col("time") - pl.col("source_time")).dt.total_days().max()).item())
                if frame.height and frame.schema.get("time") == pl.Date and frame.schema.get("source_time") == pl.Date
                    and not frame["time"].null_count() and not frame["source_time"].null_count() else None,
            **{name: None if axis not in frame.columns or not frame.height else str(getattr(frame[axis], operation)())
               for name, axis, operation in (("lower", "source_time", "min"),
                   ("upper", "source_time", "max"), ("available", "time", "min"))}}


def _digest(value: dict) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def save_coordinates(db, receipt_digest: str, alias: str, field: str,
                     selector: tuple[str, int] | None, frame: pl.DataFrame) -> None:
    """Record a complete selected coordinate axis while typed inputs are consumed."""
    save_coordinate_dates(db, receipt_digest, alias, field, selector, coordinate_dates(frame, field, selector))


def save_coordinate_dates(db, receipt_digest: str, alias: str, field: str,
                          selector: tuple[str, int] | None, dates: list[str]) -> None:
    """Publish the admitted original date vector in its existing sole store."""
    selection = json.dumps([field, selector], separators=(",", ":"))
    body = {"receipt": receipt_digest, "alias": alias, "selection": selection, "dates": dates}
    initialize(db)
    db.execute("INSERT OR REPLACE INTO input_coordinate_index VALUES(?,?,?,?,?)",
        (receipt_digest, alias, selection, json.dumps(dates), _digest(body)))


def coordinate_dates(frame: pl.DataFrame, field: str, selector: tuple[str, int] | None) -> list[str]:
    """Project the complete admitted axis, including dates without producer rows."""
    if selector is not None:
        frame = frame.filter(pl.col(selector[0]).cast(pl.Int64, strict=False) == selector[1])
    return [str(value) for value in frame[field].cast(pl.Date).drop_nulls().unique().sort().to_list()]


def read_coordinates(db, receipt_digest: str, alias: str, field: str,
                     selector: tuple[str, int] | None, *, max_bytes: int | None = None) -> list[str] | None:
    """Read an immutable receipt-bound complete axis; missing proof stays unknown."""
    if db.execute("SELECT 1 FROM sqlite_master WHERE name='input_coordinate_index'").fetchone() is None:
        return None
    selection = json.dumps([field, selector], separators=(",", ":"))
    if max_bytes is not None:
        size = db.execute("SELECT length(dates_json) FROM input_coordinate_index "
            "WHERE receipt_digest=? AND alias=? AND selection=?", (receipt_digest, alias, selection)).fetchone()
        # JSON text, parsed strings and the clipped/hash projection share this
        # admitted reserve; never fetch/decode an oversized coordinate axis.
        if size is not None and size[0] * 16 > max_bytes:
            return None
    row = db.execute("SELECT dates_json,digest FROM input_coordinate_index "
        "WHERE receipt_digest=? AND alias=? AND selection=?", (receipt_digest, alias, selection)).fetchone()
    if row is None:
        return None
    dates = json.loads(row[0])
    if row[1] != _digest({"receipt": receipt_digest, "alias": alias, "selection": selection, "dates": dates}):
        raise RuntimeError("Coordinate index metadata is damaged; run explicit index maintenance")
    return dates


def coordinate_content_identity(dates: list[str]) -> str:
    """Bind the existing original date payload, independently of its root UUID."""
    return _digest({"coordinates": dates})


def save_coordinate_projection(db, evidence_key: str, key: str, dates: list[str], *,
                               receipt_id: str, receipt_digest: str, alias: str) -> None:
    """Locate an original coordinate payload through the existing projection index.

    The date vector remains in input_coordinate_index under its original root.
    This row retains source provenance and its payload identity, not another
    date-vector store or a second preparation/currentness authority.
    """
    save_projection(db, evidence_key, key,
        {"identity": coordinate_content_identity(dates), "parents": [], "has_rows": bool(dates)},
        receipt_id=receipt_id, receipt_digest=receipt_digest, alias=alias)


def describe_bounds(db, batch: Any, *, values=False, max_bytes: int | None = None) -> dict | None:
    """Read original segment bounds; no row payload or optional index exists."""
    row = db.execute("select content_hash,bounds_json from version_segments where commit_seq=? and partition_path=?",
        (batch["commit_seq"],batch["partition_path"])).fetchone()
    if row is None or row[0] != batch["content_hash"]:
        raise RuntimeError("Original segment bounds binding differs")
    if max_bytes is not None and len(row[1])*4 > max_bytes:
        raise MemoryError("Segment bounds metadata exceeds max_buffer_bytes")
    return json.loads(row[1])


def identity(frame: pl.DataFrame) -> str:
    return frame_content_hash(frame)


def project_original(store, batch, *, max_bytes, check_canceled=None, values=False, start=None, end=None, scalar=False, observation_windows=None, work=None, fields: tuple[str, ...] | None = None) -> pl.DataFrame:
    """Explicit audit/build only: bounded projection of immutable original IPC."""
    from bagelquant_data.storage.recovery import project_batch
    chunks, retained = [], 0
    def consume(frame):
        nonlocal retained
        # Observation clipping is safe before availability/PIT resolution;
        # those operations preserve source_time. Availability is never clipped.
        from bagelquant_data.query.raw import _date_value
        if "source_time" in frame.columns:
            if start is not None:
                frame = frame.filter(pl.col("source_time") >= _date_value(start))
            if end is not None:
                frame = frame.filter(pl.col("source_time") <= _date_value(end))
        if observation_windows is not None and "source_time" in frame.columns:
            admitted = pl.lit(False)
            for lower, upper in observation_windows:
                predicate = pl.lit(True)
                if lower is not None:
                    predicate = predicate & (pl.col("source_time") >= _date_value(lower))
                if upper is not None:
                    predicate = predicate & (pl.col("source_time") <= _date_value(upper))
                admitted = admitted | predicate
            frame = frame.filter(admitted)
        if scalar and values:
            from bagelquant_data.pipeline.versions import _payload_hashes
            if check_canceled is not None:
                check_canceled()
            frame = frame.with_columns(_payload_hashes(frame, ["value"]).rename("_value_hash"))
            if fields is None:
                frame = frame.drop("value")
            if work is not None:
                work["hash_chunks"] += 1
                work["hash_rows"] += frame.height
        if check_canceled is not None:
            check_canceled()
        buffer = io.BytesIO()
        frame.write_ipc(buffer)
        raw = buffer.getvalue()
        retained += len(raw)
        if retained > max_bytes // 4:
            raise MemoryError("Historical selection metadata exceeds max_buffer_bytes")
        chunks.append(raw)
        if work is not None:
            work["owned_transport_bytes"] += len(raw)
    requested: list[str] = [*COLUMNS, *(["value"] if values else []), *(fields or ())]
    columns = tuple(dict[str, None].fromkeys(requested))
    project_batch(store, batch["partition_path"], batch["commit_seq"], batch["content_hash"],
                  columns=columns, consume=consume, buffer_bytes=max_bytes // 2,
                  check_canceled=check_canceled)
    if chunks:
        return pl.concat([pl.read_ipc(io.BytesIO(chunk)) for chunk in chunks], how="diagonal_relaxed")
    # Empty IPC contains no chunks, but its typed schema is still evidence.
    import pyarrow as pa
    rows = store._rows("SELECT schema_ipc FROM version_segments WHERE commit_seq=? AND partition_path=?",
                       (batch["commit_seq"], batch["partition_path"]))
    schema = pa.ipc.read_schema(pa.BufferReader(rows[0]["schema_ipc"]))
    typed = pl.Schema(schema)
    return pl.DataFrame(schema={name: typed[name] for name in columns if name in typed})


def audit_captured_bounds(store, batch, *, max_bytes, check_canceled=None) -> None:
    """Audit immutable receipt bounds against every retained original row."""
    from bagelquant_data.storage.recovery import project_batch
    observed: dict[str, str | None] = {key: None for key in ("min_available", "max_available", "min_observation", "max_observation")}
    def consume(frame):
        for axis, lower, upper in (("time", "min_available", "max_available"),
                                  ("source_time", "min_observation", "max_observation")):
            if axis not in frame.columns or frame.is_empty():
                continue
            for key, value, select in ((lower, frame[axis].min(), min), (upper, frame[axis].max(), max)):
                if value is not None:
                    current = str(value)
                    previous = observed[key]
                    observed[key] = current if previous is None else select(previous, current)
    project_batch(store, batch["partition_path"], batch["commit_seq"], batch["content_hash"],
        columns=("time", "source_time"), consume=consume, buffer_bytes=max_bytes // 2,
        check_canceled=check_canceled)
    if observed != {key: batch.get(key) for key in observed}:
        raise RuntimeError("Captured batch bounds differ from original evidence")

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


def selection_value(value: dict) -> dict:
    """The stored proof value, excluding context-only locator metadata."""
    return {name: value[name] for name in ("identity", "parents", "has_rows")}


def save_selection(db, digest: str, alias: str, key: str, value: dict) -> None:
    value = selection_value(value)
    binding = {"receipt": digest, "alias": alias, "request": key, "version": VERSION, **value}
    db.execute("INSERT OR REPLACE INTO input_selection_index VALUES(?,?,?,?,?,?,?)",
               (digest, alias, key, VERSION, value["identity"],
                json.dumps({"parents": value["parents"], "digest": _digest(binding)}, sort_keys=True),
                int(value["has_rows"])))


def projection(db, evidence_key: str, key: str, *, max_bytes: int | None = None) -> dict | None:
    """Read a scalar summary bound to selected immutable source metadata."""
    if not db.execute("SELECT 1 FROM sqlite_master WHERE name='input_projection_index'").fetchone():
        return None
    if max_bytes is not None:
        size = db.execute("SELECT length(request_key)+length(identity)+length(parents_json)+"
            "length(source_receipt_id)+length(source_receipt_digest)+length(alias)+512 "
            "FROM input_projection_index WHERE evidence_key=? AND request_key=?", (evidence_key, key)).fetchone()
        if size is not None and int(size[0]) * 4 > max_bytes:
            return None
    row = db.execute("SELECT * FROM input_projection_index WHERE evidence_key=? AND request_key=?",
        (evidence_key, key)).fetchone()
    if row is None or row["version"] not in {VERSION,"segment_range.v1","general_asset_append.v1"}:
        return None
    body = {name: row[name] for name in row.keys() if name != "digest"}
    if row["digest"] != _digest(body):
        raise RuntimeError("Scalar selection summary is damaged; run explicit index maintenance")
    return {"kind":row["version"],"identity": row["identity"], "parents": json.loads(row["parents_json"]),
        "has_rows": bool(row["has_rows"]), "source_receipt_id": row["source_receipt_id"],
        "source_receipt_digest": row["source_receipt_digest"], "alias": row["alias"]}


def save_projection(db, evidence_key: str, key: str, value: dict, *, receipt_id: str,
                    receipt_digest: str, alias: str) -> None:
    body = {"evidence_key": evidence_key, "request_key": key, "version": value.get("kind",VERSION),
        "identity": value["identity"], "source_receipt_id": receipt_id, "source_receipt_digest": receipt_digest,
        "alias": alias, "parents_json": json.dumps(value["parents"], sort_keys=True),
        "has_rows": int(value["has_rows"])}
    initialize(db)
    db.execute("INSERT OR REPLACE INTO input_projection_index VALUES(?,?,?,?,?,?,?,?,?,?)",
        (*body.values(), _digest(body)))


_SELECTOR_VERSION = "coordinate.selector.v1"


def selector_header(db, identity: str, *, max_bytes: int) -> dict | None:
    if not db.execute("select 1 from sqlite_master where name='input_projection_selectors'").fetchone():
        return None
    row = db.execute("select length(metadata_json) as size from input_projection_selectors where identity=?", (identity,)).fetchone()
    if row is None:
        return None
    if row["size"] * 8 > max_bytes:
        raise MemoryError("Coordinate selector header exceeds max_buffer_bytes")
    row = db.execute("select metadata_json,digest,length(payload) as stored from input_projection_selectors where identity=?", (identity,)).fetchone()
    body = json.loads(row["metadata_json"])
    if (not isinstance(body, dict)
            or set(body) != {"identity", "version", "schema", "rows", "raw_bytes", "stored_bytes", "payload"}
            or body.get("identity") != identity or body.get("version") != _SELECTOR_VERSION
            or body.get("schema") != {"time": "Date", "asset_id": "String"}
            or any(type(body.get(field)) is not int or body[field] < 0 for field in ("rows", "raw_bytes", "stored_bytes"))
            or body["stored_bytes"] < 8 or body["stored_bytes"] != row["stored"]
            or not isinstance(body.get("payload"), str) or len(body["payload"]) != 64
            or any(value not in "0123456789abcdef" for value in body["payload"])
            or row["digest"] != _digest(body)):
        raise RuntimeError("Coordinate selector metadata is damaged")
    return body


def save_selector(db, coordinates: pl.DataFrame, *, max_bytes: int, rebuild: bool = False) -> None:
    key = identity(coordinates)
    if not rebuild and selector_header(db, key, max_bytes=max_bytes // 8) is not None:
        return
    if coordinates.estimated_size() * 8 > max_bytes:
        raise MemoryError("Coordinate selector exceeds max_buffer_bytes")
    buffer = io.BytesIO()
    coordinates.sort("time", "asset_id").rechunk().write_ipc(buffer)
    raw = buffer.getvalue()
    if len(raw) * 4 > max_bytes:
        raise MemoryError("Coordinate selector transport exceeds max_buffer_bytes")
    payload = len(raw).to_bytes(8, "big") + zlib.compress(raw, 3)
    body = {"identity": key, "version": _SELECTOR_VERSION, "schema": {"time": "Date", "asset_id": "String"}, "rows": coordinates.height,
            "raw_bytes": len(raw), "stored_bytes": len(payload), "payload": hashlib.sha256(payload).hexdigest()}
    db.execute("create table if not exists input_projection_selectors("
               "identity text primary key,payload blob not null,metadata_json text not null,digest text not null)")
    db.execute("insert or replace into input_projection_selectors values(?,?,?,?)",
               (key, payload, json.dumps(body, sort_keys=True), _digest(body)))


def read_selector(db, key: str, *, max_bytes: int, check_canceled=None) -> pl.DataFrame:
    if check_canceled is not None:
        check_canceled()
    body = selector_header(db, key, max_bytes=max_bytes // 8)
    if body is None:
        raise RuntimeError("Coordinate selector is missing; run explicit index_selection maintenance")
    size = db.execute("select length(payload) as stored,substr(payload,1,8) as header from input_projection_selectors where identity=?", (key,)).fetchone()
    if (size["stored"] != body["stored_bytes"] or len(size["header"]) != 8
            or int.from_bytes(size["header"], "big") != body["raw_bytes"]):
        raise RuntimeError("Coordinate selector transport binding differs")
    if max(body["stored_bytes"], body["raw_bytes"]) * 4 > max_bytes:
        raise MemoryError("Coordinate selector decode exceeds max_buffer_bytes")
    payload = db.execute("select payload from input_projection_selectors where identity=?", (key,)).fetchone()[0]
    if hashlib.sha256(payload).hexdigest() != body["payload"]:
        raise RuntimeError("Coordinate selector payload is damaged")
    if check_canceled is not None:
        check_canceled()
    decoder = zlib.decompressobj()
    raw = decoder.decompress(payload[8:], body["raw_bytes"] + 1)
    if len(raw) != body["raw_bytes"] or not decoder.eof or decoder.unused_data:
        raise RuntimeError("Coordinate selector decompression differs")
    if check_canceled is not None:
        check_canceled()
    coordinates = pl.read_ipc(raw)
    if (coordinates.schema != {"time": pl.Date, "asset_id": pl.String}
            or coordinates.height != body["rows"] or coordinates.null_count().row(0) != (0, 0)
            or coordinates["asset_id"].str.len_chars().eq(0).any()
            or coordinates.unique().height != coordinates.height or identity(coordinates) != key):
        raise RuntimeError("Coordinate selector differs from its canonical identity")
    if check_canceled is not None:
        check_canceled()
    return coordinates
