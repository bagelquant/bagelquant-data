"""Explicit bounded comparison of later output publications with a completed result."""
from __future__ import annotations

import hashlib
import io
import json
from datetime import UTC, date, datetime, timedelta
from time import monotonic
from typing import TYPE_CHECKING, cast

import polars as pl
import pyarrow as pa

from bagelquant_data.pipeline.versions import VERSION_FIELDS, _payload_hashes
from bagelquant_data.query.raw import _date_value
from bagelquant_data.storage.recovery import project_batch
from .completion import verified_completion_digest, output_check_bound

if TYPE_CHECKING:
    from .api import ItemAPI

_PROVENANCE = {"_item_definition_hash", "_producer_key", "_producer_revision", "_dependency_digest"}
_BATCH_COLUMNS = "commit_seq,partition_path,content_hash,row_count,min_available,max_available,min_observation,max_observation"


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _own_checks(db, name, max_check=None):
    return int(db.execute("select coalesce(max(v.id),0) from version_checks v "
        "join visibility_segments e on e.check_id=v.id where v.source='items' "
        "and v.dataset=? and (? is null or v.id<=?)", (name, max_check, max_check)).fetchone()[0])


def original_binding(db, previous):
    """Bind the completed original corpus once per metadata-only plan."""
    fields = ",".join("b." + field for field in _BATCH_COLUMNS.split(","))
    rows = db.execute("select " + fields + " from version_segments b join version_commits c "
        "on c.seq=b.commit_seq where c.source='items' and c.dataset=? and c.status='committed' "
        "and b.commit_seq<=? order by b.commit_seq,b.partition_path",
        (previous["name"], previous["result_commit"] or 0)).fetchall()
    return hashlib.sha256(_json([dict(row) for row in rows]).encode()).hexdigest()


def recorded_dates(db, previous, batch, *, end: date, max_check: int, original_bound: str) -> list[date] | None:
    """Read admitted impact only; original values are never consulted here."""
    if not db.execute("select 1 from sqlite_master where name='item_output_impact_index'").fetchone():
        return None
    row = db.execute("select payload_json,digest from item_output_impact_index where build_id=? "
        "and commit_seq=? and partition_path=?", (previous["id"], batch["commit_seq"], batch["partition_path"])).fetchone()
    if row is None:
        return None
    if hashlib.sha256(row[0].encode()).hexdigest() != row[1]:
        raise RuntimeError("Output impact index is damaged; run explicit items.index_output_reuse maintenance")
    payload = json.loads(row[0])
    expected = {"completion": verified_completion_digest(db, previous["id"]), "batch": dict(batch)}
    if payload["binding"] != expected:
        raise RuntimeError("Output impact index source binding differs; run explicit output maintenance")
    if payload.get("original_batch_binding") is None:
        return None
    if payload["original_batch_binding"] != original_bound:
        raise RuntimeError("Output impact index original batch authority differs; run explicit output maintenance")
    cutoff = _date_value(payload["cutoff"])
    complete_visibility = (payload.get("bounds_audited") is True and batch["max_available"] is not None
        and _date_value(batch["max_available"]) <= cutoff)
    if (end > cutoff and not complete_visibility
            or payload["own_check"] != _own_checks(db, previous["name"], max_check)):
        return None
    return sorted({_date_value(day) for day, available in payload["affected_coordinates"]
        if _date_value(available) <= end})


def index_output_reuse(owner: ItemAPI, name, *, end, config, check_canceled, progress, run_id):
    """Compare typed output channels, keeping all historical versions and completion authority."""
    options = config
    next_heartbeat = 0.0
    def assert_lease(db):
        row = db.execute("select run_id,owner_id from update_leases where source='items' and dataset=?",
            (name,)).fetchone()
        if row is None or tuple(row) != (run_id, run_id):
            raise RuntimeError("Output maintenance lost its dataset writer lease")
    def guard():
        nonlocal next_heartbeat
        check_canceled()
        if monotonic() >= next_heartbeat:
            with owner._store.connect() as db:
                db.execute("begin immediate")
                assert_lease(db)
                now = datetime.now(UTC)
                db.execute("update update_leases set heartbeat_at=?,lease_expires_at=? "
                    "where source='items' and dataset=? and run_id=? and owner_id=?",
                    (now.isoformat(), (now + timedelta(seconds=300)).isoformat(), name, run_id, run_id))
            next_heartbeat = monotonic() + 30
    guard()
    rows = owner._completed_rows("name=? and status='success' order by end_date desc,id desc limit 1", (name,))
    if not rows:
        raise ValueError("Output reuse maintenance requires an original completed result")
    previous = rows[0]
    if owner.status(name)["definition_hash"] != previous["definition_hash"]:
        raise ValueError("Output reuse maintenance cannot cross a numerical definition change")
    cutoff = _date_value(end)
    first, through = _date_value(previous["start_date"]), _date_value(previous["end_date"])
    if cutoff < through:
        raise ValueError("Output maintenance cutoff precedes completed coverage")
    with owner._store.connect() as db:
        db.execute("begin")
        completion = verified_completion_digest(db, previous["id"])
        own_check = _own_checks(db, name)
        original_check_bound = output_check_bound(db, previous["id"])
        if own_check and (original_check_bound is None or own_check > original_check_bound):
            raise RuntimeError("Output timing attestations require an explicit selected-version impact proof")
        batches = [dict(row) for row in db.execute("select " + ",".join("b." + field for field in _BATCH_COLUMNS.split(",")) + " from version_segments b join version_commits c "
            "on c.seq=b.commit_seq where c.source='items' and c.dataset=? and c.status='committed' "
            "order by b.commit_seq,b.partition_path", (name,))]
    originals = [b for b in batches if b["commit_seq"] <= (previous["result_commit"] or 0)]
    later = [b for b in batches if b["commit_seq"] > (previous["result_commit"] or 0)]
    original_bound = hashlib.sha256(_json(originals).encode()).hexdigest()
    report = {"name": name, "build_id": previous["id"], "batches": len(later), "completed": 0,
        "unchanged_batches": 0, "affected_dates": [], "producer_calls": 0}
    def project(value, consume):
        schema = owner._store._rows("select schema_ipc from version_segments where commit_seq=? and partition_path=?",
            (value["commit_seq"], value["partition_path"]))[0]["schema_ipc"]
        columns = (tuple(field for field in pa.ipc.read_schema(pa.BufferReader(schema)).names if field not in _PROVENANCE)
            if consume is not None else ("time", "source_time"))
        if consume is not None:
            columns = (*columns,"_record_id")
        bounds: dict[str, date | None] = {field: None for field in
            ("min_available", "max_available", "min_observation", "max_observation")}
        rows = 0
        def audited(part):
            nonlocal rows
            rows += part.height
            for column, lower, upper in (("time", "min_available", "max_available"),
                    ("source_time", "min_observation", "max_observation")):
                if part[column].dtype != pl.Date or part[column].null_count():
                    raise ValueError("Output reuse original timing contains invalid coordinates")
                for field, actual, reduce in ((lower, cast(date | None, part[column].min()), min),
                        (upper, cast(date | None, part[column].max()), max)):
                    if actual is not None:
                        previous_bound = bounds[field]
                        bounds[field] = actual if previous_bound is None else reduce(previous_bound, actual)
            if consume is not None:
                consume(part)
        project_batch(owner._store, value["partition_path"], value["commit_seq"], value["content_hash"],
            columns=columns, buffer_bytes=options.max_buffer_bytes // 2, check_canceled=guard, consume=audited)
        if rows != value["row_count"] or any(value[field] is not None
                and bounds[field] != _date_value(value[field]) for field in bounds):
            raise ValueError("Output reuse original batch bounds differ from registered metadata")
    # Establish every original bound before it can exclude a reference batch.
    # Only this explicit maintenance audits bytes; plans bind metadata below.
    for original in originals:
        guard()
        project(original, None)
    for batch in later:
        guard()
        pieces: list[bytes] = []
        retained = 0
        target_keys = None
        def collect(part):
            nonlocal retained
            part = part.filter(pl.col("source_time").is_between(first, through) & (pl.col("time") <= cutoff))
            if target_keys is not None:
                part = part.join(target_keys, on=["_record_id", "time"], how="semi", maintain_order="left")
            buffer = io.BytesIO()
            part.write_ipc(buffer)
            raw = buffer.getvalue()
            retained += len(raw)
            if retained > options.max_buffer_bytes // 4:
                raise MemoryError("Output reuse projection exceeds admitted buffer; increase explicit maintenance admission")
            if part.height:
                pieces.append(raw)
        project(batch, collect)
        current = pl.concat([pl.read_ipc(part) for part in pieces], how="diagonal_relaxed") if pieces else pl.DataFrame()
        dirty = []
        affected_coordinates = []
        if current.height:
            lower, upper = cast(date, current["source_time"].min()), cast(date, current["source_time"].max())
            pieces.clear()
            retained = current.estimated_size()
            target_keys = current.select("_record_id", "time").unique()
            retained += target_keys.estimated_size()
            for original in originals:
                if (original["min_observation"] is not None and _date_value(original["min_observation"]) > upper
                        or original["max_observation"] is not None and _date_value(original["max_observation"]) < lower):
                    continue
                project(original, collect)
            old = pl.concat([pl.read_ipc(part) for part in pieces], how="diagonal_relaxed") if pieces else current.head(0)
            fields = sorted(set(current.columns) - (VERSION_FIELDS - {"_baseline"}) - _PROVENANCE)
            if set(fields) != set(old.columns) - (VERSION_FIELDS - {"_baseline"}) - _PROVENANCE:
                changed = current
            else:
                # Availability remains part of the projection. A later creator
                # cannot supersede a same-availability revision with an older
                # value merely because it appeared elsewhere in the history.
                def tokens(frame):
                    selected = frame.sort("time", "ingested_at", "_commit_seq").unique(
                        subset=["time", "_record_id"], keep="last", maintain_order=True)
                    return selected.with_columns(_payload_hashes(selected, fields).alias("_output_hash"))
                current_tokens, old_tokens = tokens(current), tokens(old)
                changed = current_tokens.join(old_tokens.select("time", "_record_id", "_output_hash"),
                    on=["time", "_record_id", "_output_hash"], how="anti")
            affected_coordinates = changed.select("source_time", "time").unique().sort("source_time", "time").rows()
            dirty = changed["source_time"].unique().sort().to_list()
        payload = _json({"binding": {"completion": completion, "batch": batch}, "cutoff": cutoff,
            "own_check": own_check, "affected_coordinates": affected_coordinates, "bounds_audited": True,
            "original_batch_binding": original_bound})
        with owner._store.connect() as db:
            db.execute("begin immediate")
            check_canceled()
            assert_lease(db)
            if completion != verified_completion_digest(db, previous["id"]) or _own_checks(db, name) != own_check:
                raise RuntimeError("Output maintenance authority changed before publication")
            if original_binding(db, previous) != original_bound:
                raise RuntimeError("Output maintenance original batch authority changed before publication")
            registered = db.execute("select " + _BATCH_COLUMNS + " from version_segments where commit_seq=? and partition_path=?",
                (batch["commit_seq"], batch["partition_path"])).fetchone()
            if registered is None or dict(registered) != batch:
                raise RuntimeError("Output maintenance batch changed before publication")
            db.execute("create table if not exists item_output_impact_index(build_id integer not null, "
                "commit_seq integer not null,partition_path text not null,payload_json text not null,digest text not null, "
                "primary key(build_id,commit_seq,partition_path))")
            db.execute("insert or replace into item_output_impact_index values(?,?,?,?,?)",
                (previous["id"], batch["commit_seq"], batch["partition_path"], payload,
                    hashlib.sha256(payload.encode()).hexdigest()))
        report["completed"] += 1
        report["unchanged_batches"] += not dirty
        report["affected_dates"] = sorted(set(report["affected_dates"]) | {str(day) for day in dirty})
        progress({**report})
    with owner._store.connect() as db:
        db.execute("begin immediate")
        check_canceled()
        assert_lease(db)
    return report
