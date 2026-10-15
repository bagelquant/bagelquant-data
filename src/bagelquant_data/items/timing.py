"""Proven General verification changes reuse completed numerical values."""
from __future__ import annotations

import calendar
import hashlib
import json
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING
from concurrent.futures import CancelledError
from types import SimpleNamespace

import polars as pl

from bagelquant_data.query.raw import _date_value
from bagelquant_data.storage.immutable_metadata import MetadataReader, canonical
from .completion import output_check_bound, pending_dates, save_completion, verified_completion_digest
from .pit import _general_snapshot_identity
from .types import ItemBuildReport, ProducerUpdatePolicy, RawInput, input_from_payload

if TYPE_CHECKING:
    from .api import ItemAPI


def _digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def _transition(receipt, payload, policy, end):
    cutoff = receipt.information_cutoff
    states = {}
    boundaries = {cutoff, end}
    for alias, request in receipt.requests.items():
        if (not isinstance(request, RawInput) or request.strict
                or alias not in policy.timing_neutral_inputs
                or any(value is not None for value in (request.start, request.end,
                    request.observation_start, request.observation_end))):
            return None
        before, after = receipt.evidence[alias], payload["evidence"][alias]
        if not before["general_snapshots"] or before["definition_hash"] != after["definition_hash"]:
            return None
        initial = _general_snapshot_identity(before["general_snapshots"], cutoff,
            checks=before["checks"], include_historical_baseline=request.include_historical_baseline)
        if initial is None:
            return None
        for snapshot in after["general_snapshots"]:
            if (_date_value(snapshot["pit_date"]) <= end and snapshot["seq"] != initial["seq"]
                    and (snapshot["seq"] > receipt.max_commit or _date_value(snapshot["pit_date"]) > cutoff)):
                return None
        original_snapshot = next(row for row in before["general_snapshots"] if row["seq"] == initial["seq"])
        current_snapshot = next((row for row in after["general_snapshots"] if row["seq"] == initial["seq"]), None)
        if current_snapshot is None or canonical(original_snapshot) != canonical(current_snapshot):
            return None
        for check in after["checks"]:
            if not check["baseline"] and check["visible_commit"] == initial["seq"]:
                day = max(_date_value(current_snapshot["pit_date"]), _date_value(check["pit_date"]))
                if cutoff < day <= end:
                    boundaries.add(day)
        states[alias] = (initial, after)
    transition = None
    for day in sorted(boundaries):
        baseline = False
        for alias, (initial, evidence) in states.items():
            selected = _general_snapshot_identity(evidence["general_snapshots"], day,
                checks=evidence["checks"], include_historical_baseline=receipt.requests[alias].include_historical_baseline)
            if selected is None or selected["seq"] != initial["seq"]:
                return None
            if day == cutoff and selected["baseline"] != initial["baseline"]:
                return None
            baseline = baseline or selected["baseline"]
        if day == cutoff and not baseline:
            return None
        if not baseline and transition is None:
            transition = day
    return transition


def _units(db, previous):
    rows = db.execute("select b.*,a.payload_json from item_builds b join item_completion_authorities a "
        "on a.build_id=b.id where b.name=? and b.id>? and "
        "json_extract(a.payload_json,'$.timing.predecessor')=? order by b.id",
        (previous["name"], previous["id"], previous["id"])).fetchall()
    result = []
    for row in rows:
        verified_completion_digest(db, row["id"])
        body = json.loads(row["payload_json"])["timing"]
        if body["unit"]:
            result.append((row, body))
    return result


def proof(owner: ItemAPI, db, previous, receipt, *, policy, start: date, end: date,
          commit_bound: int, check_bound: int, allowed_check: int | None = None):
    """Positive metadata-only proof; unsupported changes keep ordinary blocking."""
    cutoff = receipt.information_cutoff
    completed_end = _date_value(previous["end_date"])
    if (policy is None or cutoff is None or end <= cutoff
            or not receipt.requests or pending_dates(db, previous["id"])
            or output_check_bound(db, previous["id"]) is None):
        return None
    declaration = db.execute("select spec_hash from item_definitions where name=?", (previous["name"],)).fetchone()
    if declaration is None or declaration[0] != previous["definition_hash"]:
        return None
    authority = db.execute("select payload_json from item_completion_authorities where build_id=?",
        (previous["id"],)).fetchone()
    if authority is None or json.loads(authority[0])["kind"] != "native":
        return None
    if db.execute("select 1 from version_commits where source='items' and dataset=? "
            "and status='committed' and seq>? and seq<=? limit 1",
            (previous["name"], previous["result_commit"] or 0, commit_bound)).fetchone():
        return None
    for alias, request in receipt.requests.items():
        if (not isinstance(request, RawInput) or request.strict
                or alias not in policy.timing_neutral_inputs
                or any(value is not None for value in (request.start, request.end,
                    request.observation_start, request.observation_end))
                or not receipt.evidence[alias]["general_snapshots"]):
            return None
    payload = owner._lake.inputs._capture(db, receipt.requests, cutoff=end, max_commit=commit_bound,
        compact_aliases=set(), scoped_aliases=set(), retained_bounds_aliases=set(receipt.requests))
    # _capture observes the active input check boundary. A caller-provided fence
    # must never accidentally admit an event beyond its original view.
    if payload["max_check_id"] > check_bound:
        return None
    transition = _transition(receipt, payload, policy, end)
    if transition is None or transition <= completed_end:
        return None
    body = {"predecessor": previous["id"], "completion": verified_completion_digest(db, previous["id"]),
        "policy": asdict(policy), "available_date": str(transition),
        "start": str(start), "end": str(completed_end),
        "original_source": {"id": receipt.receipt_id, "digest": receipt.digest}}
    body["scope"] = _digest(body)
    units = _units(db, previous)
    permitted = {value["check_id"] for _, value in units if value["scope"] == body["scope"]}
    if allowed_check is not None:
        permitted.add(allowed_check)
    checks = db.execute("select id from version_checks where source='items' and dataset=? and id>? and id<=?",
        (previous["name"], output_check_bound(db, previous["id"]), check_bound)).fetchall()
    if any(row[0] not in permitted for row in checks):
        return None
    return body


def verify_authority(db, original, body):
    """Validate timing checkpoints without opening saved numerical values."""
    parent = db.execute("select * from item_builds where id=?", (body["predecessor"],)).fetchone()
    if (parent is None or parent["id"] >= original["id"] or parent["name"] != original["name"]
            or parent["definition_hash"] != original["definition_hash"]
            or _digest(dict(parent)) != body["completion"]
            or parent["frozen_receipt_id"] != body["original_source"]["id"]
            or original["frozen_receipt_id"] != body["current_source"]["id"]
            or original["result_commit"] != parent["result_commit"]
            or not parent["start_date"] <= original["start_date"] <= original["end_date"] <= parent["end_date"]):
        raise ValueError("timing predecessor binding differs")
    expected_scope = {key: body[key] for key in ("predecessor", "completion", "policy", "available_date",
        "start", "end", "original_source")}
    if (_digest(expected_scope) != body["scope"] or type(body["unit"]) is not bool
            or body["end"] != parent["end_date"]
            or not parent["start_date"] <= body["start"] <= original["start_date"]
            or not original["end_date"] <= body["end"]):
        raise ValueError("timing proof scope differs")
    sources = []
    for source in (body["original_source"], body["current_source"]):
        row = db.execute("select payload_json,digest from frozen_inputs where receipt_id=?", (source["id"],)).fetchone()
        if row is None or row[1] != source["digest"] or hashlib.sha256(row[0].encode()).hexdigest() != row[1]:
            raise ValueError("timing Source binding differs")
        header = json.loads(row[0])
        reader = MetadataReader(database=db)
        header["evidence"] = {alias: reader.value(reference) for alias, reference in header["evidence"].items()}
        sources.append(header)
    before, after = sources
    policy = {**body["policy"], "general_fields": tuple((key, tuple(fields)) for key, fields in body["policy"]["general_fields"]),
        "output_anchors": tuple(body["policy"]["output_anchors"]),
        "general_asset_local_inputs": tuple(body["policy"]["general_asset_local_inputs"])}
    registered = SimpleNamespace(information_cutoff=_date_value(before["information_cutoff"]),
        requests={key: input_from_payload(value) for key, value in before["requests"].items()},
        evidence=before["evidence"], max_commit=before["max_commit"])
    available = _date_value(body["available_date"])
    if (before["requests"] != after["requests"] or not registered.information_cutoff < available
            or available > _date_value(after["information_cutoff"])
            or _transition(registered, after, ProducerUpdatePolicy(**policy),
                _date_value(after["information_cutoff"])) != available):
        raise ValueError("timing selected-event proof differs")
    if body["unit"]:
        check = db.execute("select * from version_checks where id=?", (body["check_id"],)).fetchone()
        if (check is None or _digest(dict(check)) != body["check_digest"]
                or check["source"] != "items" or check["dataset"] != original["name"]
                or check["baseline"] or check["pit_date"] != body["available_date"]
                or check["input_receipt_id"] != original["frozen_receipt_id"]
                or json.loads(check["request_json"]) != _check_requests(body, original["start_date"], original["end_date"])):
            raise ValueError("timing check binding differs")
    else:
        cursor = _date_value(original["start_date"])
        for identifier in body["units"]:
            if type(identifier) is not int or not 0 < identifier < original["id"]:
                raise ValueError("timing unit predecessor differs")
            verified_completion_digest(db, identifier)
            row = db.execute("select b.*,a.payload_json from item_builds b join item_completion_authorities a "
                "on a.build_id=b.id where b.id=?", (identifier,)).fetchone()
            unit = json.loads(row["payload_json"])["timing"]
            if (unit["scope"] != body["scope"] or not unit["unit"]
                    or _date_value(row["start_date"]) != cursor):
                raise ValueError("timing unit coverage differs")
            cursor = _date_value(row["end_date"]) + timedelta(days=1)
        if cursor != _date_value(original["end_date"]) + timedelta(days=1):
            raise ValueError("timing prefix coverage is incomplete")


def _check_requests(body, first, last):
    return [{"timing_scope": body["scope"], "start": str(first), "end": str(last)}]


def _admit_saved(owner, previous, first, last, options):
    """Bound saved-output and comparison allocations before collecting values."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    from bagelquant_data.storage.recovery import _registered
    with owner._store.connect() as db:
        batches = db.execute("select b.*,s.schema_ipc from version_segments b join version_commits c "
            "on c.seq=b.commit_seq join immutable_schemas s on s.digest=b.schema_ref "
            "where c.source='items' and c.dataset=? and c.status='committed' "
            "and (b.min_observation is null or b.max_observation is null or "
            "b.min_observation<=? and b.max_observation>=?)",
            (previous["name"], str(last), str(first))).fetchall()
    estimate = 0
    for batch in batches:
        schema = pa.ipc.read_schema(pa.BufferReader(batch["schema_ipc"]))
        _, path = _registered(owner._store, batch["partition_path"], batch["commit_seq"], batch["content_hash"])
        with pq.ParquetFile(path) as reader:
            if reader.metadata.num_rows != batch["row_count"] or reader.schema_arrow != schema:
                raise RuntimeError("Saved timing unit original schema/row count differs")
            decoded = sum(reader.metadata.row_group(i).total_byte_size for i in range(reader.metadata.num_row_groups))
        estimate += max(decoded * 4, batch["file_size_bytes"] * 4,
            batch["row_count"] * max(256, len(schema) * 64))
    # Both the pinned saved snapshot and the ordinary native publisher's
    # comparison/hash/visibility working sets share this admitted buffer.
    if estimate * 8 > options.max_buffer_bytes:
        raise MemoryError("Saved timing unit exceeds max_buffer_bytes before value reads")


def execute(owner: ItemAPI, previous, original, receipt, action, *, policy, config, cancelled, progress):
    """Attest saved monthly output units, then certify their completed prefix."""
    from bagelquant_data.execution import ExecutionOptions
    options = config or ExecutionOptions()
    start, end, available = (_date_value(action[key]) for key in ("start", "end", "available_date"))
    def canceled():
        if cancelled and cancelled():
            raise CancelledError("DataItem timing attestation cancelled")
    def admitted(db, allowed_check=None):
        canceled()
        commit = db.execute("select coalesce(max(seq),0) from version_commits where status='committed'").fetchone()[0]
        check = db.execute("select coalesce(max(id),0) from version_checks").fetchone()[0]
        current = proof(owner, db, previous, original, policy=policy, start=start, end=receipt.information_cutoff,
            commit_bound=commit, check_bound=check, allowed_check=allowed_check)
        if current is None or current["available_date"] != str(available):
            raise RuntimeError("DataItem timing proof changed during publication; retry the update")
        return current
    def certify(db, body, first, last, *, check_id=None, units=None):
        body = {**body, "current_source": {"id": receipt.receipt_id, "digest": receipt.digest},
            "unit": units is None}
        if units is None:
            check = db.execute("select * from version_checks where id=?", (check_id,)).fetchone()
            body.update(check_id=check_id, check_digest=_digest(dict(check)))
        else:
            body["units"] = units
        identifier = db.execute("insert into item_builds(name,definition_hash,dependency_digest,start_date,end_date,"
            "input_commit,result_commit,status,created_at,frozen_receipt_id) values(?,?,?,?,?,?,?,'success',?,?)",
            (previous["name"], previous["definition_hash"], receipt.dependency_digest, str(first), str(last),
                receipt.max_commit, previous["result_commit"], datetime.now(UTC).isoformat(), receipt.receipt_id)).lastrowid
        save_completion(db, identifier, checked_target=str(available),
            authority={"kind": "native", "timing": body}, publish_node=units is not None)
        canceled()
        return identifier
    units = []
    first = start
    while first <= end:
        last = min(end, date(first.year, first.month, calendar.monthrange(first.year, first.month)[1]))
        with owner._store.connect() as db:
            db.execute("begin")
            body = admitted(db)
            existing = [(row, value) for row, value in _units(db, previous)
                if value["scope"] == body["scope"] and row["start_date"] == str(first) and row["end_date"] == str(last)]
        if existing:
            units.append(existing[-1][0]["id"])
        else:
            canceled()
            _admit_saved(owner, previous, first, last, options)
            saved = owner.read(previous["name"], start=first, end=last, view="snapshot",
                as_of=original.information_cutoff, max_commit=previous["result_commit"],
                max_check_id=output_check_bound_for(owner, previous)).collect()
            if saved.estimated_size() * 8 > options.max_buffer_bytes:
                raise MemoryError("Saved timing unit exceeds max_buffer_bytes")
            identifiers = []
            def checked(db, check_id):
                current = admitted(db, check_id)
                if current["scope"] != body["scope"]:
                    raise RuntimeError("DataItem timing declaration changed during publication")
                identifiers.append(certify(db, current, first, last, check_id=check_id))
            # Even empty units retain their exact acquisition date. A check is
            # the unit's publication fence, not evidence invented from row count.
            owner._publish_proven(previous["name"], saved.with_columns(pl.lit(available).alias("version_available_date")),
                historical_baseline=False, input_receipt=receipt, expected_definition_hash=previous["definition_hash"],
                cancelled=cancelled, on_unchanged_check=checked, available_date=available,
                check_requests=_check_requests(body, first, last))
            if len(identifiers) != 1:
                raise RuntimeError("Timing reuse must publish exactly one unchanged check")
            units.extend(identifiers)
        if progress:
            progress(ItemBuildReport(previous["name"], "running", 0, previous["result_commit"],
                receipt.max_commit, first, last, receipt.dependency_digest, receipt.receipt_id))
        first = last + timedelta(days=1)
    with owner._store.connect() as db:
        db.execute("begin immediate")
        certify(db, admitted(db), start, end, units=units)
    return ItemBuildReport(previous["name"], "success", 0, previous["result_commit"], receipt.max_commit,
        start, end, receipt.dependency_digest, receipt.receipt_id)


def output_check_bound_for(owner, previous):
    with owner._store.connect() as db:
        return output_check_bound(db, previous["id"])
