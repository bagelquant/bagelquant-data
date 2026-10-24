"""Explicit compact proof for asset-local General additions outside old anchors."""
from __future__ import annotations

from dataclasses import replace
import hashlib
import json
from typing import Any

import polars as pl

from bagelquant_data import input_index
from bagelquant_data.items.types import RawInput, input_payload
from bagelquant_data.query.raw import _date_value

VERSION = "general_asset_append.v1"


def _json(value: Any) -> str:
    from bagelquant_data.storage.immutable_metadata import canonical
    return canonical(value)


def _anchor_namespace(inputs, current, anchors, first, last, cutoff):
    from .inputs import _scalar_selection_evidence, _selection_key
    bindings = {}
    for name in anchors:
        request, day = inputs._selection_scope(current, name, start=first, end=last,
            as_of=cutoff, view="snapshot", strict=False)
        bindings[name] = {"evidence_key": _scalar_selection_evidence(current, name, request, day,
            fields=("asset_id",)), "request_key": _selection_key(request, day, values_only=True,
                fields=("asset_id",))}
    return hashlib.sha256(_json(bindings).encode()).hexdigest()


def locator(inputs, original, current, alias, fields, anchors, first, last, cutoff, *, scoped=True):
    from .inputs import _scalar_selection_evidence
    request = replace(original.requests[alias], view="snapshot")
    body = {
        "projection": VERSION, "request": input_payload(request), "original": original.receipt_id,
        "original_digest": original.digest, "fields": fields, "anchors": anchors,
        "start": str(first), "end": str(last), "cutoff": str(cutoff)}
    if scoped:
        body["anchor_evidence"] = _anchor_namespace(inputs, current, anchors, first, last, cutoff)
    return _scalar_selection_evidence(current, alias, request, cutoff, fields=fields), _json(body)


def _source(inputs, reference, *, max_bytes=None):
    source = inputs.get(reference["id"]) if max_bytes is None else inputs._summary_source(
        reference["id"], max_bytes=max_bytes)
    if source.digest != reference["digest"]:
        raise RuntimeError("General append proof's original Source checksum changed")
    return source


def _reference(source):
    return {"id": source.receipt_id, "digest": source.digest}


def _body(inputs, saved, evidence_key, request_key, *, options, historical_audit=False):
    from .inputs import _selected_general
    if saved is None or saved["kind"] != VERSION or saved["parents"] or saved["has_rows"]:
        raise RuntimeError("General append descriptor is missing or unsupported")
    body = json.loads(saved["identity"])
    required = {"original", "current", "alias", "fields", "anchors", "start", "end",
                "cutoff", "added_keys", "selector", "empty_anchors"}
    scoped = isinstance(body, dict) and "anchor_evidence" in body
    witnessed = isinstance(body, dict) and "selected_witness" in body
    if scoped:
        required.add("anchor_evidence")
    elif not historical_audit:
        raise RuntimeError("Current General append admission requires scoped anchor evidence")
    if witnessed:
        required.add("selected_witness")
    if (not isinstance(body, dict) or set(body) != required
            or witnessed and body["selected_witness"] is not True):
        raise RuntimeError("General append descriptor shape is damaged")
    original, current = (_source(inputs, body[name], max_bytes=options.max_buffer_bytes)
        for name in ("original", "current"))
    alias, fields, anchors = body["alias"], tuple(body["fields"]), tuple(body["anchors"])
    first, last, cutoff = (_date_value(body[name]) for name in ("start", "end", "cutoff"))
    if ((saved["source_receipt_id"], saved["source_receipt_digest"], saved["alias"])
            != (current.receipt_id, current.digest, alias)
            or not fields or "asset_id" not in fields or not anchors or len(set(anchors)) != len(anchors)
            or first > last or original.information_cutoff is None or last > original.information_cutoff
            or current.information_cutoff != cutoff
            or scoped and body["anchor_evidence"] != _anchor_namespace(inputs, current, anchors, first, last, cutoff)
            or locator(inputs, original, current, alias, fields, anchors, first, last, cutoff, scoped=scoped)
                != (evidence_key, request_key)
            or not body["added_keys"] or any(not isinstance(key, str) or not key.strip() for key in body["added_keys"])
            or sorted(set(body["added_keys"])) != body["added_keys"]):
        raise RuntimeError("General append descriptor Source/request binding is damaged")
    request = replace(original.requests[alias], view="snapshot")
    old_selected = _selected_general(original, alias, request, original.information_cutoff)
    new_selected = _selected_general(current, alias, request, cutoff)
    if (request.strict or old_selected is None or new_selected is None
            or old_selected["baseline"] or new_selected["baseline"]
            or new_selected["snapshot_date"] < original.information_cutoff
            or new_selected["seq"] <= old_selected["seq"]
            or current.evidence[alias]["definition_hash"] != original.evidence[alias]["definition_hash"]):
        raise RuntimeError("General append descriptor timing/definition is damaged")
    expected = {(side, name) for side in ("original", "current") for name in anchors}
    if (not isinstance(body["empty_anchors"], list) or len(body["empty_anchors"]) != len(expected)
            or any(set(item) != {"side", "alias", "identity", "evidence_key", "request_key"}
                for item in body["empty_anchors"])
            or {(item["side"], item["alias"]) for item in body["empty_anchors"]} != expected):
        raise RuntimeError("General append descriptor omits an output anchor")
    return body, original, current


def _anchor_capture(inputs, db, original, current, alias, anchors, cutoff, *, options):
    requests = {name: original.requests[name] for name in (alias, *anchors)}
    for name in anchors:
        request = requests[name]
        if (not isinstance(request, RawInput)
                or original.evidence[name]["definition"].get("update_type") != "by_date"):
            raise ValueError("General key anchors require by-date Raw inputs")
    payload = inputs._capture(db, requests, cutoff=cutoff, max_commit=current.max_commit,
        retained_bounds_aliases=set(requests), max_buffer_bytes=options.max_buffer_bytes)
    serialized = _json(payload)
    if len(serialized.encode()) * 16 > options.max_buffer_bytes // 4:
        raise MemoryError("General key anchor metadata exceeds max_buffer_bytes")
    source = inputs._receipt("general-key-anchors", payload, hashlib.sha256(serialized.encode()).hexdigest())
    if any(source.evidence[name]["definition_hash"] != original.evidence[name]["definition_hash"]
           or source.evidence[name]["schema_hash"] != original.evidence[name]["schema_hash"] for name in anchors):
        raise ValueError("General key anchor definition/schema changed")
    return source, payload


def _anchor_record(inputs, source, alias, first, last, cutoff, selector, *, db=None, coordinates=None, options=None):
    # Snapshot keeps a latest eligible row for every natural key, including all
    # extra-key combinations and NULL withdrawals. Empty means no visible
    # coordinate, not merely no finite numerical payload.
    request, day = inputs._selection_scope(source, alias, start=first, end=last,
        as_of=cutoff, view="snapshot", strict=False)
    metadata = {}
    value = inputs._indexed_selection(source, alias, request=request, cutoff=day,
        db=db, values_only=True, fields=("asset_id",), coordinates=coordinates,
        coordinate_key=selector, exact_summaries=True, summaries_only=True,
        metadata=metadata, options=options)
    return value, metadata


def _selector_valid(db, selector, budget):
    header = input_index.selector_header(db, selector, max_bytes=budget // 16)
    if header is None:
        return False
    if header["stored_bytes"] * 4 > budget // 4:
        raise MemoryError("General append selector exceeds max_buffer_bytes")
    payload = db.execute("select payload from input_projection_selectors where identity=?", (selector,)).fetchone()
    if payload is None or hashlib.sha256(payload[0]).hexdigest() != header["payload"]:
        raise RuntimeError("General append coordinate selector is damaged")
    return True


def positive(inputs, db, original, current, *, alias, fields, anchors, first, last, cutoff, options):
    """Read only a complete bound descriptor and its exact empty summary headers."""
    latest, _ = _anchor_capture(inputs, db, original, current, alias, anchors, cutoff, options=options)
    key = locator(inputs, original, latest, alias, fields, anchors, first, last, cutoff)
    saved = input_index.projection(db, *key, max_bytes=options.max_buffer_bytes // 4)
    if saved is None or saved["kind"] != VERSION:
        return False
    body, old, retained = _body(inputs, saved, *key, options=options)
    from .inputs import _selected_general
    selected = _selected_general(old, alias, replace(old.requests[alias], view="snapshot"), old.information_cutoff)
    if selected is not None and selected["attestation_id"] is not None and not body.get("selected_witness"):
        # Earlier optional proofs could compare an empty physical baseline to
        # a later selected check. Only a proof built with witness replay can
        # certify this class; explicit preparation/audit uses the original rows.
        return False
    if (old.digest != original.digest or locator(inputs, old, retained, alias, fields,
            anchors, first, last, cutoff) != key or body["anchors"] != list(anchors)
            or not body["added_keys"] or any(not isinstance(key, str) or not key for key in body["added_keys"])
            or sorted(set(body["added_keys"])) != body["added_keys"]):
        raise RuntimeError("General append proof binding is damaged")
    if not _selector_valid(db, body["selector"], options.max_buffer_bytes):
        return False
    for item in body["empty_anchors"]:
        source = old if item["side"] == "original" else latest
        day = original.information_cutoff if item["side"] == "original" else cutoff
        value, metadata = _anchor_record(inputs, source, item["alias"], first, last, day,
            body["selector"], db=db, options=options)
        if (value is None or value["has_rows"] or value["identity"] != item["identity"]
                or metadata["evidence_key"] != item["evidence_key"]
                or metadata["request_key"] != item["request_key"]):
            return False
    return True


def _general_frame(inputs, source, alias, fields, cutoff, options, check):
    from .inputs import _selection_batches
    request = replace(source.requests[alias], view="snapshot")
    pieces, retained = [], 0
    for batch in _selection_batches(source, alias, request, cutoff, fields=fields):
        check()
        piece = input_index.project_original(inputs._store, batch,
            max_bytes=max(1, options.max_buffer_bytes // 4-retained), fields=fields, check_canceled=check)
        retained += piece.estimated_size() * 12
        if retained > options.max_buffer_bytes // 4:
            raise MemoryError("General append projected rows exceed max_buffer_bytes")
        pieces.append(piece)
    if not pieces:
        raise ValueError("General append needs an original typed atomic schema")
    frame = pl.concat(pieces, how="diagonal_relaxed")
    # A selected baseline may be known through a later unchanged-content
    # check. Replay those same immutable witnesses as ordinary input reads
    # before resolving the complete General event; its original body alone
    # still has the acquisition date and cannot match the selected check.
    from .query.raw import _attested_versions
    from .inputs import _selected_general
    evidence = source.evidence[alias]
    selected = _selected_general(source, alias, request, cutoff)
    # Only this atomic state's witness is consumed. Replaying every old
    # same-content check would multiply rows that selection immediately drops.
    checks = [] if selected is None else [row for row in evidence["checks"]
        if row["id"] == selected["attestation_id"]]
    if frame.estimated_size() * (24 if checks else 12) > options.max_buffer_bytes // 4:
        raise MemoryError("General selected witness rows exceed max_buffer_bytes")
    frame = _attested_versions(frame.lazy(), checks, (),
        as_of_date=cutoff, max_check_id=source.max_check_id,
        max_buffer_bytes=options.max_buffer_bytes // 4, check_canceled=check).collect()
    if frame.estimated_size() * 12 > options.max_buffer_bytes // 4:
        raise MemoryError("General selected witness rows exceed max_buffer_bytes")
    evidence = {**source.evidence[alias], "general_snapshots": [
        {**snapshot, "schema_ipc": None} for snapshot in source.evidence[alias]["general_snapshots"]]}
    return inputs._select(frame, request, cutoff, evidence=evidence).select(fields)


def _added(before, after):
    if before.schema != after.schema or before.schema.get("asset_id") != pl.String:
        raise ValueError("General append consumed schema changed or asset_id is not String")
    old_null, new_null = before.filter(pl.col("asset_id").is_null()), after.filter(pl.col("asset_id").is_null())
    if input_index.identity(old_null) != input_index.identity(new_null):
        raise ValueError("General append changed the typed NULL-key multiset")
    old, new = before.filter(pl.col("asset_id").is_not_null()), after.filter(pl.col("asset_id").is_not_null())
    if (old["asset_id"].str.len_chars().eq(0).any() or new["asset_id"].str.len_chars().eq(0).any()
            or old["asset_id"].n_unique() != old.height or new["asset_id"].n_unique() != new.height):
        raise ValueError("General append requires unique nonempty nonnull asset keys")
    retained = new.join(old.select("asset_id"), on="asset_id", how="semi")
    if input_index.identity(before) != input_index.identity(pl.concat([retained, new_null])):
        raise ValueError("General append changed or removed original consumed tuples")
    return sorted(new.join(old.select("asset_id"), on="asset_id", how="anti")["asset_id"].to_list())


def prepare(inputs, original, alias, fields, *, anchors, first, last, end, options, check, progress):
    """Prepare only pure key additions and complete old/current empty anchors."""
    from .inputs import _selected_general
    from .items.incremental import general_projection_states
    anchors = tuple(anchors)
    if (not anchors or len(set(anchors)) != len(anchors) or "asset_id" not in fields
            or any(name not in original.requests or name == alias for name in anchors)
            or original.information_cutoff is None or first > last or last > original.information_cutoff):
        raise ValueError("General key preparation requires anchors and a finite completed prefix")
    request = replace(original.requests[alias], view="snapshot")
    if request.strict:
        raise ValueError("General key preparation requires known nonstrict General timing")
    with inputs._store.connect() as db:
        payload = inputs._capture(db, {alias: original.requests[alias]}, cutoff=end,
            retained_bounds_aliases={alias} if original.evidence[alias].get("retained_batch_bounds") else set(),
            max_buffer_bytes=options.max_buffer_bytes)
    states, reused, prepared = [], 0, 0
    for candidate, cutoff, _ceiling, _captured in general_projection_states(inputs, original, payload, alias=alias, end=end):
        check()
        old_selected = _selected_general(original, alias, request, original.information_cutoff)
        new_selected = _selected_general(candidate, alias, request, cutoff)
        if old_selected == new_selected:
            continue
        state: dict[str, Any] = {"as_of": str(cutoff), "status": "unsupported"}
        states.append(state)
        if (old_selected is None or new_selected is None or old_selected["baseline"] or new_selected["baseline"]
                or new_selected["snapshot_date"] < original.information_cutoff
                or new_selected["seq"] <= old_selected["seq"]
                or candidate.evidence[alias]["definition_hash"] != original.evidence[alias]["definition_hash"]):
            state["reason"] = "General append needs unchanged definition and later commit with nonregressing known timing"
            continue
        old_value = inputs._indexed_selection(original, alias, request=request,
            cutoff=original.information_cutoff, values_only=True, fields=fields,
            summaries_only=True, exact_summaries=True, options=options)
        new_value = inputs._indexed_selection(candidate, alias, request=request,
            cutoff=cutoff, values_only=True, fields=fields,
            summaries_only=True, exact_summaries=True, options=options)
        if old_value is not None and new_value is not None and old_value["identity"] == new_value["identity"]:
            state["status"] = "unchanged"
            reused += 1
            continue
        with inputs._store.connect() as db:
            if positive(inputs, db, original, candidate, alias=alias, fields=fields, anchors=anchors,
                    first=first, last=last, cutoff=cutoff, options=options):
                state["status"] = "known"
                reused += 1
                continue
            current, captured = _anchor_capture(inputs, db, original, candidate, alias, anchors, cutoff, options=options)
        with inputs._store.connect() as db:
            db.execute("begin immediate")
            current = inputs._register_capture(db, captured)
        before = _general_frame(inputs, original, alias, fields, original.information_cutoff, options, check)
        after = _general_frame(inputs, current, alias, fields, cutoff, options, check)
        if (before.estimated_size()+after.estimated_size()) * 16 > options.max_buffer_bytes // 4:
            raise MemoryError("General append comparison exceeds max_buffer_bytes")
        try:
            added = _added(before, after)
        except ValueError as error:
            state["reason"] = str(error)
            continue
        if not added:
            state["status"] = "unchanged"
            continue
        count = ((last-first).days+1) * len(added)
        if count * 256 + sum(len(key.encode()) * 16 for key in added) > options.max_buffer_bytes // 4:
            raise MemoryError("General append coordinate grid exceeds max_buffer_bytes")
        coordinates = pl.DataFrame({"time": pl.date_range(first, last, eager=True)}).join(
            pl.DataFrame({"asset_id": added}), how="cross").select("time", "asset_id")
        selector = inputs.coordinate_selector_identity(coordinates, config=options, check_canceled=check)
        records = []
        for side, source, day in (("original", original, original.information_cutoff), ("current", current, cutoff)):
            for anchor in anchors:
                check()
                inputs.index_selection(source, anchor, start=first, end=last, as_of=day, view="snapshot", strict=False,
                    fields=("asset_id",), coordinates=coordinates, config=options, check_canceled=check)
                value, metadata = _anchor_record(inputs, source, anchor, first, last, day, selector,
                    coordinates=coordinates, options=options)
                if value is None or value["has_rows"]:
                    break
                records.append({"side": side, "alias": anchor, "identity": value["identity"],
                    "evidence_key": metadata["evidence_key"], "request_key": metadata["request_key"]})
            if len(records) != (len(anchors) if side == "original" else 2*len(anchors)):
                break
        if len(records) != 2*len(anchors):
            state["reason"] = "Added General keys have old-prefix anchor coordinates"
            continue
        body = {"original": _reference(original), "current": _reference(current), "alias": alias,
            "fields": list(fields), "anchors": list(anchors), "start": str(first), "end": str(last),
            "cutoff": str(cutoff), "added_keys": added, "selector": selector, "empty_anchors": records,
            "anchor_evidence": _anchor_namespace(inputs, current, anchors, first, last, cutoff),
            "selected_witness": True}
        key = locator(inputs, original, current, alias, fields, anchors, first, last, cutoff)
        with inputs._store.connect() as db:
            db.execute("begin immediate")
            check()
            input_index.save_projection(db, *key, {"kind": VERSION, "identity": _json(body), "parents": [], "has_rows": False},
                receipt_id=current.receipt_id, receipt_digest=current.digest, alias=alias)
            check()
        state.update(status="known", added_keys=added)
        prepared += 1
        if progress is not None:
            progress({"stage": "general_key_append_preparation", "prepared": prepared, "reused": reused})
    return {"states": states, "prepared": prepared, "reused": reused}


def references(inputs, saved):
    """Follow both original Sources before any explicit Original byte audit."""
    if saved is None or saved["kind"] != VERSION:
        raise RuntimeError("General append descriptor is missing or unsupported")
    body = json.loads(saved["identity"])
    return _source(inputs, body["original"]), _source(inputs, body["current"])


def audit(inputs, db, evidence_key, request_key, saved, *, options, check):
    """Recompute key append and every empty selected anchor, not a scalar hash."""
    body, original, current = _body(inputs, saved, evidence_key, request_key, options=options, historical_audit=True)
    alias, fields, anchors = body["alias"], tuple(body["fields"]), tuple(body["anchors"])
    first, last, cutoff = (_date_value(body[name]) for name in ("start", "end", "cutoff"))
    if locator(inputs, original, current, alias, fields, anchors, first, last, cutoff,
            scoped="anchor_evidence" in body) != (evidence_key, request_key):
        raise RuntimeError("General append descriptor binding differs from original evidence")
    before = _general_frame(inputs, original, alias, fields, original.information_cutoff, options, check)
    after = _general_frame(inputs, current, alias, fields, cutoff, options, check)
    if _added(before, after) != body["added_keys"]:
        raise RuntimeError("General append changed keys differ from original evidence")
    coordinates = input_index.read_selector(db, body["selector"], max_bytes=options.max_buffer_bytes // 4, check_canceled=check)
    expected = pl.DataFrame({"time": pl.date_range(first, last, eager=True)}).join(
        pl.DataFrame({"asset_id": body["added_keys"]}), how="cross").select("time", "asset_id")
    if input_index.identity(coordinates) != input_index.identity(expected):
        raise RuntimeError("General append selector differs from the complete old prefix")
    for item in body["empty_anchors"]:
        source = original if item["side"] == "original" else current
        day = original.information_cutoff if item["side"] == "original" else cutoff
        request, day = inputs._selection_scope(source, item["alias"], start=first, end=last,
            as_of=day, view="snapshot", strict=False)
        saved_child, metadata = _anchor_record(inputs, source, item["alias"], first, last, day,
            body["selector"], db=db, options=options)
        if (saved_child is None or metadata["evidence_key"] != item["evidence_key"]
                or metadata["request_key"] != item["request_key"]):
            raise RuntimeError("General append child locator differs from original evidence")
        value = inputs._indexed_selection(source, item["alias"], request=request, cutoff=day,
            db=db, recompute=True, originals=True, values_only=True, fields=("asset_id",),
            coordinates=coordinates, options=options, check_canceled=check)
        if value is None or value["has_rows"] or value["identity"] != item["identity"]:
            raise RuntimeError("General append anchor absence differs from original evidence")
