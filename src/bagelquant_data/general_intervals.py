"""Original-bound compact impact for producer-declared General intervals."""
from __future__ import annotations

from dataclasses import replace
from datetime import date, timedelta
import json

import polars as pl

from . import input_index
from .general_key_appends import _general_frame
from .items.types import input_payload
from .query.raw import _date_value
from .storage.immutable_metadata import canonical

VERSION = "general_interval_impact.v1"


def locator(original, current, alias, fields, interval, cutoff):
    from .inputs import _scalar_selection_evidence
    request = replace(original.requests[alias], view="snapshot")
    return _scalar_selection_evidence(current, alias, request, cutoff, fields=fields), canonical({
        "projection": VERSION, "request": input_payload(request), "fields": fields,
        "interval": interval, "original": original.receipt_id, "original_digest": original.digest,
        "cutoff": str(cutoff)})


def _ranges(before, after, interval):
    if before.schema != after.schema:
        raise ValueError("General interval consumed schema changed")
    first_field, last_field = interval
    if any(before.schema[name] not in (pl.String, pl.Date, pl.Null) for name in interval):
        raise ValueError("General interval dates require Date or compact/ISO String fields")
    # Polars tuple equality merges signed floating zero. The existing typed
    # row identity preserves that distinction and complete tuple correlation.
    from .pipeline.versions import _payload_hashes
    fields = [*before.columns, "_typed_tuple"]
    before = before.with_columns(_payload_hashes(before, before.columns).alias("_typed_tuple"))
    after = after.with_columns(_payload_hashes(after, after.columns).alias("_typed_tuple"))
    counts = (before.group_by(fields).len(name="_before")
        .join(after.group_by(fields).len(name="_after"), on=fields, how="full",
            coalesce=True, nulls_equal=True))
    unchanged = counts.filter(pl.col("_before").fill_null(0) == pl.col("_after").fill_null(0)).select("_typed_tuple")
    # Canonical scalar encoding intentionally merges NaN representations.
    # Independently prove every supposedly unchanged tuple with exact Arrow
    # identity, including when another tuple has an ordinary visible change.
    if input_index.identity(before.join(unchanged, on="_typed_tuple", how="semi")) != input_index.identity(
            after.join(unchanged, on="_typed_tuple", how="semi")):
        raise ValueError("General interval exact unchanged-tuple evidence is unsupported")
    changes = counts.filter(pl.col("_before").fill_null(0) != pl.col("_after").fill_null(0))
    def parsed(name):
        return (pl.col(name).cast(pl.String)
            .str.replace_all(r"^(\d{4})(\d{2})(\d{2})$", r"${1}-${2}-${3}")
            .str.to_date("%Y-%m-%d", strict=False))
    changes = changes.with_columns(parsed(first_field).alias("_first"), parsed(last_field).alias("_last"))
    if changes.filter(pl.col("_first").is_null()
            | (pl.col(last_field).is_not_null() & pl.col("_last").is_null())).height:
        raise ValueError("General interval changed rows have missing/invalid dates")
    intervals = changes.select("_first", "_last").unique().sort("_first", "_last", nulls_last=True)
    result = []
    for first, exclusive_end in intervals.iter_rows():
        if exclusive_end is not None and exclusive_end <= first:
            continue
        last = date.max if exclusive_end is None else exclusive_end - timedelta(days=1)
        if result and (result[-1][1] == date.max or first <= result[-1][1] + timedelta(days=1)):
            result[-1] = result[-1][0], max(result[-1][1], last)
        else:
            result.append((first, last))
    return [[str(first), str(last)] for first, last in result]


def _source(inputs, reference, options):
    source = inputs._summary_source(reference["id"], max_bytes=options.max_buffer_bytes)
    if source.digest != reference["digest"]:
        raise RuntimeError("General interval proof's original Source checksum changed")
    return source


def _body(inputs, saved, key, *, options):
    from .inputs import _selected_general
    if saved is None or saved["kind"] != VERSION or saved["parents"] or saved["has_rows"]:
        raise RuntimeError("General interval descriptor is missing or unsupported")
    body = json.loads(saved["identity"])
    if (not isinstance(body, dict) or set(body) != {
            "original", "current", "alias", "fields", "interval", "cutoff", "ranges"}):
        raise RuntimeError("General interval descriptor shape is damaged")
    original, current = (_source(inputs, body[name], options) for name in ("original", "current"))
    alias, fields, interval = body["alias"], tuple(body["fields"]), tuple(body["interval"])
    cutoff = _date_value(body["cutoff"])
    if (not fields or len(interval) != 2 or interval[0] == interval[1]
            or not set(interval).issubset(fields) or original.information_cutoff is None
            or current.information_cutoff is None or cutoff > current.information_cutoff
            or (saved["source_receipt_id"], saved["source_receipt_digest"], saved["alias"])
                != (current.receipt_id, current.digest, alias)
            or locator(original, current, alias, fields, interval, cutoff) != key
            or original.requests[alias] != current.requests[alias]
            or original.evidence[alias]["definition_hash"] != current.evidence[alias]["definition_hash"]):
        raise RuntimeError("General interval descriptor Source/request binding is damaged")
    request = replace(original.requests[alias], view="snapshot")
    before = _selected_general(original, alias, request, min(cutoff, original.information_cutoff))
    after = _selected_general(current, alias, request, cutoff)
    if (request.strict or before is None or after is None
            or before["baseline"] != after["baseline"]):
        raise RuntimeError("General interval descriptor timing is unsupported")
    previous = None
    for value in body["ranges"]:
        if not isinstance(value, list) or len(value) != 2:
            raise RuntimeError("General interval descriptor ranges are damaged")
        first, last = map(_date_value, value)
        if first > last or previous is not None and first <= previous:
            raise RuntimeError("General interval descriptor ranges are damaged")
        previous = last
    return body, original, current


def impact(inputs, db, original, current, *, alias, fields, interval, cutoff, options):
    """Resolve registered impact using metadata only; missing evidence is unknown."""
    key = locator(original, current, alias, fields, interval, cutoff)
    saved = input_index.projection(db, *key, max_bytes=options.max_buffer_bytes // 4)
    if saved is None:
        return None
    body, retained, _current = _body(inputs, saved, key, options=options)
    if retained.digest != original.digest:
        raise RuntimeError("General interval descriptor predecessor changed")
    return [tuple(map(_date_value, window)) for window in body["ranges"]]


def prepare(inputs, original, alias, fields, interval, *, end, options, check, progress):
    """Compare explicit typed atomic states once and save only changed-date unions."""
    from .inputs import _selected_general
    from .items.incremental import general_projection_states
    if (len(interval) != 2 or interval[0] == interval[1] or not set(interval).issubset(fields)
            or original.information_cutoff is None):
        raise ValueError("General interval preparation requires consumed start/end fields and original cutoff")
    request = replace(original.requests[alias], view="snapshot")
    with inputs._store.connect() as db:
        payload = inputs._capture(db, {alias: original.requests[alias]}, cutoff=end,
            retained_bounds_aliases={alias} if original.evidence[alias].get("retained_batch_bounds") else set(),
            max_buffer_bytes=options.max_buffer_bytes)
    states, seen, prepared, reused = [], set(), 0, 0
    for candidate, cutoff, _ceiling, captured in general_projection_states(inputs, original, payload, alias=alias, end=end):
        check()
        before = _selected_general(original, alias, request, min(cutoff, original.information_cutoff))
        after = _selected_general(candidate, alias, request, cutoff)
        if before == after:
            continue
        key = locator(original, candidate, alias, fields, interval, cutoff)
        if key in seen:
            continue
        seen.add(key)
        state = {"as_of": str(cutoff), "max_commit": candidate.max_commit, "status": "unsupported"}
        states.append(state)
        if request.strict or before is None or after is None or before["baseline"] != after["baseline"]:
            state["reason"] = "General interval impact requires unchanged selected visibility/baseline timing"
            continue
        with inputs._store.connect() as db:
            known = impact(inputs, db, original, candidate, alias=alias, fields=fields,
                interval=interval, cutoff=cutoff, options=options)
        if known is not None:
            state.update(status="known", ranges=[[str(a), str(b)] for a, b in known])
            reused += 1
            continue
        try:
            with options.admission("columnar"):
                old = _general_frame(inputs, original, alias, fields, min(cutoff, original.information_cutoff), options, check)
                new = _general_frame(inputs, candidate, alias, fields, cutoff, options, check)
                if (old.estimated_size() + new.estimated_size()) * 32 > options.max_buffer_bytes // 4:
                    raise MemoryError("General interval comparison exceeds max_buffer_bytes")
                ranges = _ranges(old, new, interval)
        except ValueError as error:
            state["reason"] = str(error)
            continue
        with inputs._store.connect() as db:
            db.execute("begin immediate")
            check()
            current = inputs._register_capture(db, captured)
            body = {"original": {"id": original.receipt_id, "digest": original.digest},
                "current": {"id": current.receipt_id, "digest": current.digest},
                "alias": alias, "fields": list(fields), "interval": list(interval),
                "cutoff": str(cutoff), "ranges": ranges}
            encoded = canonical(body)
            if len(encoded.encode()) * 16 > options.max_buffer_bytes // 4:
                raise MemoryError("General interval impact metadata exceeds max_buffer_bytes")
            input_index.save_projection(db, *key, {"kind": VERSION, "identity": encoded,
                "parents": [], "has_rows": False}, receipt_id=current.receipt_id,
                receipt_digest=current.digest, alias=alias)
            check()
        prepared += 1
        state.update(status="known", ranges=ranges)
        if progress is not None:
            progress({"stage": "general_interval_preparation", "prepared": prepared, "reused": reused})
    return {"states": states, "prepared": prepared, "reused": reused}


def references(inputs, saved, *, options):
    body = json.loads(saved["identity"])
    return tuple(_source(inputs, body[name], options) for name in ("original", "current"))


def audit(inputs, db, evidence_key, request_key, saved, *, options, check):
    body, original, current = _body(inputs, saved, (evidence_key, request_key), options=options)
    alias, fields = body["alias"], tuple(body["fields"])
    cutoff = _date_value(body["cutoff"])
    with options.admission("columnar"):
        old = _general_frame(inputs, original, alias, fields, min(cutoff, original.information_cutoff), options, check)
        new = _general_frame(inputs, current, alias, fields, cutoff, options, check)
        if (old.estimated_size() + new.estimated_size()) * 32 > options.max_buffer_bytes // 4:
            raise MemoryError("General interval audit exceeds max_buffer_bytes")
        ranges = _ranges(old, new, tuple(body["interval"]))
    if ranges != body["ranges"]:
        raise RuntimeError("General interval impact differs from original evidence")
