"""Read-only output impact plans backed by registered completion evidence."""
from __future__ import annotations

from dataclasses import replace
from datetime import date, timedelta
from bisect import bisect_right
from typing import TYPE_CHECKING, cast


from bagelquant_data import input_index
from bagelquant_data.execution import ExecutionOptions
from bagelquant_data.core.types import DateLike
from bagelquant_data.query.raw import _date_value
from bagelquant_data.inputs import _json
from .completion import pending_dates
from .output_reuse import _BATCH_COLUMNS, original_binding, recorded_dates
from .types import RawInput
import hashlib

if TYPE_CHECKING:
    from .api import ItemAPI
    from .types import ProducerUpdatePolicy


def _merge(windows: list[tuple[date, date]]) -> list[tuple[date, date]]:
    result: list[tuple[date, date]] = []
    for first, last in sorted(windows):
        if result and first <= result[-1][1] + timedelta(days=1):
            result[-1] = result[-1][0], max(last, result[-1][1])
        else:
            result.append((first, last))
    return result


def _neutral_timing_preserved(owner: ItemAPI, db, receipt, *, name: str,
                              alias: str, checks, end: date, commit_bound: int) -> bool:
    """Prove unchanged output timing for a producer-declared neutral General input.

    Already-known General timing is alias-local; other aliases are planned
    independently. First promotion requires current original information and
    a visible baseline surviving every new timing boundary.
    """
    request = receipt.requests[alias]
    if not isinstance(request, RawInput) or request.strict:
        return False
    evidence = receipt.evidence[alias]
    if not evidence["general_snapshots"]:
        # A declared timing-neutral producer ignores acquisition provenance,
        # but first visibility/baseline promotion still matters. Each newly
        # witnessed original baseline record must already have a known copy
        # at an equal or earlier availability in the completed Source. Mixed
        # checks cannot be judged by their single visible_commit header.
        original_events = {row["content_hash"] for row in evidence["record_checks"] if row.get("event")}
        events = owner._store.visibility_headers(db,evidence["source"],evidence["dataset"])
        return all(row["content_hash"] in original_events for row in events if row["check_id"] in {check["id"] for check in checks})
    from .pit import _general_snapshot_identity
    original_general = _general_snapshot_identity(evidence["general_snapshots"], receipt.information_cutoff,
        checks=evidence["checks"], strict=request.strict,
        include_historical_baseline=request.include_historical_baseline)
    # A later verification of already-known identical General content cannot
    # promote unknown timing again. Only the declared neutral alias is covered;
    # every other dependency still follows ordinary numerical/timing impact.
    if original_general is not None and original_general["baseline"] is False:
        snapshot = next(value for value in evidence["general_snapshots"]
            if value["seq"] == original_general["seq"])
        known_dates = ([] if snapshot["baseline"] else [_date_value(snapshot["pit_date"])])
        known_dates.extend(max(_date_value(snapshot["pit_date"]), _date_value(check["pit_date"]))
            for check in evidence["checks"] if not check["baseline"]
            and check["visible_commit"] == snapshot["seq"]
            and _date_value(check["pit_date"]) <= receipt.information_cutoff)
        if not known_dates:
            return False
        known_since = min(known_dates)
        if any(_date_value(check["pit_date"]) <= receipt.information_cutoff and (
                check["visible_commit"] != snapshot["seq"]
                or max(_date_value(snapshot["pit_date"]), _date_value(check["pit_date"])) < known_since)
                for check in checks):
            return False
        # completed_actions already rejects new General content and changed
        # declarations. The immutable selected event is sufficient here;
        # whole-receipt currentness can be unknown solely because an unrelated
        # dependency lacks an old index or has a separately planned revision.
        current_checks = [*evidence["checks"], *[dict(check) for check in checks]]
        for cutoff in sorted({_date_value(check["pit_date"]) for check in checks} | {end}):
            current_general = _general_snapshot_identity(evidence["general_snapshots"], cutoff,
                checks=current_checks, strict=request.strict,
                include_historical_baseline=request.include_historical_baseline)
            if (current_general is None or current_general["seq"] != original_general["seq"]
                    or current_general["baseline"] is not False):
                return False
        return True
    if any(_date_value(check["pit_date"]) <= receipt.information_cutoff for check in checks):
        return False
    if db.execute("select 1 from version_commits where source='items' and dataset=? "
                  "and status='committed' and baseline=0 limit 1", (name,)).fetchone():
        return False
    if not owner._lake.inputs.is_current(receipt):
        return False
    captured = owner._lake.inputs._capture(db, receipt.requests, cutoff=end, max_commit=commit_bound,
        compact_aliases={key for key, value in receipt.evidence.items() if "full_commit_checks" in value},
        scoped_aliases={key for key, value in receipt.evidence.items() if value.get("scoped_batches")},
        retained_bounds_aliases=set(receipt.requests))
    current = owner._lake.inputs._receipt("timing-impact", captured, hashlib.sha256(_json(captured).encode()).hexdigest())
    boundaries = {_date_value(check["pit_date"]) for check in checks} | {end}
    for cutoff in sorted(boundaries):
        if not any(owner._lake.inputs._baseline_at(current, key, cutoff) for key in current.requests):
            return False
    return True


def general_projection_states(inputs, receipt, payload, *, alias: str, end: date):
    """Share the exact atomic states required by planning and explicit preparation."""
    assert receipt.information_cutoff is not None
    old_cutoff, request = receipt.information_cutoff, receipt.requests[alias]
    assert isinstance(request, RawInput)
    current = inputs._receipt("general-consumption-impact", payload,
        hashlib.sha256(_json(payload).encode()).hexdigest())
    evidence = current.evidence[alias]
    boundaries = {min(old_cutoff, end), end}
    for snapshot in evidence["general_snapshots"]:
        day = _date_value(snapshot["pit_date"])
        if (snapshot["seq"] > receipt.max_commit or day > old_cutoff) and day <= end:
            boundaries.add(day)
    snapshots = {row["seq"]: row for row in evidence["general_snapshots"]}
    for check in evidence["audit_checks"]:
        if check["baseline"] or (check["id"] <= receipt.max_check_id
                and _date_value(check["pit_date"]) <= old_cutoff):
            continue
        snapshot = snapshots.get(check["visible_commit"])
        day = _date_value(check["pit_date"])
        if snapshot is not None:
            day = max(day, _date_value(snapshot["pit_date"]))
        if day <= end:
            boundaries.add(day)
    for cutoff in sorted(boundaries):
        yield current, cutoff, None, payload
    for snapshot in evidence["general_snapshots"]:
        day = _date_value(snapshot["pit_date"])
        if not (snapshot["seq"] > receipt.max_commit or day > old_cutoff):
            continue
        if day > end and not (snapshot["baseline"] and request.include_historical_baseline):
            continue
        # Equal dates can contain A -> B -> A. Preserve each original ceiling.
        ceiling = snapshot["seq"]
        bounded_evidence = {**evidence,
            "general_snapshots": [row for row in evidence["general_snapshots"] if row["seq"] <= ceiling],
            "batches": [row for row in evidence["batches"] if row["commit_seq"] <= ceiling]}
        for name in ("checks", "audit_checks", "empty_checks"):
            bounded_evidence[name] = [row for row in evidence[name]
                if row["visible_commit"] is None or row["visible_commit"] <= ceiling]
        bounded_payload = {**payload, "max_commit": ceiling, "evidence": {alias: bounded_evidence}}
        bounded = inputs._receipt("general-consumption-impact", bounded_payload,
            hashlib.sha256(_json(bounded_payload).encode()).hexdigest())
        yield bounded, min(day, end), ceiling, bounded_payload


def _general_fields_problem(owner: ItemAPI, db, receipt, *, alias: str, fields: tuple[str, ...],
                            end: date, commit_bound: int, policy: ProducerUpdatePolicy,
                            completed_start: date, completed_end: date,
                            changed: bool = True) -> str | None:
    """Admit whole atomic consumed equality, separately from timing provenance."""
    from .pit import _general_snapshot_identity
    inputs = owner._lake.inputs
    request, original = receipt.requests[alias], receipt.evidence[alias]
    assert isinstance(request, RawInput) and receipt.information_cutoff is not None
    context = inputs._read_context()
    budget = (context.options if context is not None else ExecutionOptions()).max_buffer_bytes
    old_cutoff = receipt.information_cutoff
    def selected(evidence, cutoff):
        return _general_snapshot_identity(evidence["general_snapshots"], cutoff,
            checks=evidence["checks"], strict=request.strict,
            include_historical_baseline=request.include_historical_baseline)
    initial = selected(original, old_cutoff)
    payload = inputs._capture(db, {alias: request}, cutoff=end, max_commit=commit_bound,
        compact_aliases={alias} if "full_commit_checks" in original else set(),
        scoped_aliases=set(), retained_bounds_aliases={alias} if original.get("retained_batch_bounds") else set(),
        max_buffer_bytes=budget)
    serialized = _json(payload)
    if len(serialized.encode()) * 16 > budget // 4:
        return f"{alias}: General consumed projection metadata exceeds max_buffer_bytes; use an admitted read_context"
    current = inputs._receipt("general-consumption-impact", payload,
        hashlib.sha256(serialized.encode()).hexdigest())
    try:
        for source, cutoff in ((receipt, old_cutoff), (current, end)):
            if selected(source.evidence[alias], cutoff) is not None:
                inputs._validate_selection_fields(source, alias, fields,
                    request=replace(request, view="snapshot"), cutoff=cutoff)
    except (ValueError, MemoryError) as error:
        return f"{alias}: General consumed projection schema cannot be admitted: {error}"
    if not changed:
        # No selected event means no new field-absence authority. Preserve the
        # existing unchanged historical completion rather than inventing one.
        return None
    if (alias not in policy.timing_neutral_inputs or request.strict
            or initial is None or initial["baseline"]):
        return f"{alias}: changed General consumed fields require an already-known timing-neutral snapshot; explicit timing-impact proof is required"
    # Only new/captured-future transitions need comparison. Include every
    # relevant event, rather than comparing the last A in an A -> B -> A path.
    # Backdated new events are checked at their original availability as well.
    for candidate, cutoff, ceiling, _ in general_projection_states(inputs, receipt, payload, alias=alias, end=end):
        original_cutoff = min(cutoff, old_cutoff)
        before, after = selected(original, original_cutoff), selected(candidate.evidence[alias], cutoff)
        if ((before is None) != (after is None)
                or before is not None and after is not None and before["baseline"] != after["baseline"]):
            return f"{alias}: General first visibility/baseline timing changed at {cutoff}; explicit timing-impact proof is required"
        try:
            original_request = replace(request,view="snapshot")
            old_token = inputs._range_selection(receipt,alias,original_request,original_cutoff,fields=fields,values_only=True)["identity"]
            new_token = inputs._range_selection(candidate,alias,original_request,cutoff,fields=fields,values_only=True)["identity"]
            equivalent = True if old_token == new_token else inputs._selection_equivalent(receipt, candidate, alias,
                as_of=original_cutoff, current_as_of=cutoff, view="snapshot", fields=fields, summaries_only=True)
        except (ValueError, MemoryError) as error:
            return f"{alias}: General consumed projection cannot be admitted at {cutoff}: {error}"
        bounded_source = "" if ceiling is None else f" (max_commit={ceiling})"
        if equivalent is None:
            return (f"{alias}: General consumed projection summary is missing at {cutoff}; "
                f"run explicit inputs.index_selection(original receipt {receipt.receipt_id}, {alias!r}, "
                f"view='snapshot', fields={fields!r}, as_of={str(original_cutoff)!r}) and the same "
                f"projection on an original-bound current frozen receipt{bounded_source} at as_of={str(cutoff)!r}")
        if not equivalent and alias in policy.general_asset_local_inputs:
            from bagelquant_data.general_key_appends import positive
            options = context.options if context is not None else ExecutionOptions()
            try:
                equivalent = positive(inputs, db, receipt, candidate, alias=alias, fields=fields,
                    anchors=policy.output_anchors, first=completed_start, last=completed_end,
                    cutoff=cutoff, options=options)
            except (ValueError, MemoryError) as error:
                return f"{alias}: General asset-local append proof cannot be admitted: {error}"
        if not equivalent:
            return f"{alias}: General consumed fields changed at {cutoff}{bounded_source}; explicit selected-input impact resolution is required"
    return None


def completed_actions(owner: ItemAPI, previous, *, start: date, end: date,
                      policy: ProducerUpdatePolicy | None) -> list[dict[str, str]]:
    """Do not infer completion from rows, or fall back to reading original IPC."""
    receipt = owner._lake.inputs.get(previous["frozen_receipt_id"])
    through = min(end, _date_value(previous["end_date"]))
    windows: list[tuple[date, date]] = []
    unknown: str | None = None
    if receipt.information_cutoff is None:
        return [{"mode": "blocked", "start": start.isoformat(), "end": end.isoformat(),
                 "reason": "completed receipt lacks an information boundary; explicit impact proof is required"}]
    with owner._store.connect() as db:
        if not db.in_transaction:
            db.execute("begin")
        commit_bound = int(db.execute("select coalesce(max(seq),0) from version_commits where status='committed'").fetchone()[0])
        check_bound = int(db.execute("select coalesce(max(id),0) from version_checks").fetchone()[0])
        boundary = owner._lake.inputs._boundary
        if boundary is not None:
            commit_bound = min(commit_bound, boundary.max_commit)
            if boundary.max_check_id is not None:
                check_bound = min(check_bound, boundary.max_check_id)
            if boundary.as_of is not None and end > boundary.as_of:
                return [{"mode": "blocked", "start": start.isoformat(), "end": end.isoformat(),
                    "reason": "requested output exceeds the active frozen information boundary"}]
        if commit_bound < receipt.max_commit or check_bound < receipt.max_check_id:
            return [{"mode": "blocked", "start": start.isoformat(), "end": end.isoformat(),
                "reason": "active input boundary precedes completed evidence; explicit impact proof is required"}]
        anchor_empty = False
        if policy is not None and policy.output_anchors:
            anchor_empty = True
        for alias in (() if policy is None else policy.output_anchors):
            anchor = receipt.requests.get(alias)
            if (not isinstance(anchor, RawInput)
                    or receipt.evidence[alias]["definition"]["update_type"] != "by_date"):
                raise ValueError("Output anchor requires a declared by-date Raw input")
            # Absence is a complete coordinate proof, not an output-row count.
            # A late observation version in the old interval prevents reuse.
            anchor_batches = db.execute("select " + ",".join("b." + field for field in _BATCH_COLUMNS.split(","))
                + ",c.baseline from version_segments b join version_commits c on c.seq=b.commit_seq "
                "where c.source=? and c.dataset=? and c.status='committed' and c.seq<=?",
                (anchor.source, anchor.dataset, commit_bound)).fetchall()
            registered = {(batch["commit_seq"], batch["partition_path"]): batch for batch in anchor_batches}
            for original in receipt.evidence[alias]["batches"]:
                if original["row_count"] and (original.get("min_observation") is None
                        or original.get("max_observation") is None):
                    anchor_empty = False
                current = registered.get((original["commit_seq"], original["partition_path"]))
                if current is None or any(original[field] != current[field] for field in
                        (*_BATCH_COLUMNS.split(","), "baseline") if field in original):
                    # A current header cannot disprove coordinates retained in
                    # the immutable original receipt. Corruption is explicit.
                    raise RuntimeError("Output anchor original batch binding changed; explicit integrity recovery is required")
            for batch in anchor_batches:
                if not batch["row_count"]:
                    continue
                if (not batch["baseline"] and batch["min_available"] is not None
                        and _date_value(batch["min_available"]) > end):
                    continue
                if (batch["min_observation"] is None or batch["max_observation"] is None
                        or _date_value(batch["min_observation"]) <= through
                        and _date_value(batch["max_observation"]) >= start):
                    anchor_empty = False
                    break
        # Later direct publications do not inherit the completed-range proof.
        # Rebuild their bounded output coordinates before issuing a new proof.
        batch_columns = ",".join("b." + field for field in _BATCH_COLUMNS.split(","))
        own = db.execute("select " + batch_columns + " from version_segments b join version_commits c on c.seq=b.commit_seq "
            "where c.source='items' and c.dataset=? and c.status='committed' and b.commit_seq>? "
            "and b.commit_seq<=?", (previous["name"], previous["result_commit"] or 0, commit_bound)).fetchall()
        original_bound = original_binding(db, previous) if own else ""
        for batch in own:
            if batch["min_available"] is not None and _date_value(batch["min_available"]) > end:
                continue
            dates = recorded_dates(db, previous, batch, end=end, max_check=check_bound, original_bound=original_bound)
            if dates is not None:
                windows.extend((day, day) for day in dates if start <= day <= through)
                continue
            if batch["min_observation"] is None or batch["max_observation"] is None:
                if batch["row_count"]:
                    return [{"mode": "blocked", "start": start.isoformat(), "end": end.isoformat(),
                        "reason": "later output publication lacks affected-coordinate proof"}]
                continue
            lower = max(start, _date_value(batch["min_observation"]))
            upper = min(through, _date_value(batch["max_observation"]))
            if lower <= upper:
                windows.append((lower, upper))
        for day, available in pending_dates(db, previous["id"]):
            if receipt.information_cutoff < _date_value(available) <= end and start <= _date_value(day) <= through:
                windows.append((_date_value(day), _date_value(day)))
        if (not windows and end <= _date_value(previous["end_date"])
                and not (policy is not None and policy.general_fields)
                and owner._lake.inputs.is_current(receipt)):
            return []
        if not windows:
            from .timing import proof
            timing = proof(owner, db, previous, receipt, policy=policy, start=start, end=end,
                commit_bound=commit_bound, check_bound=check_bound)
            if timing is not None:
                return [{"mode": "attest", "start": start.isoformat(), "end": through.isoformat(),
                    "available_date": timing["available_date"]},
                    *([{"mode": "incremental", "start": (through + timedelta(days=1)).isoformat(),
                        "end": end.isoformat()}] if end > through else [])]
        calendar: list[date] | None = None
        for alias, request in receipt.requests.items():
            evidence = receipt.evidence[alias]
            captured_future_timing = []
            general_projection_equal = False
            declaration = db.execute("select spec_hash from "
                + ("item_definitions where name=?" if evidence["source"] == "items"
                   else "datasets where source=? and name=?"),
                (evidence["dataset"],) if evidence["source"] == "items"
                else (evidence["source"], evidence["dataset"])).fetchone()
            if declaration is None or declaration[0] != evidence["definition_hash"]:
                unknown = f"{alias}: dependency definition changed; re-resolve the numerical definition"
                break
            fields = None if policy is None else dict(policy.general_fields).get(alias)
            general = bool(evidence["general_snapshots"]) or fields is not None
            new_general = general and not anchor_empty and db.execute("select 1 from version_commits "
                "where source=? and dataset=? and status='committed' and seq>? and seq<=? "
                "and (pit_date<=? or (baseline=1 and ?)) limit 1",
                (evidence["source"], evidence["dataset"], receipt.max_commit, commit_bound, end.isoformat(),
                 isinstance(request, RawInput) and request.include_historical_baseline)).fetchone()
            if general:
                from .pit import _general_snapshot_identity
                def selected_general(cutoff):
                    return _general_snapshot_identity(evidence["general_snapshots"], cutoff,
                        checks=evidence["checks"], strict=request.strict,
                        include_historical_baseline=isinstance(request, RawInput) and request.include_historical_baseline)
                original_general, later_general = selected_general(receipt.information_cutoff), selected_general(end)
                later_changed = not anchor_empty and ((original_general is None and later_general is not None)
                        or original_general is not None and (later_general is None
                            or later_general["seq"] != original_general["seq"]))
                changed = bool(new_general or later_changed)
                if fields is not None:
                    assert policy is not None
                    unknown = _general_fields_problem(owner, db, receipt, alias=alias, fields=fields,
                        end=end, commit_bound=commit_bound, policy=policy, changed=changed,
                        completed_start=start, completed_end=through)
                    general_projection_equal = changed and unknown is None
                elif changed:
                    unknown = f"{alias}: {'changed' if new_general else 'newly visible'} full snapshot needs a selected-row impact proof"
                if changed or fields is not None:
                    if unknown:
                        break
                captured_future_timing = [check for check in evidence["checks"] if not check["baseline"]
                    and receipt.information_cutoff < _date_value(check["pit_date"]) <= end]
            batches = db.execute("select " + batch_columns + ",c.source,c.dataset,c.pit_date from version_segments b "
                "join version_commits c on c.seq=b.commit_seq "
                "where c.source=? and c.dataset=? and c.status='committed' and b.commit_seq<=? "
                "and (b.commit_seq>? or b.max_available is null or b.max_available>?) "
                "order by b.commit_seq,b.partition_path", (evidence["source"], evidence["dataset"], commit_bound,
                    receipt.max_commit, str(receipt.information_cutoff))).fetchall()
            # General is an atomic selected event, not a daily availability
            # batch. Its rows may have no `time` at all. New content/visibility
            # and future attestations are handled by their event proofs above.
            for batch in (() if evidence["general_snapshots"] else batches):
                if batch["min_available"] is not None and _date_value(batch["min_available"]) > end:
                    continue
                lower, upper = batch["min_observation"], batch["max_observation"]
                if batch["commit_seq"] <= receipt.max_commit:
                    # Compact observation/availability bounds conservatively repair
                    # this interval when an original segment becomes visible.
                    # No per-row proof index or historical input decode is needed.
                    if batch["max_available"] is not None and _date_value(batch["max_available"]) <= receipt.information_cutoff:
                        continue
                    summary = input_index.describe_bounds(db,batch)
                    delay = summary.get("max_availability_delay_days") if summary is not None else None
                    if delay is not None and lower is not None:
                        lower = max(_date_value(lower),receipt.information_cutoff+timedelta(days=1-int(delay)))
                if lower is None or upper is None:
                    if not batch["row_count"]:
                        continue
                    unknown = f"{alias}: missing observation bounds require explicit impact proof"
                    break
                first, last = _date_value(cast(DateLike, lower)), _date_value(cast(DateLike, upper))
                first = max([first, *(_date_value(value) for value in
                    (request.start, getattr(request, "observation_start", None)) if value is not None)])
                last = min(last, end, *(_date_value(value) for value in
                    (request.end, getattr(request, "observation_end", None)) if value is not None))
                if first <= last and first <= through:
                    if policy is not None and policy.historical_impact == "forward":
                        affected_end = end
                        if policy.history_observations is not None and calendar is None:
                            assert policy.calendar_input is not None
                            coordinates = input_index.read_coordinates(db, receipt.digest, policy.calendar_input,
                                policy.calendar_field, policy.calendar_selector)
                            if coordinates is None:
                                unknown = "finite-history calendar proof is missing; run explicit inputs.index_coordinates maintenance"
                                break
                            calendar = [_date_value(value) for value in coordinates]
                        if calendar is not None:
                            count = cast(int, policy.history_observations)
                            # Complete admitted coordinates include empty/null
                            # output dates. Output row counts lose those dates.
                            position = bisect_right(calendar, last) - 1 + count
                            if position < len(calendar):
                                affected_end = min(end, calendar[max(0, position)])
                        if max(first, start) <= affected_end:
                            windows.append((max(first, start), affected_end))
                    elif last >= start:
                        windows.append((max(first, start), min(last, through)))
            if unknown:
                break
            # Nonbaseline same-content checks cannot change causal visibility.
            # Baseline attestations can; a timing-impact proof is required rather
            # than treating an observation bound or row count as a safe delta.
            timing = db.execute("select v.* from version_checks v "
                "left join version_commits c on c.seq=v.visible_commit "
                "where v.source=? and v.dataset=? and v.id>? and v.id<=? and v.baseline=0 "
                "and (v.visible_commit is null or v.visible_commit<=?) "
                "and (coalesce(c.baseline,1)=1 or exists(select 1 from visibility_segments e "
                "where e.check_id=v.id)) and v.pit_date<=?",
                (evidence["source"], evidence["dataset"], receipt.max_check_id, check_bound,
                    commit_bound, end.isoformat())).fetchall()
            timing = [] if anchor_empty and evidence["general_snapshots"] else [*timing, *captured_future_timing]
            if timing and not (general_projection_equal or policy is not None and alias in policy.timing_neutral_inputs
                    and _neutral_timing_preserved(owner, db, receipt, name=previous["name"], alias=alias,
                        checks=timing, end=end, commit_bound=commit_bound)):
                events = owner._store.visibility_headers(db,evidence["source"],evidence["dataset"],check_bound=check_bound)
                identifiers = {check["id"] for check in timing}
                relevant = [event for event in events if event["check_id"] in identifiers]
                if not relevant or any(event["min_observation"] is None or event["max_observation"] is None for event in relevant):
                    unknown = f"{alias}: new timing attestations require explicit selected-event impact; observation impact is unknown"
                    break
                for event in relevant:
                    # First knowledge changes computations from its actual
                    # availability boundary, never the earlier observation date.
                    first = max(start,_date_value(event["pit_date"]))
                    last = min(through,max(_date_value(event["max_observation"]),first))
                    if first <= last:
                        windows.append((first,end if policy is not None and policy.historical_impact == "forward" else last))
    if unknown is None and policy is None:
        unknown = "producer lacks a causal incremental update contract"
    if unknown:
        return [{"mode": "blocked", "start": start.isoformat(), "end": end.isoformat(), "reason": unknown}]
    assert policy is not None
    if policy.historical_impact == "forward" and policy.history_observations is None and windows:
        windows = [(min(first for first, _ in windows), end)]
    if end > through:
        windows.append((through + timedelta(days=1), end))
    merged = _merge(windows)
    if merged and merged[0][0] <= start and merged[0][1] >= _date_value(previous["end_date"]):
        return [{"mode": "blocked", "start": start.isoformat(), "end": end.isoformat(),
                 "reason": "proven input impact covers the completed history; explicit resolution is required"}]
    return [{"mode": "incremental", "start": first.isoformat(), "end": last.isoformat()}
            for first, last in merged]
