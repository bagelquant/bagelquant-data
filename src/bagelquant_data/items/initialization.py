"""One frozen initialization ledger and bounded observation units per Item."""
from __future__ import annotations

from collections import deque
from collections.abc import Callable, Sequence
from contextlib import ExitStack
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
import json
from time import perf_counter
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from bagelquant_data.execution import ExecutionOptions
from bagelquant_data.query.raw import _date_value
from bagelquant_data.transforms import Cast, Filter, MapValues, Select

from .completion import output_check_bound, pending_dates, save_completion
from .preparation import prepared_window
from .types import DataItemSpec, ItemBuildReport, ItemInput, Producer, ProducerUpdatePolicy

if TYPE_CHECKING:
    from bagelquant_data.inputs import FrozenInputReceipt
    from .api import ItemAPI


@dataclass(slots=True)
class _Initialization:
    spec: DataItemSpec
    definition_hash: str
    policy: ProducerUpdatePolicy | None
    producer: Producer | None
    receipt: FrozenInputReceipt
    build_id: int
    writer_id: str
    bounded: bool
    rows: int = 0
    result_commit: int | None = None
    work: dict[str, int | float] = field(default_factory=lambda: {
        "windows_built": 0, "windows_reused": 0, "build_seconds": 0.0})


def _window_policy(api: ItemAPI, spec: DataItemSpec) -> ProducerUpdatePolicy | None:
    policy = api._policy(spec)
    if (policy is None and spec.producer_key is None
            and all(isinstance(value, (Select, Cast, Filter, MapValues)) for value in spec.transforms)):
        return ProducerUpdatePolicy("same_date")
    return policy


def _small_range(receipt: FrozenInputReceipt, byte_limit: int) -> bool:
    """Try one tiny range using original counts, retaining ordinary byte checks.

    This is admission, not proof of an allocation maximum. The broad reserve
    leaves room for keys, revisions and publication; bounded reads and output
    admission still split an unexpectedly large range before certification.
    """
    reserve = 0
    for evidence in receipt.evidence.values():
        for batch in evidence["batches"]:
            reserve += max(int(batch["row_count"]) * 4096, int(batch.get("file_size_bytes", 0)))
            if reserve > byte_limit // 16:
                return False
        for check in evidence["record_checks"]:
            if check.get("event"):
                reserve += int(check["row_count"]) * 4096
                if reserve > byte_limit // 16:
                    return False
    return True


def _reusable_unit(db, state: _Initialization, unit: dict[str, Any]) -> bool:
    """Authenticate retained output authority without reading historical rows."""
    cutoff = state.receipt.information_cutoff
    changed = db.execute("select 1 from version_segments b join version_commits c on c.seq=b.commit_seq "
        "where c.source='items' and c.dataset=? and c.status='committed' and c.seq>? "
        "and (b.min_available is null or b.min_available<=?) and (b.min_observation is null "
        "or b.max_observation is null or (b.min_observation<=? and b.max_observation>=?)) limit 1",
        (state.spec.name, unit["result_commit"] or 0, str(cutoff), unit["end_date"], unit["start_date"])).fetchone()
    if changed is not None:
        return False
    bound = output_check_bound(db, unit["id"])
    if bound is None:
        raise RuntimeError("Initialization unit lacks its original output-check boundary; explicit completion maintenance is required")
    visibility = db.execute("select 1 from visibility_segments e join version_checks c on c.id=e.check_id "
        "where c.source='items' and c.dataset=? and c.id>? and c.pit_date<=? "
        "and (e.min_observation is null or e.max_observation is null "
        "or (e.min_observation<=? and e.max_observation>=?)) limit 1",
        (state.spec.name, bound, str(cutoff), unit["end_date"], unit["start_date"])).fetchone()
    if visibility is not None:
        return False
    checks = db.execute("select request_json from version_checks where source='items' and dataset=? "
        "and id>? and baseline=0 and row_count=0 and pit_date<=?", (state.spec.name, bound, str(cutoff)))
    for check in checks:
        for request in json.loads(check[0]):
            interval = request.get("item_range")
            if interval and interval["start"] <= unit["end_date"] and interval["end"] >= unit["start_date"]:
                return False
    return True


def initialize_many(api: ItemAPI, names: Sequence[str], *, start, end,
                    config: ExecutionOptions | None,
                    progress: Callable[[ItemBuildReport], None] | None,
                    cancelled: Callable[[], bool] | None) -> dict[str, ItemBuildReport]:
    """Interleave independent outputs; reuse original/key/visibility work per window."""
    from .api import _definition_hash, _json
    api._store.ensure_writable()
    if isinstance(names, str) or not names or len(set(names)) != len(names):
        raise ValueError("names must be a nonempty sequence of unique DataItem names")
    first, last = _date_value(start), _date_value(end)
    if last < first:
        raise ValueError("end precedes start")
    options = config or ExecutionOptions()
    specs = {name: api.get(name) for name in sorted(names)}
    if not api._independent_group(tuple(specs)):
        raise ValueError("Initialize independent ready Items together; complete their declared Item dependencies first")
    result: dict[str, ItemBuildReport] = {}
    pending: dict[str, DataItemSpec] = {}
    # The unchanged path reads only declaration/build metadata. No executor,
    # preparation context, frozen Source or native permit is created.
    for name, spec in specs.items():
        api._validate_output_anchors(spec, api._policy(spec))
        definition_hash = _definition_hash(spec)
        rows = api._store._rows("select * from item_initializations where name=?", (name,))
        original = rows[0] if rows else None
        if original is not None and (original["definition_hash"] != definition_hash
                or original["start_date"] != str(first) or original["end_date"] != str(last)):
            raise ValueError("Initialization range and definition are fixed; use update after completion")
        if original is not None and original["status"] == "complete":
            builds = api._completed_rows("name=? and definition_hash=? and start_date=? and end_date=? "
                "and status='success' order by id desc limit 1", (name, definition_hash, str(first), str(last)))
            if not builds:
                raise RuntimeError("Completed initialization has no committed build evidence")
            build = builds[0]
            result[name] = ItemBuildReport(name, "unchanged", 0, build["result_commit"], build["input_commit"],
                first, last, build["dependency_digest"], build["frozen_receipt_id"], {"windows_built": 0})
        else:
            pending[name] = spec
    if not pending:
        return result
    if cancelled and cancelled():
        with api._store.connect() as db:
            db.execute("begin immediate")
            for name, spec in pending.items():
                if db.execute("select 1 from item_initializations where name=?", (name,)).fetchone() is None:
                    if db.execute("select 1 from version_commits where source='items' and dataset=? and status='committed' limit 1", (name,)).fetchone():
                        raise ValueError("Historical initialization requires a new DataItem")
                    db.execute("insert into item_initializations values(?,?,?,?,'running')",
                        (name, _definition_hash(spec), str(first), str(last)))
        result.update({name: ItemBuildReport(name, "cancelled", 0, None, 0, first, last, "") for name in pending})
        return result
    for spec in pending.values():
        if not spec.inputs:
            raise ValueError("An initialized DataItem must declare inputs")
        for dependency in sorted((value for value in spec.inputs if isinstance(value, ItemInput)), key=lambda value: value.name):
            if api.get(dependency.name).inputs:
                report = api.update(dependency.name, start=first, end=last, config=options,
                    progress=progress, cancelled=cancelled)
                if report.status == "cancelled":
                    result.update({name: ItemBuildReport(name, "cancelled", 0, None, 0, first, last, "") for name in pending})
                    return result
    active: dict[str, _Initialization] = {}
    with ExitStack() as leases:
        writer_ids = {name: uuid4().hex for name in pending}
        for name in pending:
            leases.enter_context(api._store.dataset_writer("items", name, writer_ids[name]))
        # Capture fresh Sources together under one commit/check snapshot.
        with api._store.atomic_transaction() as db:
            for name, spec in pending.items():
                definition_hash = _definition_hash(spec)
                if _definition_hash(api.get(name)) != definition_hash:
                    raise RuntimeError("DataItem definition changed before initialization admission")
                policy = _window_policy(api, spec)
                binding = _json(None if policy is None else asdict(policy))
                original = db.execute("select * from item_initializations where name=?", (name,)).fetchone()
                saved = db.execute("select * from item_builds where name=? and definition_hash=? and start_date=? "
                    "and end_date=? and status='initializing' order by id desc limit 1",
                    (name, definition_hash, str(first), str(last))).fetchone()
                if original is None:
                    if db.execute("select 1 from version_commits where source='items' and dataset=? and status='committed' limit 1", (name,)).fetchone():
                        raise ValueError("Historical initialization requires a new DataItem")
                    db.execute("insert into item_initializations values(?,?,?,?,'running')", (name, definition_hash, str(first), str(last)))
                if saved is not None:
                    stored = db.execute("select value from data_meta_state where key=?", (f"item_initialization_policy:{saved['id']}",)).fetchone()
                    if stored is None or stored[0] != binding:
                        raise RuntimeError("Initialization producer policy changed; resume with its original declaration")
                    receipt = api._lake.inputs.get(saved["frozen_receipt_id"])
                    build_id = saved["id"]
                else:
                    boundary = api._lake.inputs._boundary
                    cutoff = min(last, boundary.as_of) if boundary is not None and boundary.as_of is not None else last
                    payload = api._lake.inputs._capture(db, api._input_requests(spec, None), cutoff=cutoff,
                        max_commit=None if boundary is None else boundary.max_commit,
                        persist_checks=True, retained_bounds_aliases={value.key for value in spec.inputs})
                    receipt = api._lake.inputs._register_capture(db, payload)
                    cursor = db.execute("insert into item_builds(name,definition_hash,dependency_digest,start_date,end_date,"
                        "input_commit,result_commit,status,created_at,frozen_receipt_id) values(?,?,?,?,?,?,NULL,'initializing',?,?)",
                        (name, definition_hash, receipt.dependency_digest, str(first), str(last), receipt.max_commit,
                         datetime.now(UTC).isoformat(), receipt.receipt_id))
                    build_id = cursor.lastrowid
                    assert build_id is not None
                    db.execute("insert into data_meta_state(key,value,updated_at) values(?,?,?)",
                        (f"item_initialization_policy:{build_id}", binding, datetime.now(UTC).isoformat()))
                producer = api._producers.get((spec.producer_key, str(spec.producer_revision))) if spec.producer_key else None
                if spec.producer_key and producer is None:
                    raise ValueError(f"Producer is not registered: {spec.producer_key}@{spec.producer_revision}")
                bounded = (policy is not None and policy.historical_impact == "same_date"
                           and policy.history_observations in (None, 0) and options.observation_window_days is not None)
                active[name] = _Initialization(spec, definition_hash, policy, producer, receipt,
                    build_id, writer_ids[name], bounded)
                commits = db.execute("select max(result_commit) from item_builds where name=? and frozen_receipt_id=? and status='unit_success'",
                    (name, receipt.receipt_id)).fetchone()
                active[name].result_commit = commits[0]
                db.execute("update datasets set update_incomplete=1 where source='items' and name=?", (name,))
        units: deque[tuple[date, date, tuple[str, ...]]] = deque()
        # The default month size is a conservative fallback for large inputs,
        # not a reason to repeat metadata/publication for a tiny admitted panel.
        small_names = tuple(name for name, state in active.items() if state.bounded
            and options.observation_window_days == 31 and _small_range(state.receipt, options.max_buffer_bytes))
        bounded_names = tuple(name for name, state in active.items() if state.bounded and name not in small_names)
        if bounded_names:
            window_days = options.observation_window_days
            assert window_days is not None
            day = first
            while day <= last:
                next_month = date(day.year + (day.month == 12), day.month % 12 + 1, 1)
                stop = min(last, next_month - timedelta(days=1), day + timedelta(days=window_days - 1))
                units.append((day, stop, bounded_names))
                day = stop + timedelta(days=1)
        if small_names:
            units.append((first, last, small_names))
        units.extend((first, last, (name,)) for name, state in active.items() if not state.bounded)
        totals: dict[str, Any] = {"prepared_hits": 0, "prepared_misses": 0, "source_rows_read": 0,
            "key_rows_prepared": 0, "prepared_peak_bytes": 0}
        stopped = False
        while units and not stopped:
            unit_first, unit_last, unit_names = units.popleft()
            with prepared_window(api._store.data_meta_path, options.max_buffer_bytes // 4) as prepared:
                for name in unit_names:
                    state = active[name]
                    if cancelled and cancelled():
                        stopped = True
                        break
                    completed = api._completed_rows("name=? and definition_hash=? and frozen_receipt_id=? "
                        "and status='unit_success' and start_date<=? and end_date>=? order by start_date,end_date",
                        (name, state.definition_hash, state.receipt.receipt_id, str(unit_last), str(unit_first)))
                    with api._store.connect() as db:
                        completed = [unit for unit in completed if _reusable_unit(db, state, unit)]
                    gaps = []
                    cursor = unit_first
                    for old in completed:
                        old_first, old_last = _date_value(old["start_date"]), _date_value(old["end_date"])
                        if old_first > cursor:
                            gaps.append((cursor, old_first - timedelta(days=1)))
                        cursor = max(cursor, old_last + timedelta(days=1))
                    if cursor <= unit_last:
                        gaps.append((cursor, unit_last))
                    if gaps != [(unit_first, unit_last)]:
                        state.work["windows_reused"] += 1
                        for gap_first, gap_last in reversed(gaps):
                            units.appendleft((gap_first, gap_last, (name,)))
                        continue
                    before = perf_counter()
                    try:
                        report = api._build(name, start=unit_first, end=unit_last, initialize=True,
                            config=replace(options, max_buffer_bytes=max(1, options.max_buffer_bytes * 3 // 4)),
                            stack=(), progress=progress, cancelled=cancelled, frozen_receipt=state.receipt,
                            frozen_spec=state.spec, pinned_producer=state.producer, pinned_policy=state.policy,
                            publish_completion=False, writer_run_id=state.writer_id)
                    except MemoryError:
                        if not state.bounded or unit_first == unit_last:
                            raise
                        middle = unit_first + (unit_last - unit_first) // 2
                        units.appendleft((middle + timedelta(days=1), unit_last, (name,)))
                        units.appendleft((unit_first, middle, (name,)))
                        state.work["windows_split"] = state.work.get("windows_split", 0) + 1
                        continue
                    state.work["build_seconds"] += perf_counter() - before
                    for metric, value in report.work.items():
                        state.work[metric] = state.work.get(metric, 0) + value
                    state.work["windows_reused" if report.status == "unchanged" else "windows_built"] += 1
                    state.rows += report.rows_committed
                    state.result_commit = report.commit_seq or state.result_commit
                    if report.status == "cancelled":
                        stopped = True
                        break
                totals["prepared_hits"] += prepared.hits
                totals["prepared_misses"] += prepared.misses
                totals["source_rows_read"] += prepared.source_rows
                totals["key_rows_prepared"] += prepared.key_rows
                totals["prepared_peak_bytes"] = max(totals["prepared_peak_bytes"], prepared.peak_bytes)
        for name, state in active.items():
            if stopped:
                result[name] = ItemBuildReport(name, "cancelled", state.rows, state.result_commit,
                    state.receipt.max_commit, first, last, state.receipt.dependency_digest, state.receipt.receipt_id,
                    {**state.work, **totals})
                continue
            with api._store.atomic_transaction() as db:
                if _definition_hash(api.get(name)) != state.definition_hash or _window_policy(api, api.get(name)) != state.policy:
                    raise RuntimeError("DataItem definition or producer policy changed before initialization completion")
                if cancelled and cancelled():
                    result[name] = ItemBuildReport(name, "cancelled", state.rows, state.result_commit,
                        state.receipt.max_commit, first, last, state.receipt.dependency_digest, state.receipt.receipt_id,
                        {**state.work, **totals})
                    stopped = True
                    continue
                completed = api._completed_rows("name=? and definition_hash=? and frozen_receipt_id=? and status='unit_success' "
                    "and start_date>=? and end_date<=? order by start_date,end_date,id",
                    (name, state.definition_hash, state.receipt.receipt_id, str(first), str(last)))
                completed = [unit for unit in completed if _reusable_unit(db, state, unit)]
                boundary = first
                pending_coordinates = []
                for unit in completed:
                    if _date_value(unit["start_date"]) > boundary:
                        break
                    boundary = max(boundary, _date_value(unit["end_date"]) + timedelta(days=1))
                    pending_coordinates.extend(pending_dates(db, unit["id"]))
                if boundary <= last:
                    raise RuntimeError("Initialization units do not cover the frozen whole range")
                db.execute("update item_builds set status='initialized',result_commit=? where id=? and status='initializing'",
                    (state.result_commit, state.build_id))
                completed_build = db.execute("insert into item_builds(name,definition_hash,dependency_digest,start_date,end_date,"
                    "input_commit,result_commit,status,created_at,frozen_receipt_id) values(?,?,?,?,?,?,?,'success',?,?)",
                    (name, state.definition_hash, state.receipt.dependency_digest, str(first), str(last),
                     state.receipt.max_commit, state.result_commit, datetime.now(UTC).isoformat(), state.receipt.receipt_id))
                save_completion(db, completed_build.lastrowid, pending=pending_coordinates,
                    publish_node=api._lake.inputs.is_current(state.receipt))
                db.execute("update item_initializations set status='complete' where name=?", (name,))
                if cancelled and cancelled():
                    db.rollback()
                    result[name] = ItemBuildReport(name, "cancelled", state.rows, state.result_commit,
                        state.receipt.max_commit, first, last, state.receipt.dependency_digest, state.receipt.receipt_id,
                        {**state.work, **totals})
                    stopped = True
                    continue
            result[name] = ItemBuildReport(name, "success", state.rows, state.result_commit,
                state.receipt.max_commit, first, last, state.receipt.dependency_digest, state.receipt.receipt_id,
                {**state.work, **totals})
    return result
