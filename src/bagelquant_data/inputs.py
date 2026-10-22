"""Frozen neutral input receipts backed by central immutable ingestion batches."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, nullcontext
from contextvars import ContextVar
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from threading import get_ident, RLock
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

import polars as pl
from bagelquant_data.core.hashing import frame_content_hash
import pyarrow as pa

from bagelquant_data.core.schema import align_frame, compatible_schema, concat_compatible_frames
from bagelquant_data.core.types import DateLike
from bagelquant_data.items.types import DataInput, ItemInput, RawInput, input_from_payload, input_payload
from bagelquant_data.query.raw import _date_value
from bagelquant_data.storage.recovery import read_batch, verify_batch
from bagelquant_data.execution import ExecutionOptions
from bagelquant_data.storage import immutable_metadata

if TYPE_CHECKING:
    from bagelquant_data.management.lake import DataLake


@dataclass(frozen=True, slots=True)
class InputSelectionScope:
    alias: str
    start: DateLike | None = None
    end: DateLike | None = None
    as_of: DateLike | None = None
    view: str | None = None
    strict: bool | None = None
    causal_versions: bool = False
    coordinates: pl.DataFrame | None = None
    coordinate_selector: str | None = None
    fields: tuple[str, ...] | None = None


_COORDINATE_PROJECTION = "general_coordinates.v2"


@dataclass(frozen=True, slots=True)
class InputReadBoundary:
    data_meta_path: Path
    max_commit: int
    as_of: date | None = None
    max_check_id: int | None = None


_READ_BOUNDARIES: ContextVar[tuple[InputReadBoundary, ...]] = ContextVar("data_input_read_boundaries", default=())


@contextmanager
def input_read_boundary(data_meta_path: str | Path, max_commit: int, as_of: DateLike | None = None,
                        *, max_check_id: int | None = None) -> Iterator[None]:
    """Bound new readers using committed runtime metadata without copying it.

    Readers capture the value on construction so callers can explicitly pass
    them to worker threads. Nested contexts may only tighten an existing bound.
    """
    if max_commit < 0:
        raise ValueError("max_commit must be non-negative")
    resolved_data_meta_path = Path(data_meta_path).resolve()
    explicit_check_boundary = max_check_id is not None
    if max_check_id is None:
        from bagelquant_data.storage.data_meta import DataMetaStore
        store = DataMetaStore(data_meta_path=resolved_data_meta_path, read_only=True, runtime=True)
        max_check_id = int(store._rows("select coalesce(max(id),0) as id from version_checks")[0]["id"])
    if max_check_id < 0:
        raise ValueError("max_check_id must be non-negative")
    previous = _boundary_record(resolved_data_meta_path)
    cutoff = None if as_of is None else _date_value(as_of)
    if previous:
        if max_commit > previous.max_commit:
            raise ValueError("nested input boundary cannot widen max_commit")
        if previous.as_of is not None and cutoff is not None and cutoff > previous.as_of:
            raise ValueError("nested input boundary cannot widen the information cutoff")
        max_commit = min(max_commit, previous.max_commit)
        if previous.max_check_id is not None:
            if explicit_check_boundary and max_check_id > previous.max_check_id:
                raise ValueError("nested input boundary cannot widen max_check_id")
            max_check_id = min(max_check_id, previous.max_check_id)
        if previous.as_of is not None:
            cutoff = previous.as_of if cutoff is None else min(cutoff, previous.as_of)
    token = _READ_BOUNDARIES.set((*_READ_BOUNDARIES.get(), InputReadBoundary(resolved_data_meta_path, max_commit, cutoff, max_check_id)))
    try:
        yield
    finally:
        _READ_BOUNDARIES.reset(token)


def _boundary_record(data_meta_path: str | Path) -> InputReadBoundary | None:
    resolved_data_meta_path = Path(data_meta_path).resolve()
    return next((value for value in reversed(_READ_BOUNDARIES.get()) if value.data_meta_path == resolved_data_meta_path), None)


def current_input_boundary(data_meta_path: str | Path) -> tuple[int | None, date | None]:
    """Return the applicable captured commit and information bounds."""
    value = _boundary_record(data_meta_path)
    return (value.max_commit, value.as_of) if value else (None, None)


def current_input_check_boundary(data_meta_path: str | Path) -> int | None:
    """Return the captured immutable same-value check boundary."""
    value = _boundary_record(data_meta_path)
    return value.max_check_id if value else None


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      default=lambda value: dict(value) if isinstance(value, Mapping) else
                      list(value) if isinstance(value, Sequence) and not isinstance(value, (str, bytes)) else str(value))


@dataclass(frozen=True, slots=True)
class FrozenInputReceipt:
    receipt_id: str
    max_commit: int
    max_check_id: int
    information_cutoff: date | None
    requests: Mapping[str, DataInput]
    evidence: Mapping[str, Mapping[str, Any]]
    digest: str
    dependency_digest: str


def _readonly(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _readonly(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_readonly(item) for item in value)
    return value


def _identity_evidence(evidence: Mapping[str, Any]) -> dict[str, Any]:
    """Read bounds and descriptive prose do not change numerical identity."""
    result = dict(evidence)
    result["definition"] = {key: value for key, value in evidence["definition"].items() if key != "description"}
    if result.pop("retained_batch_bounds", False):
        bounds = {"min_available", "max_available", "min_observation", "max_observation"}
        result["batches"] = [{key: value for key, value in batch.items() if key not in bounds}
                             for batch in evidence["batches"]]
    return result


def _general_fields(frozen, alias, fields):
    return fields is not None and isinstance(frozen.requests[alias], RawInput) and (
        frozen.evidence[alias]["definition"]["update_type"] == "general")


def _selected_general(frozen, alias, request, cutoff):
    from bagelquant_data.items.pit import _general_snapshot_identity
    evidence = frozen.evidence[alias]
    return _general_snapshot_identity(evidence["general_snapshots"], cutoff,
        checks=evidence["checks"], strict=request.strict,
        include_historical_baseline=request.include_historical_baseline)


def _selection_batches(frozen, alias, request, cutoff, *, fields=None):
    """Prune only by the original captured observation/availability bounds."""
    evidence = frozen.evidence[alias]
    if _general_fields(frozen, alias, fields):
        selected = _selected_general(frozen, alias, request, cutoff)
        if selected is None:
            raise ValueError("General consumed fields require a selected atomic snapshot; no eligible event has original schema authority")
        return [batch for batch in evidence["batches"]
            if batch["commit_seq"] == selected["seq"]]
    lower = None if request.start is None else _date_value(request.start)
    upper = None if request.end is None else _date_value(request.end)
    if isinstance(request, RawInput):
        lower = max((_date_value(item) for item in (lower, request.observation_start) if item is not None), default=None)
        upper = min((_date_value(item) for item in (upper, request.observation_end) if item is not None), default=None)
    batches = evidence["batches"]
    if isinstance(batches, immutable_metadata.LazyMetadataSequence) and (evidence.get("scoped_batches") or evidence.get("retained_batch_bounds")):
        return list(batches.selected(lower, upper, cutoff))
    return [batch for batch in batches if not (
        (evidence.get("scoped_batches") or evidence.get("retained_batch_bounds")) and (
            lower is not None and batch["max_observation"] is not None and _date_value(batch["max_observation"]) < lower
            or upper is not None and batch["min_observation"] is not None and _date_value(batch["min_observation"]) > upper
            or cutoff is not None and batch["min_available"] is not None and _date_value(batch["min_available"]) > cutoff))]


def _scalar_selection_records(frozen, alias, request, cutoff, *, fields=None):
    evidence = frozen.evidence[alias]
    batches = _selection_batches(frozen, alias, request, cutoff)
    commits = {int(batch["commit_seq"]) for batch in batches}
    checks = [row for row in evidence["checks"] if row["row_count"]]
    check_ids = {int(row["id"]) for row in checks}
    records = [row for row in evidence["record_checks"]
        if row["check_id"] in check_ids and (bool(commits.intersection(row["version_commits"])) if row.get("event") else row["version_commit"] in commits)]
    seals = [row for row in evidence.get("full_commit_checks", ())
        if row["binding"]["check"]["id"] in check_ids]
    if fields is not None:
        # Only these witnesses can create a selected copy of a retained row.
        # Appended/outside-window checks must not expire unchanged field proofs.
        # Unknown batch bounds still retain every potentially selected commit.
        eligible = {int(row["id"]) for row in checks if not row["baseline"]
            and (cutoff is None or _date_value(row["pit_date"]) <= cutoff)}
        records = [row for row in records if row["check_id"] in eligible
            and (cutoff is None or _date_value(row.get("pit_date", row.get("available_date"))) <= cutoff)]
        seals = [row for row in seals if row["binding"]["check"]["id"] in eligible
            and row["binding"]["check"]["visible_commit"] in commits
            and (cutoff is None or _date_value(row.get("pit_date", row.get("available_date"))) <= cutoff)]
        relevant = {row["check_id"] for row in records} | {row["binding"]["check"]["id"] for row in seals}
        checks = [row for row in checks if row["id"] in relevant]
    return batches, checks, records, seals


def _range_events(evidence, batches, request, cutoff):
    """Retain witnesses that can change precedence inside this observation range."""
    lower = max((_date_value(value) for value in (request.start, getattr(request, "observation_start", None)) if value is not None), default=None)
    upper = min((_date_value(value) for value in (request.end, getattr(request, "observation_end", None)) if value is not None), default=None)
    commits = {batch["commit_seq"] for batch in batches}
    return [dict(row) for row in evidence["record_checks"] if row.get("event")
        and commits.intersection(row["version_commits"])
        and (cutoff is None or _date_value(row["pit_date"]) <= cutoff)
        and not (lower is not None and row.get("max_observation") is not None and _date_value(row["max_observation"]) < lower
            or upper is not None and row.get("min_observation") is not None and _date_value(row["min_observation"]) > upper)]


def _scalar_selection_evidence(frozen, alias, request, cutoff, *, fields=None):
    """Bind scalar selection mechanics, separately from enclosing provenance.

    Creator ordering and all selected batch identities/witnesses remain bound.
    Known empty completion/parent receipts do not add scalar events. Unknown
    empty baseline history remains explicit authority for causal versions.
    """
    evidence = frozen.evidence[alias]
    if _general_fields(frozen, alias, fields):
        selected = _selected_general(frozen, alias, request, cutoff)
        batches = _selection_batches(frozen, alias, request, cutoff, fields=fields)
        if selected is None:
            raise ValueError("General consumed fields require a selected atomic snapshot; no eligible event has original schema authority")
        body = {"version": "general_consumed_fields_evidence.v1",
            "definition": evidence["definition_hash"], "selected": selected,
            "batches": [{key: value for key, value in batch.items()
                if key not in {"min_available", "max_available", "min_observation", "max_observation"}}
                for batch in batches]}
        if selected["attestation_id"] is not None:
            body["check"] = next(dict(row) for row in evidence["checks"]
                if row["id"] == selected["attestation_id"])
        return hashlib.sha256(_json(body).encode()).hexdigest()
    batches, checks, records, seals = _scalar_selection_records(frozen, alias, request, cutoff, fields=fields)
    body = {"version": "consumed_fields_evidence.v1" if fields is not None else "scalar_selection_evidence.v1",
        "definition": evidence["definition_hash"],
        "schema": evidence["schema_hash"], "batches": [{key: value for key, value in batch.items()
            if key not in {"min_available", "max_available", "min_observation", "max_observation"}} for batch in batches],
        "checks": checks, "record_checks": records, "full_commit_checks": seals,
        "empty_baseline": [row for row in evidence["empty_checks"] if row["baseline"]]}
    return hashlib.sha256(_json(body).encode()).hexdigest()


def _selection_key(request, cutoff, *, values_only=False, causal_versions=False, coordinates=None, fields=None):
    body = {"request": input_payload(request), "cutoff": cutoff}
    if values_only:
        body["projection"] = (("causal_fields_versions.v1" if causal_versions else "fields.v1")
            if fields is not None else "causal_value_versions.v1" if causal_versions else "value.v1")
    if fields is not None:
        body["fields"] = fields
    if coordinates is not None:
        body["coordinates"] = coordinates
    return _json(body)


def _selection_fields(fields):
    if fields is None:
        return None
    if (not isinstance(fields, (tuple, list)) or not fields
            or any(not isinstance(name, str) or not name.strip() for name in fields)
            or len(set(fields)) != len(fields)):
        raise ValueError("fields require a nonempty tuple/list of distinct column names")
    if any(name.startswith("_") or name in {"year", "month", "ingested_at"} for name in fields):
        raise ValueError("Consumed field projections exclude partition and creator provenance fields")
    return tuple(fields)


def _original_projection_fields(request, fields):
    if fields is None:
        return None
    if isinstance(request, ItemInput):
        return tuple(dict.fromkeys("source_time" if name == "time" else "time"
            if name == "version_available_date" else name for name in fields))
    return fields


def _validate_raw_payload_projection(request):
    if isinstance(request, RawInput) and any(field in {"time", "year", "month", "ingested_at",
            "version_available_date"} or field.startswith("_") for field in request.fields):
        raise ValueError("Raw payload projections exclude partition and version provenance fields")


def _scalar_value_request(request):
    """Bind the consumed Raw observation intersection, not its enclosing bounds."""
    if not isinstance(request, RawInput):
        return request
    return replace(request,
        start=max((_date_value(day) for day in (request.start, request.observation_start) if day is not None), default=None),
        end=min((_date_value(day) for day in (request.end, request.observation_end) if day is not None), default=None),
        observation_start=None, observation_end=None)


def _scalar_complete_cutoff(frozen, alias, request, cutoff, *, fields=None):
    """Canonicalize a cutoff only after every captured scalar event is visible.

    The canonical cutoff remains an actual date inside the original source
    contract, so explicit audits can reproduce the same complete event stream.
    Unknown bounds/empty baseline authority retain the exact requested cutoff.
    """
    if cutoff is not None and _general_fields(frozen, alias, fields):
        selected = _selected_general(frozen, alias, request, cutoff)
        if selected is not None and selected["snapshot_date"] <= cutoff:
            return selected["snapshot_date"]
    if cutoff is None or not isinstance(request, ItemInput):
        return cutoff
    evidence = frozen.evidence[alias]
    batches = _selection_batches(frozen, alias, request, cutoff)
    if not batches or any(row["baseline"] for row in evidence["empty_checks"]):
        return cutoff
    if any(batch.get("max_available") is None for batch in batches):
        return cutoff
    days = [_date_value(batch["max_available"]) for batch in batches]
    if fields is not None:
        _, checks, records, seals = _scalar_selection_records(frozen, alias, request, cutoff, fields=fields)
    else:
        checks, records, seals = evidence["checks"], evidence["record_checks"], evidence.get("full_commit_checks", ())
    days.extend(_date_value(row["pit_date"]) for row in checks if row["row_count"])
    days.extend(_date_value(row.get("pit_date",row.get("available_date"))) for row in records)
    days.extend(_date_value(row["binding"]["check"]["pit_date"])
        for row in seals)
    days.extend(_date_value(row["available_date"]) for row in seals)
    complete = max(days)
    return complete if complete <= cutoff else cutoff


def _has_visible_item_batch(frozen: FrozenInputReceipt, alias: str) -> bool:
    """Prove a nonempty selection from complete captured batch headers only."""
    request = frozen.requests[alias]
    return any(batch["row_count"]
        and not (request.strict and batch["baseline"])
        and batch.get("min_observation") is not None
        and batch.get("max_observation") is not None
        and (request.start is None or _date_value(batch["min_observation"]) >= _date_value(request.start))
        and (request.end is None or _date_value(batch["max_observation"]) <= _date_value(request.end))
        and (frozen.information_cutoff is None or batch.get("max_available") is not None
            and _date_value(batch["max_available"]) <= frozen.information_cutoff)
        for batch in frozen.evidence[alias]["batches"])


def _content_metadata_selection(frozen: FrozenInputReceipt, alias: str, *, values_only: bool = False) -> str | None:
    """Bind complete by-date content and witnesses eligible at the fixed cutoff.

    Every original content batch remains bound; only future known attestations
    are omitted. Unknown empty baseline evidence remains significant even when
    its acquisition date follows the cutoff. No row-level equivalence is inferred.
    """
    request, evidence = frozen.requests[alias], frozen.evidence[alias]
    item = isinstance(request, ItemInput)
    if item:
        # A newer covering build is not a value revision. Ignore that metadata
        # only when complete captured bounds prove that this versions request
        # contains original rows; empty-input baseline authority stays distinct.
        if request.view != "versions" or not values_only and not _has_visible_item_batch(frozen, alias):
            return None
    elif evidence["definition"]["update_type"] != "by_date":
        return None
    cutoff = frozen.information_cutoff
    checks = [dict(row) for row in evidence["checks"]
        if (not values_only or row["row_count"]) and (cutoff is None or _date_value(row["pit_date"]) <= cutoff)]
    identifiers = {row["id"] for row in checks}
    witnesses = [dict(row) for row in evidence["record_checks"] if row["check_id"] in identifiers
        and (cutoff is None or _date_value(row.get("pit_date", row.get("available_date"))) <= cutoff)]
    from bagelquant_data.storage.full_commit_checks import validate_seal
    for seal in evidence.get("full_commit_checks", ()):
        validate_seal(seal)
    seals = [dict(row) for row in evidence.get("full_commit_checks", ())
        if row["binding"]["check"]["id"] in identifiers]
    bounds = {"min_available", "max_available", "min_observation", "max_observation"}
    batches = [{key: value for key, value in batch.items() if key not in bounds}
        for batch in evidence["batches"]]
    empty = [dict(row) for row in evidence["empty_checks"] if row["baseline"]
        or not values_only and (cutoff is None or _date_value(row["pit_date"]) <= cutoff)]
    return _json({"request": input_payload(request), "definition": evidence["definition_hash"],
        "schema": evidence["schema_hash"], "batches": batches, "checks": checks,
        "record_checks": witnesses, "full_commit_checks": seals, "empty_checks": empty})


def _general_metadata_selection(frozen: FrozenInputReceipt, alias: str) -> str | None:
    """Bind visible atomic General versions and witnesses using original headers."""
    request, evidence = frozen.requests[alias], frozen.evidence[alias]
    if not isinstance(request, RawInput) or not evidence["general_snapshots"]:
        return None
    cutoff = frozen.information_cutoff
    snapshots = evidence["general_snapshots"]
    if request.view == "versions":
        selected = [dict(row) for row in snapshots if not (request.strict and row["baseline"])
            and (cutoff is None or _date_value(row["pit_date"]) <= cutoff
                or row["baseline"] and request.include_historical_baseline)]
        # Known copies of baseline events preserve their full attestation
        # identity; later checks outside the cutoff add no visible versions.
        baseline = {row["seq"]: row for row in snapshots if row["baseline"]}
        checks = [dict(row) for row in evidence["checks"] if not row["baseline"]
            and row["visible_commit"] in baseline
            and (cutoff is None or max(_date_value(row["pit_date"]),
                _date_value(baseline[row["visible_commit"]]["pit_date"])) <= cutoff)]
        sequences = {row["seq"] for row in selected} | {row["visible_commit"] for row in checks}
    else:
        from bagelquant_data.items.pit import _general_snapshot_identity
        event = _general_snapshot_identity(snapshots, cutoff, checks=evidence["checks"],
            strict=request.strict, include_historical_baseline=request.include_historical_baseline)
        selected = [] if event is None else [event]
        sequences = {row["seq"] for row in selected}
        checks = []
    bounds = {"min_available", "max_available", "min_observation", "max_observation"}
    batches = [{key: value for key, value in batch.items() if key not in bounds}
        for batch in evidence["batches"] if batch["commit_seq"] in sequences]
    return _json({"request": input_payload(request), "definition": evidence["definition_hash"],
        "schema": evidence["schema_hash"], "snapshots": selected, "checks": checks,
        "batches": batches, "empty_checks": evidence["empty_checks"]})


@dataclass(slots=True)
class _ReadContext:
    path: Path
    thread: int
    receipts: dict[str, FrozenInputReceipt]
    check_canceled: Callable[[], None] | None
    options: ExecutionOptions
    progress: Callable[[dict[str, Any]], None] | None = None
    verified_batches: set[tuple[int, str, str]] = field(default_factory=set)
    verified_graph: dict[str, tuple[int, tuple[FrozenInputReceipt, ...]]] = field(default_factory=dict)
    currentness: dict[tuple[str, str, Path, InputReadBoundary | None, sqlite3.Connection, int, int], bool] = field(default_factory=dict)
    selections: dict[tuple[str, str, str, bool, bool, bool, bool], tuple[dict, int]] = field(default_factory=dict)
    request_identities: dict[tuple[str, str], str] = field(default_factory=dict)
    capture_batches: dict[tuple[sqlite3.Connection, int, str, str, int], tuple[tuple[sqlite3.Row, ...], int]] = field(default_factory=dict)
    capture_parents: dict[tuple[sqlite3.Connection, int, str], tuple[str, int]] = field(default_factory=dict)
    capture_records: dict[tuple[sqlite3.Connection, int, str, str, tuple[int, ...], tuple[int, ...]], tuple[frozenset[int], int]] = field(default_factory=dict)
    database: sqlite3.Connection | None = None
    entry_changes: int | None = None
    metadata_memo: immutable_metadata.MetadataMemo | None = None
    parent: _ReadContext | None = None
    closed: bool = False
    lock: Any = field(default_factory=RLock)

    @property
    def memo_available(self) -> bool:
        # SQLite total_changes is monotone across savepoint rollback. Once
        # this view writes, neither the pre-write nor post-write facts can be
        # reused safely; rollback does not introduce a new generation.
        return (self.database is not None and self.entry_changes is not None
                and self.database.total_changes == self.entry_changes)


_READ_CONTEXTS: ContextVar[tuple[_ReadContext, ...]] = ContextVar("frozen_input_read_contexts", default=())


class InputsAPI:
    """Freeze contracts and exact registered immutable batch identities."""

    def __init__(self, lake: DataLake) -> None:
        self._lake = lake
        self._store = lake._data_meta
        self._metadata_path = self._store.data_meta_path.resolve()
        self._boundary = _boundary_record(self._store.data_meta_path)
        self._shared_context: _ReadContext | None = None
        if not self._store.read_only:
            with self._store.connect() as db:
                db.execute("create table if not exists frozen_inputs(receipt_id text primary key,payload_json text not null,digest text not null unique,created_at text not null)")

    def _read_context(self) -> _ReadContext | None:
        path = self._metadata_path
        return next((value for value in reversed(_READ_CONTEXTS.get())
                     if value.path == path and value.thread == get_ident()), None)

    def _metadata_memo(self):
        context = self._read_context()
        if context is None or context.closed:
            return None
        if not context.memo_available:
            if context.metadata_memo is not None:
                context.metadata_memo.clear()
                context.metadata_memo.enabled = False
        return context.metadata_memo

    @contextmanager
    def read_context(self, receipt: FrozenInputReceipt | str | Sequence[FrozenInputReceipt | str], *,
                     config: ExecutionOptions | None = None,
                     check_canceled: Callable[[], None] | None = None,
                     progress: Callable[[dict[str, Any]], None] | None = None,
                     verify: bool = False, source: InputsAPI | None = None) -> Iterator[InputsAPI]:
        """Load original metadata once within a finite, read-only operation.

        Matching readers opened on this thread share a SQLite snapshot and
        immutable receipt metadata. Caller receipt objects supply only ID/digest;
        their mutable evidence is never trusted. No frames or validity proofs
        survive exit. ``verify=False`` defers bytes to an explicit verify call.
        """
        single = isinstance(receipt, (FrozenInputReceipt, str))
        supplied = [receipt] if single else list(receipt)
        if not supplied:
            raise ValueError("Read context requires at least one frozen input receipt")
        parent = self._read_context() if source is None else source._shared_context
        if source is not None and (parent is None or parent.closed or parent.path != self._store.data_meta_path.resolve()):
            raise ValueError("Frozen metadata source is not an active matching read context")
        options = config or (parent.options if parent is not None else ExecutionOptions())
        state = _ReadContext(self._metadata_path, get_ident(), {}, check_canceled, options, progress=progress)
        if parent is not None:
            state.receipts, state.lock, state.parent = parent.receipts, parent.lock, parent
        previous_shared, self._shared_context = self._shared_context, state
        token = _READ_CONTEXTS.set((*_READ_CONTEXTS.get(), state))
        try:
            with self._store.read_view() as database:
                state.database = database
                state.entry_changes = (parent.entry_changes if parent is not None
                    and parent.thread == get_ident() and parent.database is database
                    else database.total_changes)
                if (parent is not None and parent.thread == get_ident()
                        and parent.database is database
                        and parent.options.max_buffer_bytes == state.options.max_buffer_bytes):
                    state.selections = parent.selections
                    state.capture_batches = parent.capture_batches
                    state.capture_parents = parent.capture_parents
                    state.capture_records = parent.capture_records
                if (parent is not None and parent.thread == get_ident()
                        and parent.database is database):
                    state.metadata_memo = parent.metadata_memo
                    if state.metadata_memo is not None:
                        state.metadata_memo.shrink(state.options.max_buffer_bytes // 16)
                if state.metadata_memo is None:
                    state.metadata_memo = immutable_metadata.MetadataMemo(state.options.max_buffer_bytes // 16)
                roots = [self.get(value) for value in supplied]
                visiting: set[str] = set()
                loaded: set[str] = set()
                def load(value: FrozenInputReceipt) -> None:
                    if check_canceled is not None:
                        check_canceled()
                    if value.receipt_id in visiting:
                        raise RuntimeError("Frozen input receipt dependency cycle")
                    if value.receipt_id in loaded:
                        return
                    visiting.add(value.receipt_id)
                    for evidence in value.evidence.values():
                        for key, digest in evidence["parent_receipts"].items():
                            parent = self.get(key)
                            if parent.digest != digest:
                                raise RuntimeError("Frozen upstream input receipt checksum mismatch")
                            load(parent)
                    visiting.remove(value.receipt_id)
                    loaded.add(value.receipt_id)
                if verify:
                    for root in roots:
                        load(root)
                    self.verify(roots[0] if single else roots, config=config,
                                check_canceled=check_canceled, progress=progress)
                yield self
                if check_canceled is not None:
                    check_canceled()
        finally:
            _READ_CONTEXTS.reset(token)
            state.closed = True
            self._shared_context = previous_shared
            if parent is None:
                state.receipts.clear()
            state.verified_batches.clear()
            state.verified_graph.clear()
            state.currentness.clear()
            if state.parent is None or state.selections is not state.parent.selections:
                state.selections.clear()
            state.request_identities.clear()
            if state.metadata_memo is not None and (state.parent is None
                    or state.metadata_memo is not state.parent.metadata_memo):
                state.metadata_memo.clear()
            if state.parent is None or state.capture_batches is not state.parent.capture_batches:
                state.capture_batches.clear()
            if state.parent is None or state.capture_parents is not state.parent.capture_parents:
                state.capture_parents.clear()
            if state.parent is None or state.capture_records is not state.parent.capture_records:
                state.capture_records.clear()

    @contextmanager
    def publication_guard(self, receipt: FrozenInputReceipt | str | Sequence[FrozenInputReceipt | str], *,
                          config: ExecutionOptions | None = None,
                          check_canceled: Callable[[], None] | None = None) -> Iterator[InputsAPI]:
        """Fence Data metadata through a consumer's completed-record publication.

        Requires a writable lake and entry outside an existing read context.
        Validate original receipt identities and share one fixed metadata view;
        competing Data writers wait until exit. It writes no Data records,
        audits no bytes and creates no optional proofs. The consumer still owns
        its own revision CAS and cancellation checks before publishing.
        """
        with self._store.publication_read_view():
            with self.read_context(receipt, config=config, check_canceled=check_canceled):
                yield self

    def describe(self, receipt: FrozenInputReceipt | str) -> dict[str, Any]:
        """Describe registered identity without loading the recursive evidence tree."""
        # The small root is immutable evidence too. Verify its registered digest
        # before admitting its alias headers; descendants remain untouched.
        key = self.get(receipt).receipt_id
        rows = self._store._rows("SELECT receipt_id,digest,created_at,"
            "json_extract(payload_json,'$.max_commit') AS max_commit,"
            "json_extract(payload_json,'$.max_check_id') AS max_check_id,"
            "json_extract(payload_json,'$.information_cutoff') AS information_cutoff,"
            "json_extract(payload_json,'$.dependency_digest') AS dependency_digest "
            "FROM frozen_inputs WHERE receipt_id=?", (key,))
        if not rows:
            raise KeyError(f"Unknown frozen input receipt: {key}")
        value = dict(rows[0])
        if isinstance(receipt, FrozenInputReceipt) and receipt.digest != value["digest"]:
            raise RuntimeError("Frozen input receipt checksum mismatch")
        with self._store.connect() as db:
            value["aliases"] = json.loads(db.execute("SELECT json_extract(payload_json,'$.aliases') FROM frozen_inputs WHERE receipt_id=?", (key,)).fetchone()[0])
        return value

    def request_identity(self, receipt: FrozenInputReceipt | str, alias: str) -> str:
        """Identify an alias from original immutable request/evidence metadata.

        Optional indexes and the enclosing receipt's ID, digest and global
        counters never participate. Alias-local evidence and its parent links
        are retained conservatively; selected-window reuse has a separate
        ``selection_identity`` proof. No original batch values are read.
        """
        frozen = self.get(receipt)
        if alias not in frozen.requests:
            raise KeyError(f"Unknown frozen input alias: {alias}")
        if frozen.receipt_id != "current":
            with self._store.connect() as db:
                row = db.execute("SELECT value FROM frozen_inputs,json_each(payload_json,'$.request_identities') WHERE receipt_id=? AND key=?", (frozen.receipt_id, alias)).fetchone()
            if row is not None:
                return row[0]
        context = self._read_context()
        key = (frozen.digest, alias)
        if context is not None and context.memo_available and key in context.request_identities:
            return context.request_identities[key]
        payload = {"version": "frozen_request.v1",
                   "request": input_payload(frozen.requests[alias]),
                   "information_cutoff": frozen.information_cutoff,
                   "evidence": _identity_evidence(frozen.evidence[alias])}
        identity = hashlib.sha256(_json(payload).encode()).hexdigest()
        if context is not None and context.memo_available:
            context.request_identities[key] = identity
        return identity

    def currentness_identity(self, receipt: FrozenInputReceipt | str | None = None, *,
                            config: ExecutionOptions | None = None,
                            check_canceled: Callable[[], None] = lambda: None) -> str:
        """Identify registered Data state for reuse of a previously complete consumer.

        This is a metadata revision identity, not a currentness verdict. The
        consumer must bind it while validating its complete publication inside
        publication_guard, then compare it under a fresh guard before reuse.
        It never validates original numerical bytes or creates proof records.
        Immutable bodies retain their registered identities; audits are separate.
        """
        if self._boundary is not None:
            raise ValueError("Currentness state requires an unbounded current Data view")
        context = self._read_context()
        options = config or (context.options if context is not None else ExecutionOptions())
        digest = hashlib.sha256(b"data.currentness.state.v1\n")
        # Mutable declarations, publication heads, visibility/completion records
        # and original completion bindings. Optional selectors and new unrelated
        # frozen Sources do not change these authorities. No row values or
        # immutable metadata descendants are expanded or audited here.
        tables = ("datasets", "dataset_schemas", "item_definitions", "item_dependencies",
            "node_completions", "version_commits", "version_checks", "item_builds",
            "item_completion_proofs", "item_completion_authorities", "item_pending_outputs",
            "item_completion_attestations")
        with self._store.read_view() as db:
            if receipt is not None:
                self.get(receipt)
            for table in tables:
                check_canceled()
                present = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
                digest.update(_json((table, bool(present))).encode() + b"\n")
                if not present:
                    continue
                for index, row in enumerate(db.execute(f'SELECT * FROM "{table}" ORDER BY rowid')):
                    if index % 128 == 0:
                        check_canceled()
                    size = sum(sys.getsizeof(cell) for cell in row)
                    if size * 8 > options.max_buffer_bytes:
                        raise MemoryError("Currentness metadata row exceeds max_buffer_bytes")
                    digest.update(_json(tuple(row)).encode() + b"\n")
            for index, row in enumerate(db.execute("SELECT b.id,f.receipt_id,f.digest FROM item_builds b "
                    "LEFT JOIN frozen_inputs f ON f.receipt_id=b.frozen_receipt_id ORDER BY b.id")):
                if index % 128 == 0:
                    check_canceled()
                digest.update(_json(tuple(row)).encode() + b"\n")
        check_canceled()
        return digest.hexdigest()

    @staticmethod
    def selection_descriptor_identity(descriptor: Mapping[str, Any]) -> str:
        """Stable immutable selection identity, separate from Source binding.

        Only the enclosing Source and its requested cutoff are omitted. Exact
        scope, explicit as_of, canonical cutoff and original proof remain bound.
        This identity alone claims neither currentness nor Source admission.
        """
        body = {key: value for key, value in descriptor.items() if key not in {"source", "requested_cutoff"}}
        return hashlib.sha256(json.dumps({"schema": "selection.handle.v1", "selection": body}, sort_keys=True, separators=(",", ":"),
            ensure_ascii=False, default=str).encode()).hexdigest()

    def register_selection_set(self, receipt: FrozenInputReceipt | str,
            descriptors: Sequence[Mapping[str, Any]], *, config: ExecutionOptions | None = None,
            check_canceled: Callable[[], None] = lambda: None) -> dict[str, Any]:
        """Register immutable owner-validated selection references, without values.

        Complete descriptor validation happens here once. Ordinary consumers use
        describe_selection_set; this is a Source-bound publication, not a cache
        claiming that a current mutable Source or original bytes were audited.
        """
        self._store.ensure_writable()
        if not descriptors or isinstance(descriptors, (str, bytes)):
            raise ValueError("Selection sets require complete known descriptors")
        frozen = self.get(receipt)
        grouped: dict[str, list[Mapping[str, Any]]] = {}
        for descriptor in descriptors:
            check_canceled()
            grouped.setdefault(str(descriptor["alias"]), []).append(descriptor)
        context = (self.read_context(frozen, config=config, check_canceled=check_canceled)
            if self._read_context() is None
            and getattr(self._store._thread_state, "writer_connection", None) is None else nullcontext())
        with context:
            for values in grouped.values():
                for first in range(0, len(values), 16):
                    group = values[first:first + 16]
                    resolved = self.resolve_selection_descriptors(frozen, group,
                        config=config, check_canceled=check_canceled)
                    if list(resolved) != list(group):
                        raise ValueError("Selection set descriptor differs from its registered Source")
        keys = [self.selection_descriptor_identity(value) for value in descriptors]
        if len(keys) != len(set(keys)):
            raise ValueError("Selection set descriptor identities must be unique")
        source = {"id": frozen.receipt_id, "digest": frozen.digest}
        with self._store.connect() as db:
            db.execute("begin immediate")
            self.get(frozen)
            members, run_ends, previous_scope = [], [], None
            for key, descriptor in zip(keys, descriptors):
                check_canceled()
                # The immutable Source plus explicit as_of reconstructs the
                # requested boundary exactly. Keep canonical/PIT proof fields.
                base = {name: value for name, value in descriptor.items()
                    if name not in {"source", "requested_cutoff"}}
                scope = _json({"alias": base["alias"], "fields": base["fields"], "as_of": base["as_of"],
                    "request": {name: value for name, value in base["request"].items() if name not in {"start", "end"}},
                    "coordinates": base["coordinate_identity"] is not None})
                if previous_scope is not None and scope != previous_scope:
                    run_ends.append(len(members))
                previous_scope = scope
                members.append({"key": key, "reference": immutable_metadata.save(db, base)})
            run_ends.append(len(members))
            root, blocks = immutable_metadata.save_selection_membership(db, members, run_ends=run_ends)
            body = {"schema": "selection.set.v2", "source": source, "count": len(members), "root_ref": root["ref"]}
            digest = hashlib.sha256(_json(body).encode()).hexdigest()
            db.execute("INSERT OR IGNORE INTO input_selection_sets VALUES(?,?,?,?,?,?)",
                (digest, digest, frozen.receipt_id, frozen.digest, root["ref"], len(members)))
            for block in blocks:
                db.executemany("INSERT OR IGNORE INTO input_selection_chunk_entries VALUES(?,?,?,?)",
                    ((block["ref"], member["key"], index, _json(member["reference"]))
                     for index, member in enumerate(members[block["start"]:block["start"] + block["count"]])))
        return {"id": digest, "digest": digest, "source": source, "count": len(members)}

    def describe_selection_set(self, reference: Mapping[str, Any], *,
            receipt: FrozenInputReceipt | str | None = None, config: ExecutionOptions | None = None,
            check_canceled: Callable[[], None] = lambda: None) -> dict[str, Any]:
        """Validate a registered set header and original Source without descendants."""
        check_canceled()
        with self._store.connect() as db:
            row = db.execute("SELECT * FROM input_selection_sets WHERE id=?", (reference["id"],)).fetchone()
        if row is None:
            raise KeyError("Unknown registered selection set")
        source = {"id": row["source_id"], "digest": row["source_digest"]}
        body = {"schema": "selection.set.v2", "source": source, "count": row["item_count"], "root_ref": row["root_ref"]}
        result = {"id": row["id"], "digest": row["digest"], "source": source, "count": row["item_count"]}
        if (hashlib.sha256(_json(body).encode()).hexdigest() != row["digest"]
                or any(reference.get(key) != value for key, value in result.items())):
            raise RuntimeError("Selection set identity or original Source changed")
        if config is not None and len(_json(body).encode()) * 8 > config.max_buffer_bytes // 4:
            raise MemoryError("Selection set header exceeds max_buffer_bytes")
        frozen = self.describe(source["id"] if receipt is None else receipt)
        if frozen["receipt_id"] != source["id"] or frozen["digest"] != source["digest"]:
            raise RuntimeError("Selection set names a different original Source")
        return result

    def _selection_membership(self, reference, *, config=None, check_canceled=lambda: None):
        """Read the shared member-tree and small chunk index, without bodies."""
        header = self.describe_selection_set(reference, config=config, check_canceled=check_canceled)
        with self._store.connect() as db:
            root = db.execute("SELECT s.root_ref,m.kind,m.item_count FROM input_selection_sets s "
                "JOIN immutable_metadata m ON m.digest=s.root_ref WHERE s.id=?", (header["id"],)).fetchone()
        if root is None or root["kind"] != "selection_membership" or root["item_count"] != header["count"]:
            raise RuntimeError("Registered selection membership is missing or unsupported")
        reader = immutable_metadata.MetadataReader(self._store, memo=self._metadata_memo)
        body = reader.block({"ref": root["root_ref"], "kind": root["kind"], "count": root["item_count"]})
        if body.get("schema") != "selection.membership.v1" or body["members"]["count"] != header["count"]:
            raise RuntimeError("Registered selection membership inventory changed")
        blocks = []
        start = 0
        for block in reader.value(body["blocks"]):
            check_canceled()
            block = dict(block)
            if (set(block) != {"ref", "count"} or type(block["count"]) is not int
                    or not 1 <= block["count"] <= immutable_metadata.BLOCK_SIZE):
                raise RuntimeError("Registered selection chunk inventory changed")
            block["start"] = start
            start += block["count"]
            blocks.append(block)
        if start != header["count"] or len({block["ref"] for block in blocks}) != len(blocks):
            raise RuntimeError("Registered selection chunk inventory changed")
        options = config or ExecutionOptions()
        if len(_json(blocks).encode()) * 8 > options.max_buffer_bytes // 4:
            raise MemoryError("Selection chunk inventory exceeds max_buffer_bytes")
        return header, reader, reader.value(body["members"]), blocks

    def read_selection_set(self, reference: Mapping[str, Any], *, keys: Sequence[str] | None = None,
            config: ExecutionOptions | None = None,
            check_canceled: Callable[[], None] = lambda: None) -> tuple[dict[str, Any], ...]:
        """Read selected descriptor keys; all-key reads are explicit preparation/audit."""
        scoped = (self._read_context() is None
            and getattr(self._store._thread_state, "writer_connection", None) is None)
        # Admit the registered header before its claimed Source, and keep lazy
        # descendants on that same finite SQLite snapshot. An existing writer
        # must retain visibility of its uncommitted selection inventory.
        with self._store.read_view() if scoped else nullcontext():
            header = self.describe_selection_set(reference, config=config, check_canceled=check_canceled)
            if isinstance(keys, (str, bytes)):
                raise ValueError("Selection set keys require a finite sequence")
            if keys is not None and not keys:
                return ()
            context = (self.read_context(header["source"]["id"], config=config, check_canceled=check_canceled)
                if scoped else nullcontext())
            with context:
                membership = self._selection_membership(reference, config=config,
                    check_canceled=check_canceled)
                return self._read_selection_members(*membership, keys=keys, config=config,
                    check_canceled=check_canceled)

    def _read_selection_members(self, header, reader, members, blocks, *, keys,
            config=None, check_canceled=lambda: None):
        """Read a bounded group through an already validated membership view."""
        check_canceled()
        with self._store.connect() as db:
            query = "SELECT e.descriptor_key,e.ordinal,e.reference_json,e.block_ref," \
                "json_extract(b.value,'$.start') AS start,json_extract(b.value,'$.count') AS block_count " \
                "FROM input_selection_chunk_entries e JOIN json_each(?) b " \
                "ON e.block_ref=json_extract(b.value,'$.ref')"
            if keys is None:
                rows = db.execute(query + " ORDER BY start,e.ordinal", (_json(blocks),)).fetchall()
                if len(rows) != header["count"]:
                    raise RuntimeError("Registered selection chunk entries are incomplete")
            else:
                rows = db.execute(query + " WHERE e.descriptor_key IN(SELECT value FROM json_each(?)) ORDER BY start,e.ordinal",
                    (_json(blocks), _json(list(keys)))).fetchall()
                if len(rows) != len(set(keys)):
                    raise KeyError("Selection set descriptor key is unknown")
        source = self.get(header["source"]["id"])
        if source.digest != header["source"]["digest"]:
            raise RuntimeError("Selection set names a different original Source")
        result, charge = [], 0
        options = config or ExecutionOptions()
        for row in rows:
            check_canceled()
            if not 0 <= row["ordinal"] < row["block_count"]:
                raise RuntimeError("Registered selection chunk ordinal changed")
            member = members[row["start"] + row["ordinal"]]
            if member["key"] != row["descriptor_key"] or member["reference"] != json.loads(row["reference_json"]):
                raise RuntimeError("Registered selection membership differs from its immutable root")
            value = json.loads(_json(reader.value(json.loads(row["reference_json"]))))
            request = input_from_payload(value["request"])
            effective, cutoff = self._selection_scope(source, value["alias"], start=request.start, end=request.end,
                as_of=value["as_of"], view=request.view, strict=request.strict)
            if _json(input_payload(effective)) != _json(value["request"]):
                raise RuntimeError("Registered selection differs from its original Source request")
            value.update(source=header["source"], requested_cutoff=None if cutoff is None else str(cutoff))
            # Descriptor dictionaries contain bounded nested public JSON, not
            # lazy source-history views, so the public wire shape stays ordinary.
            value = json.loads(_json(value))
            charge += len(_json(value).encode()) * 8
            if charge > options.max_buffer_bytes // 4:
                raise MemoryError("Selected descriptors exceed max_buffer_bytes")
            if self.selection_descriptor_identity(value) != row["descriptor_key"]:
                raise RuntimeError("Registered selection descriptor checksum mismatch")
            result.append(value)
        return tuple(result)

    def range_identity(self, receipt: FrozenInputReceipt | str, alias: str, *,
                       start: DateLike | None = None, end: DateLike | None = None,
                       fields: tuple[str,...] | list[str] = (),
                       coordinates: pl.DataFrame | None = None) -> str:
        """Identify a frozen range from compact immutable segment/change metadata.

        New segments outside the range do not change its identity. In-range changes
        conservatively invalidate all requested fields/coordinates; optional exact
        consumed summaries can prove stronger equality. No original files are read.
        """
        context = self._read_context()
        options = context.options if context is not None else ExecutionOptions()
        frozen = self._summary_source(receipt,max_bytes=options.max_buffer_bytes)
        request,cutoff = self._selection_scope(frozen,alias,start=start,end=end,as_of=None,view=None,strict=None)
        return self._range_selection(frozen,alias,request,cutoff,coordinates=coordinates,
            fields=tuple(fields) or None)["identity"]

    def _range_selection(self, frozen, alias, request, cutoff, *, coordinates=None,
                         fields=None, values_only=False, causal_versions=False, coordinate_key=None):
        evidence = frozen.evidence[alias]
        # Keep actual original bounds for every segment, so interval tokens never
        # depend on enclosing receipt IDs, global commit/check counters or run IDs.
        batches = _selection_batches(frozen,alias,request,cutoff,fields=fields)
        if (values_only and fields is None and coordinates is None and coordinate_key is None
                and isinstance(request, ItemInput) and not batches):
            # Ordinary scalar planning already uses this canonical empty token.
            # Metadata range publication retains the whole Source's parent
            # provenance; its audit must reproduce both channels independently.
            empty = self._empty_scalar_selection(frozen, alias, request, causal_versions)
            return {"kind": "segment_range.v1", "identity": empty["identity"],
                "parents": sorted(evidence["parent_receipts"]), "has_rows": False}
        selected = None
        if evidence["general_snapshots"]:
            selected = _selected_general(frozen,alias,request,cutoff)
            batches = [] if selected is None else [batch for batch in batches if batch["commit_seq"] == selected["seq"]]
        lower = max((_date_value(value) for value in (request.start,getattr(request,"observation_start",None)) if value is not None),default=None)
        upper = min((_date_value(value) for value in (request.end,getattr(request,"observation_end",None)) if value is not None),default=None)
        batches = [batch for batch in batches if not (
            lower is not None and batch.get("max_observation") is not None and _date_value(batch["max_observation"]) < lower
            or upper is not None and batch.get("min_observation") is not None and _date_value(batch["min_observation"]) > upper
            or cutoff is not None and batch.get("min_available") is not None and _date_value(batch["min_available"]) > cutoff
                and selected is None)]
        events = _range_events(evidence, batches, request, cutoff)
        consumed = tuple(fields) if fields else (("value",) if values_only and isinstance(request,ItemInput) else ())
        selected_changes = []
        if consumed:
            for batch in batches:
                changes = json.loads(batch.get("field_changes_json","{}"))
                for field in consumed:
                    change = changes.get(field)
                    if change is None and field in changes.get("$initial",{}).get("fields",()):
                        change = changes["$initial"]
                    if change is None:
                        continue
                    if lower is not None and change["upper"] is not None and _date_value(change["upper"]) < lower:
                        continue
                    if upper is not None and change["lower"] is not None and _date_value(change["lower"]) > upper:
                        continue
                    if coordinates is not None and change.get("asset_lower") is not None:
                        matched_coordinates = coordinates.filter(pl.col("asset_id").is_between(pl.lit(change["asset_lower"]),pl.lit(change["asset_upper"])))
                        if change["lower"] is not None:
                            matched_coordinates = matched_coordinates.filter(pl.col("time") >= _date_value(change["lower"]))
                        if change["upper"] is not None:
                            matched_coordinates = matched_coordinates.filter(pl.col("time") <= _date_value(change["upper"]))
                        if matched_coordinates.is_empty():
                            continue
                    if change["identity"] is not None:
                        selected_changes.append((field,change["identity"]))
                    if (causal_versions or request.strict or request.view == "history") and change.get("timing_identity") is not None:
                        selected_changes.append((field,"timing",change["timing_identity"]))
        typed_schema = self._selection_schema(frozen,alias,request=request,cutoff=cutoff)
        consumed_schema = [(field,str(typed_schema.get(field))) for field in consumed] if consumed else evidence["schema_hash"]
        body = {"version":"segment_range.v1","definition":evidence["definition_hash"],
            "schema":consumed_schema,"request":input_payload(request),
            "fields":fields,"values_only":values_only,"causal_versions":causal_versions,
            "coordinates":coordinate_key if coordinates is None else frame_content_hash(coordinates),
            "segments":(selected_changes if consumed else [(batch["partition_path"],batch["content_hash"]) for batch in batches]),
            "visibility":([] if not request.strict and (isinstance(request,RawInput) and request.view == "history"
                or values_only and not causal_versions and request.view != "history") else [(row["content_hash"],row["pit_date"]) for row in events]),
            "general":(None if consumed else selected),"empty_baseline":[dict(row) for row in evidence["empty_checks"] if row["baseline"]],
            "empty_scopes":[{key:row[key] for key in ("pit_date","request_json","input_receipt_id")} for row in evidence["empty_checks"]
                if not any(batch["row_count"] for batch in batches) and not row["baseline"] and json.loads(row["request_json"])
                and (cutoff is None or _date_value(row["pit_date"]) <= cutoff)],
            "empty_completion":(evidence["empty_item_build"] if not any(batch["row_count"] for batch in batches) else None)}
        return {"kind":"segment_range.v1","identity":hashlib.sha256(_json(body).encode()).hexdigest(),
            "parents":sorted(evidence["parent_receipts"]),"has_rows":any(batch["row_count"] for batch in batches)}

    def selection_identity(self, receipt: FrozenInputReceipt | str, alias: str, *,
                           start: DateLike | None = None, end: DateLike | None = None,
                           as_of: DateLike | None = None, view: str | None = None,
                           strict: bool | None = None, values_only: bool = False,
                           causal_versions: bool = False,
                           coordinates: pl.DataFrame | None = None,
                           fields: tuple[str, ...] | list[str] | None = None) -> str | None:
        """Return a stable compact range token without reading or preparing inputs.

        Optional exact consumed summaries do not change this token. New segments
        outside the range and changes to unconsumed fields retain the identity.
        Date/asset change bounds conservatively admit coordinate intersections;
        explicit selection_equivalent can use registered exact scalar summaries.
        """
        context = self._read_context()
        options = context.options if context is not None else ExecutionOptions()
        frozen = self._summary_source(receipt,max_bytes=options.max_buffer_bytes)
        if alias not in frozen.requests:
            raise KeyError(f"Unknown frozen input alias: {alias}")
        fields = _selection_fields(fields)
        values_only = values_only or fields is not None
        request = frozen.requests[alias]
        if values_only and fields is None:
            _validate_raw_payload_projection(request)
        if causal_versions and (not values_only or (view or request.view) != "versions" or coordinates is not None):
            raise ValueError("causal_versions requires a scalar versions projection without coordinates")
        request, cutoff = self._selection_scope(frozen, alias, start=start, end=end, as_of=as_of,
            view=view, strict=strict)
        self._validate_selection_projection(frozen, alias, request, fields=fields,
            values_only=values_only, causal_versions=causal_versions, coordinates=coordinates)
        self._validate_selection_fields(frozen, alias, fields, request=request, cutoff=cutoff)
        try:
            coordinates = self._selection_coordinates(request, coordinates, values_only=values_only, fields=fields)
        except MemoryError:
            return None
        value = self._indexed_selection(frozen, alias, request=request, cutoff=cutoff, values_only=values_only,
            coordinates=coordinates, causal_versions=causal_versions, fields=fields,
            summaries_only=fields is not None)
        return None if value is None else value["identity"]

    def selection_descriptors(self, receipt: FrozenInputReceipt | str, scopes: Sequence[InputSelectionScope], *,
                              config: ExecutionOptions | None = None,
                              check_canceled: Callable[[], None] = lambda: None) -> tuple[dict[str, Any], ...]:
        """Describe one finite group's registered exact projections without preparation.

        Actual coordinates are required; registered selector references belong
        to explicit maintenance. Unknown scopes retain their canonical locator
        but never decode original rows/indexes or write optional records.
        """
        from bagelquant_data import input_index
        if (not isinstance(scopes, Sequence) or isinstance(scopes, (str, bytes))
                or not 1 <= len(scopes) <= 16):
            raise ValueError("Selection descriptors require a finite group of 1..16 scopes")
        if any(not isinstance(scope, InputSelectionScope) for scope in scopes):
            raise TypeError("Selection descriptors require InputSelectionScope")
        if len({scope.alias for scope in scopes}) != 1:
            raise ValueError("Selection descriptors admit only one alias per group")
        if any(scope.coordinate_selector is not None for scope in scopes):
            raise ValueError("Selection descriptors require actual coordinates, not registered selector references")
        context = self._read_context()
        options = config or (context.options if context is not None else ExecutionOptions())
        charge = 2048 * len(scopes) + sum(8 * scope.coordinates.estimated_size()
            for scope in scopes if isinstance(scope.coordinates, pl.DataFrame))
        if charge > options.max_buffer_bytes // 4:
            raise MemoryError("Aggregate selection descriptor admission exceeds max_buffer_bytes")
        check_canceled()
        frozen = self._summary_source(receipt, max_bytes=options.max_buffer_bytes)
        descriptors = []
        with self.read_context(frozen, config=options, check_canceled=check_canceled):
            for scope in scopes:
                check_canceled()
                request, cutoff = self._selection_scope(frozen, scope.alias, start=scope.start, end=scope.end,
                    as_of=scope.as_of, view=scope.view, strict=scope.strict)
                fields = _selection_fields(scope.fields)
                self._validate_selection_projection(frozen, scope.alias, request, fields=fields, values_only=True,
                    causal_versions=scope.causal_versions, coordinates=scope.coordinates)
                self._validate_selection_fields(frozen, scope.alias, fields, request=request, cutoff=cutoff)
                if fields is None:
                    _validate_raw_payload_projection(request)
                if (request.view == "versions") != scope.causal_versions:
                    raise ValueError("Value summaries require selected snapshots/history; causal_versions requires an Item")
                coords = self._selection_coordinates(request, scope.coordinates, options=options, fields=fields)
                coordinate = None if coords is None else input_index.identity(coords)
                metadata: dict[str, Any] = {}
                value = self._indexed_selection(frozen, scope.alias, request=request, cutoff=cutoff,
                    values_only=True, causal_versions=scope.causal_versions, summaries_only=True,
                    coordinates=coords, fields=fields, coordinate_key=coordinate, metadata=metadata)
                descriptor = {"source": {"id": frozen.receipt_id, "digest": frozen.digest}, "alias": scope.alias,
                    "request": input_payload(request), **metadata,
                    "as_of": None if scope.as_of is None else str(_date_value(scope.as_of)),
                    "requested_cutoff": None if cutoff is None else str(cutoff),
                    "fields": None if fields is None else list(fields), "coordinate_identity": coordinate,
                    "status": "unknown" if value is None else "known",
                    "content_token": None if value is None else value["identity"],
                    "proof_source": None if value is None else value.get("proof_source"),
                    "has_rows": None if value is None else value["has_rows"]}
                # Keep the public result independent of immutable receipt views
                # and private memo dictionaries, with dates represented as ISO.
                descriptor = json.loads(_json(descriptor))
                charge += len(_json(descriptor).encode()) * 8
                if charge > options.max_buffer_bytes // 4:
                    raise MemoryError("Aggregate selection descriptor metadata exceeds max_buffer_bytes")
                descriptors.append(descriptor)
        check_canceled()
        return tuple(descriptors)

    def resolve_selection_descriptors(self, receipt: FrozenInputReceipt | str,
                                      descriptors: Sequence[Mapping[str, Any]], *,
                                      config: ExecutionOptions | None = None,
                                      check_canceled: Callable[[], None] = lambda: None) -> tuple[dict[str, Any], ...]:
        """Recheck registered descriptor scopes against a specified frozen Source.

        This resolves existing complete proofs and selector headers, without
        decoding selectors/rows, creating an axis, or proving that downstream
        membership is unchanged. New axes require actual coordinate scopes.
        Implicit cutoffs follow the target Source; explicit as_of stays fixed.
        """
        if (not isinstance(descriptors, Sequence) or isinstance(descriptors, (str, bytes))
                or not 1 <= len(descriptors) <= 16):
            raise ValueError("Descriptor resolution requires a finite group of 1..16 descriptors")
        keys = {"source", "alias", "request", "request_key", "evidence_key", "as_of", "requested_cutoff",
            "canonical_cutoff", "fields", "coordinate_identity", "status", "content_token", "proof_source", "has_rows"}
        if any(not isinstance(item, Mapping) or set(item) != keys or item.get("status") != "known"
                for item in descriptors):
            raise ValueError("Descriptor resolution requires complete known Data descriptors")
        if len({item["alias"] for item in descriptors}) != 1:
            raise ValueError("Descriptor resolution admits only one alias per group")
        context = self._read_context()
        options = config or (context.options if context is not None else ExecutionOptions())
        charge = sum(2048 + len(_json(item).encode()) * 8 for item in descriptors)
        if charge > options.max_buffer_bytes // 4:
            raise MemoryError("Aggregate descriptor resolution admission exceeds max_buffer_bytes")
        check_canceled()
        frozen = self._summary_source(receipt, max_bytes=options.max_buffer_bytes)
        results = []
        with self.read_context(frozen, config=options, check_canceled=check_canceled):
            for supplied in descriptors:
                check_canceled()
                reference = supplied["source"]
                if not isinstance(reference, Mapping) or set(reference) != {"id", "digest"}:
                    raise ValueError("Descriptor original source reference is malformed")
                original = self._summary_source(reference["id"], max_bytes=options.max_buffer_bytes)
                if original.digest != reference["digest"]:
                    raise RuntimeError("Descriptor original source checksum mismatch")
                request = input_from_payload(supplied["request"])
                fields = _selection_fields(supplied["fields"])
                coordinate = supplied["coordinate_identity"]
                if coordinate is not None and (not isinstance(coordinate, str) or not coordinate.startswith("arrow-ipc-v1:")
                        or len(coordinate.removeprefix("arrow-ipc-v1:")) != 32
                        or any(char not in "0123456789abcdef" for char in coordinate.removeprefix("arrow-ipc-v1:"))):
                    raise ValueError("Descriptor coordinate identity is malformed")
                resolved = []
                for source in (original, frozen):
                    alias = supplied["alias"]
                    effective, cutoff = self._selection_scope(source, alias, start=request.start, end=request.end,
                        as_of=supplied["as_of"], view=request.view, strict=request.strict)
                    if _json(input_payload(effective)) != _json(supplied["request"]):
                        raise ValueError("Descriptor request differs from the frozen input contract")
                    self._validate_selection_projection(source, alias, effective, fields=fields, values_only=True,
                        causal_versions=request.view == "versions", coordinate_selector=coordinate)
                    self._validate_selection_fields(source, alias, fields, request=effective, cutoff=cutoff)
                    if fields is None:
                        _validate_raw_payload_projection(effective)
                    if coordinate is not None and (request.view == "versions"
                            or isinstance(request, RawInput) and fields is None):
                        raise ValueError("Descriptor coordinates require a selected Item or explicit by-date Raw fields")
                    metadata: dict[str, Any] = {}
                    value = self._indexed_selection(source, alias, request=effective, cutoff=cutoff,
                        values_only=True, causal_versions=request.view == "versions", summaries_only=True,
                        coordinate_key=coordinate, fields=fields, metadata=metadata)
                    result = {"source": {"id": source.receipt_id, "digest": source.digest}, "alias": alias,
                        "request": input_payload(effective), **metadata, "as_of": supplied["as_of"],
                        "requested_cutoff": None if cutoff is None else str(cutoff),
                        "fields": None if fields is None else list(fields), "coordinate_identity": coordinate,
                        "status": "unknown" if value is None else "known",
                        "content_token": None if value is None else value["identity"],
                        "proof_source": None if value is None else value.get("proof_source"),
                        "has_rows": None if value is None else value["has_rows"]}
                    resolved.append(json.loads(_json(result)))
                if resolved[0] != dict(supplied):
                    raise RuntimeError("Descriptor differs from its registered original projection")
                charge += len(_json(resolved[1]).encode()) * 8
                if charge > options.max_buffer_bytes // 4:
                    raise MemoryError("Aggregate resolved descriptor metadata exceeds max_buffer_bytes")
                results.append(resolved[1])
        check_canceled()
        return tuple(results)

    @staticmethod
    def _selection_schema(frozen, alias, *, request=None, cutoff=None):
        evidence = frozen.evidence[alias]
        request = frozen.requests[alias] if request is None else request
        schema_ipc = evidence["schema_ipc"]
        if isinstance(request, RawInput) and evidence["definition"]["update_type"] == "general":
            selected = _selected_general(frozen, alias, request,
                frozen.information_cutoff if cutoff is None else cutoff)
            if selected is None:
                raise ValueError("General consumed fields require a selected atomic snapshot; no eligible event has original schema authority")
            schema_ipc = selected["schema_ipc"]
            if not schema_ipc:
                raise ValueError("Selected General snapshot lacks original typed schema; explicit integrity recovery is required")
        if schema_ipc:
            return pl.Schema(pa.ipc.read_schema(pa.BufferReader(bytes.fromhex(schema_ipc))))
        if isinstance(request, ItemInput):
            from bagelquant_data.transforms import scalar_dtype
            return pl.Schema({"time": pl.Date, "source_time": pl.Date, "asset_id": pl.String,
                "value": scalar_dtype(evidence["definition"]["value_dtype"]), "_baseline": pl.Boolean})
        return pl.Schema()

    @classmethod
    def _validate_selection_fields(cls, frozen, alias, fields, *, request=None, cutoff=None):
        if fields is None:
            return
        request = frozen.requests[alias] if request is None else request
        schema = cls._selection_schema(frozen, alias, request=request, cutoff=cutoff)
        names = {"version_available_date" if name == "time" else "time" if name == "source_time" else name
            for name in schema} if isinstance(request, ItemInput) else set(schema)
        missing = set(fields) - names
        if missing:
            raise ValueError(f"Frozen input is missing requested fields: {sorted(missing)}")
        if _general_fields(frozen, alias, fields):
            # Ordinary General reads concatenate retained snapshots before the
            # atomic selector. A conflicting consumed dtype can round old
            # values on that intermediate cast, even for an unselected future
            # snapshot. Do not certify the narrower original-batch projection.
            for snapshot in frozen.evidence[alias]["general_snapshots"]:
                if not snapshot["schema_ipc"]:
                    raise ValueError("General consumed fields require every captured atomic original schema; unsupported field proof without schema authority")
                captured = pl.Schema(pa.ipc.read_schema(pa.BufferReader(bytes.fromhex(snapshot["schema_ipc"]))))
                conflicts = [name for name in fields if captured.get(name) != schema[name]]
                if conflicts:
                    raise ValueError(f"General consumed fields have conflicting captured atomic schema at max_commit={snapshot['seq']}: {sorted(conflicts)}; unsupported field proof")

    @staticmethod
    def _validate_selection_projection(frozen, alias, request, *, fields, values_only,
                                       causal_versions, coordinates=None, coordinate_selector=None):
        if _general_fields(frozen, alias, fields):
            if (request.view != "snapshot" or causal_versions or coordinates is not None
                    or coordinate_selector is not None
                    or any(value is not None for value in (request.start, request.end,
                        request.observation_start, request.observation_end))):
                raise ValueError("General consumed fields require a whole snapshot without windows, coordinates or causal_versions")
            return
        if values_only and isinstance(request, RawInput) and (causal_versions or request.view == "versions"
                or frozen.evidence[alias]["definition"]["update_type"] != "by_date"):
            raise ValueError("Raw values_only requires a by-date selected snapshot/history; General requires explicit snapshot fields; causal_versions requires a DataItem")
        if values_only and request.view == "versions" and not causal_versions:
            raise ValueError("values_only versions require causal_versions")

    def _selection_coordinates(self, request, coordinates, *, values_only=True, options=None, fields=None):
        if coordinates is not None:
            allowed = isinstance(request, ItemInput) or (
                isinstance(request, RawInput) and fields is not None and request.view in {"snapshot", "history"})
            if (not values_only or not allowed or not isinstance(coordinates, pl.DataFrame)
                    or dict(coordinates.schema) != {"time": pl.Date, "asset_id": pl.String}):
                raise ValueError("coordinates require an Item scalar projection or explicit by-date Raw fields projection with Date time and String asset_id")
            context = self._read_context()
            budget = (options or (context.options if context is not None else ExecutionOptions())).max_buffer_bytes
            if coordinates.estimated_size() > budget // 4:
                raise MemoryError("Coordinate selection exceeds max_buffer_bytes")
            if (coordinates.null_count().sum_horizontal().sum() or coordinates["asset_id"].str.len_chars().eq(0).any()
                    or coordinates.n_unique() != coordinates.height):
                raise ValueError("coordinates must have unique non-null time/asset keys")
            lower = (request.start, request.observation_start) if isinstance(request, RawInput) else (request.start,)
            upper = (request.end, request.observation_end) if isinstance(request, RawInput) else (request.end,)
            if (any(value is not None and coordinates["time"].lt(_date_value(value)).any() for value in lower)
                    or any(value is not None and coordinates["time"].gt(_date_value(value)).any() for value in upper)):
                raise ValueError("coordinates cannot widen the frozen observation window")
            coordinates = coordinates.select("time", "asset_id").sort("time", "asset_id")
        return coordinates

    @staticmethod
    def _selection_scope(frozen, alias, *, start, end, as_of, view, strict):
        request = frozen.requests[alias]
        for supplied, bound, lower in ((start, request.start, True), (end, request.end, False)):
            if supplied is not None and bound is not None and ((_date_value(supplied) < _date_value(bound)) if lower else (_date_value(supplied) > _date_value(bound))):
                raise ValueError("selection cannot widen the frozen observation window")
        cutoff = frozen.information_cutoff if as_of is None else _date_value(as_of)
        if cutoff is not None and frozen.information_cutoff is not None and cutoff > frozen.information_cutoff:
            raise ValueError("as_of exceeds the frozen information cutoff")
        request = replace(request, start=request.start if start is None else start,
                          end=request.end if end is None else end,
                          view=request.view if view is None else view,
                          strict=request.strict if strict is None else strict)
        if request.start is not None and request.end is not None and _date_value(request.start) > _date_value(request.end):
            raise ValueError("end precedes start")
        if request.view not in {"snapshot", "history", "versions"}:
            raise ValueError("Frozen input view must be history, snapshot, or versions")
        if request.view == "snapshot" and cutoff is None:
            raise ValueError("snapshot requests require an information cutoff")
        return request, cutoff

    def _current_selection_receipt(self, original, *, cutoff, config=None):
        """Capture current alias metadata in the existing fixed read view."""
        if self._boundary is not None and self._boundary.as_of is not None and (
                cutoff is None or cutoff > self._boundary.as_of):
            raise ValueError("Current selection exceeds the active frozen information boundary")
        return self._capture_current_receipt(original, cutoff=cutoff, config=config)

    def _capture_current_receipt(self, original, *, cutoff, config=None):
        """Capture exact registered requests once, without changing caller cutoff semantics."""
        context = self._read_context()
        options = config or (context.options if context is not None else ExecutionOptions())
        if context is not None and context.options.max_buffer_bytes != options.max_buffer_bytes:
            context = None
        if context is not None and context.check_canceled is not None:
            context.check_canceled()
        compact = {alias for alias, evidence in original.evidence.items() if "full_commit_checks" in evidence}
        scoped = {alias for alias, evidence in original.evidence.items() if evidence.get("scoped_batches")}
        retained = {alias for alias, evidence in original.evidence.items() if evidence.get("retained_batch_bounds")}
        key = _json({"requests": {alias: input_payload(request) for alias, request in original.requests.items()},
            "cutoff": cutoff, "boundary": None if self._boundary is None else {
                "commit": self._boundary.max_commit, "check": self._boundary.max_check_id,
                "as_of": self._boundary.as_of}, "compact": sorted(compact),
            "scoped": sorted(scoped), "retained": sorted(retained),
            "write_generation": None if context is None or context.database is None else context.database.total_changes})
        memo = self._metadata_memo() if context is not None else None
        memo_key = ("current_capture", key)
        cached = None if memo is None or not memo.enabled else memo.get(memo_key)
        if cached is not None:
            return cached[0]
        with self._store.connect() as db:
            if not db.in_transaction:
                db.execute("begin")
            payload = self._capture(db, original.requests, cutoff=cutoff,
                max_commit=None if self._boundary is None else self._boundary.max_commit,
                compact_aliases=compact, scoped_aliases=scoped, retained_bounds_aliases=retained,
                max_buffer_bytes=options.max_buffer_bytes)
        serialized = _json(payload)
        value = self._receipt("current-selection", payload, hashlib.sha256(serialized.encode()).hexdigest())
        # Same-thread nested readers share the owner's SQLite snapshot. Keep
        # only immutable headers, with a finite entry and buffer reserve; a
        # different request/boundary or a later operation always captures anew.
        size = (len(serialized.encode()) + len(key.encode())) * 8 + 256
        if memo is not None and memo.enabled and size <= memo.maximum_bytes:
            value = replace(value, requests=_readonly(value.requests), evidence=_readonly(value.evidence))
            memo.put(memo_key, value, size)
        return value

    def _currentness_selection(self, frozen, alias, *, general=False):
        """Reuse an immutable alias fingerprint inside this fixed metadata view."""
        memo = self._metadata_memo()
        evidence = frozen.evidence
        reference = (_json(evidence.references[alias])
            if isinstance(evidence, immutable_metadata.FrozenEvidenceMapping) else frozen.digest + ":" + alias)
        key = ("currentness_selection", general, reference,
            _selection_key(frozen.requests[alias], frozen.information_cutoff))
        cached = None if memo is None or not memo.enabled else memo.get(key)
        if cached is not None:
            return cached[0]
        value = (_general_metadata_selection(frozen, alias) if general
            else _content_metadata_selection(frozen, alias))
        if value is not None:
            value = hashlib.sha256(value.encode()).hexdigest()
        if memo is not None and memo.enabled:
            memo.put(key, value, 256 + 8 * (len(reference.encode()) + len(key[-1].encode())
                + (0 if value is None else len(value.encode()))))
        return value

    def selection_equivalent(self, original: FrozenInputReceipt | str, current: FrozenInputReceipt | str | None,
                             alias: str, *, start: DateLike | None = None, end: DateLike | None = None,
                             as_of: DateLike | None = None, current_as_of: DateLike | None = None,
                             view: str | None = None, strict: bool | None = None,
                             values_only: bool = False, causal_versions: bool = False,
                             records_only: bool = False, summaries_only: bool = False,
                             coordinates: pl.DataFrame | None = None,
                             fields: tuple[str, ...] | list[str] | None = None) -> bool | None:
        """Compare actual consumed projections using records; absent proof is unknown.

        Exact retained content/witness equality avoids needing row indexes.
        Otherwise compare optional canonical selection proofs. This establishes
        consumed-content equivalence, independently of producer currentness.
        It never calculates, audits bytes, freezes receipts or warms indexes.
        ``records_only=True`` also admits equal compact scalar snapshot ranges
        when their selected values are fully visible and relevant visibility
        witnesses are unchanged. Raw observation bounds
        are intersected only for this comparison; stored tokens stay unchanged.
        Otherwise it returns unknown without consulting optional summaries.
        ``summaries_only=True`` permits existing canonical summary records but
        does not decode per-row indexes.
        ``fields`` uses typed projected summaries and implies that restriction;
        ``coordinates`` binds actual Item keys or, with explicit fields,
        by-date Raw observation keys, including absent keys. Both selectors
        retain the original PIT and baseline admission.
        """
        before = self.get(original)
        original_alias = replace(before, requests={alias: before.requests[alias]}, evidence={alias: before.evidence[alias]})
        after = (self._current_selection_receipt(original_alias,
            cutoff=before.information_cutoff if current_as_of is None else _date_value(current_as_of))
            if current is None else self.get(current))
        return self._selection_equivalent(before, after, alias, start=start, end=end,
            as_of=as_of, current_as_of=current_as_of, view=view, strict=strict,
            values_only=values_only, causal_versions=causal_versions,
            records_only=records_only, summaries_only=summaries_only,
            coordinates=coordinates, fields=fields)

    def _selection_equivalent(self, before: FrozenInputReceipt, after: FrozenInputReceipt,
                              alias: str, *, start: DateLike | None = None, end: DateLike | None = None,
                              as_of: DateLike | None = None, current_as_of: DateLike | None = None,
                              view: str | None = None, strict: bool | None = None,
                              values_only: bool = False, causal_versions: bool = False,
                              records_only: bool = False, summaries_only: bool = False,
                              coordinates: pl.DataFrame | None = None,
                              fields: tuple[str, ...] | list[str] | None = None) -> bool | None:
        """Compare resolved original metadata in the caller's fixed read view."""
        context = self._read_context()
        if context is not None and context.check_canceled is not None:
            context.check_canceled()
        fields = _selection_fields(fields)
        values_only = values_only or fields is not None
        summaries_only = summaries_only or fields is not None
        scoped = []
        selected_requests = []
        effective_requests = []
        for frozen, cutoff in ((before, as_of), (after, current_as_of)):
            request, day = self._selection_scope(frozen, alias, start=start, end=end, as_of=cutoff,
                view=view, strict=strict)
            self._validate_selection_projection(frozen, alias, request, fields=fields,
                values_only=values_only, causal_versions=causal_versions, coordinates=coordinates)
            self._validate_selection_fields(frozen, alias, fields, request=request, cutoff=day)
            try:
                coordinates = self._selection_coordinates(request, coordinates, values_only=values_only, fields=fields)
            except MemoryError:
                return None
            if values_only and fields is None:
                _validate_raw_payload_projection(request)
            effective_requests.append(input_payload(request))
            selected_requests.append(request)
            if causal_versions and (not values_only or request.view != "versions" or coordinates is not None):
                raise ValueError("causal_versions requires a scalar versions projection without coordinates")
            if values_only and request.view == "versions" and not causal_versions:
                raise ValueError("values_only versions require causal_versions")
            evidence = frozen.evidence[alias]
            batches = []
            lower = None if request.start is None else _date_value(request.start)
            upper = None if request.end is None else _date_value(request.end)
            captured_bounds = bool(evidence.get("scoped_batches") or evidence.get("retained_batch_bounds"))
            candidates = evidence["batches"]
            if captured_bounds and isinstance(candidates, immutable_metadata.LazyMetadataSequence):
                # Use the same retained bounds before decoding unrelated segment
                # headers. Missing bounds stay conservative; summary-only Raw
                # bounds below still apply independently to each candidate.
                candidates = candidates.selected(lower, upper, day)
            with self._store.connect() as bounds_db:
                for batch in candidates:
                    bounded = captured_bounds
                    if (not bounded and isinstance(request, RawInput)
                            and evidence["definition"]["update_type"] == "by_date"):
                        # Optional original-batch summaries may narrow this proof,
                        # never the retained receipt's ordinary replay. Missing
                        # summaries retain the whole batch conservatively.
                        from bagelquant_data import input_index
                        summary = input_index.describe_bounds(bounds_db, batch)
                        if summary is not None and summary["record"]:
                            batch = {**batch, "min_observation": summary["lower"],
                                "max_observation": summary["upper"], "min_available": summary["available"]}
                            bounded = True
                    if bounded:
                        if ((lower is not None and batch.get("max_observation") is not None
                                and _date_value(batch["max_observation"]) < lower)
                                or (upper is not None and batch.get("min_observation") is not None
                                    and _date_value(batch["min_observation"]) > upper)
                                or (day is not None and batch.get("min_available") is not None
                                    and _date_value(batch["min_available"]) > day)):
                            continue
                    batches.append(batch)
            scoped.append(replace(frozen, information_cutoff=day,
                requests={alias: replace(request, view="versions")}, evidence={alias: {**evidence, "batches": batches}}))
        old, new = scoped
        if _general_fields(before, alias, fields) and effective_requests[0] == effective_requests[1]:
            old_request, old_cutoff = self._selection_scope(before, alias, start=start, end=end,
                as_of=as_of, view=view, strict=strict)
            new_request, new_cutoff = self._selection_scope(after, alias, start=start, end=end,
                as_of=current_as_of, view=view, strict=strict)
            if _scalar_selection_evidence(before, alias, old_request, old_cutoff, fields=fields) == (
                    _scalar_selection_evidence(after, alias, new_request, new_cutoff, fields=fields)):
                return True
        if effective_requests[0] == effective_requests[1] and not values_only:
            old_general = _general_metadata_selection(old, alias)
            if old_general is not None and old_general == _general_metadata_selection(new, alias):
                return True
        def visible(frozen):
            from bagelquant_data import input_index
            day, selected = frozen.information_cutoff, frozen.requests[alias]
            if day is None:
                return True
            for batch in frozen.evidence[alias]["batches"]:
                if batch.get("max_available") is not None and _date_value(batch["max_available"]) <= day:
                    continue
                if not (isinstance(selected, ItemInput) or frozen.evidence[alias]["definition"]["update_type"] == "by_date"):
                    return False
                selected = _scalar_value_request(selected)
                if selected.end is None:
                    return False
                with self._store.connect() as db:
                    summary = input_index.describe_bounds(db, batch)
                delay = None if summary is None else summary.get("max_availability_delay_days")
                if delay is None or _date_value(selected.end) + timedelta(days=int(delay)) > day:
                    return False
            return True
        fully_visible = all(visible(frozen) for frozen in scoped)
        old_content = _content_metadata_selection(old, alias, values_only=values_only)
        if (effective_requests[0] == effective_requests[1]
                and (old.information_cutoff == new.information_cutoff or fully_visible)
                and old_content is not None and old_content == _content_metadata_selection(new, alias, values_only=values_only)):
            return True
        scalar_snapshot = (values_only and fields is None and coordinates is None and not causal_versions
            and all(request.view == "snapshot" for request in selected_requests)
            and all(isinstance(request, ItemInput) or frozen.evidence[alias]["definition"]["update_type"] == "by_date"
                for frozen, request in zip(scoped, selected_requests, strict=True)))
        range_supported = (scalar_snapshot and fully_visible and not any(request.strict for request in selected_requests)
            and all(not frozen.evidence[alias].get("full_commit_checks")
                and all(row.get("event") for row in frozen.evidence[alias]["record_checks"])
                and (not isinstance(request, ItemInput)
                    or all("field_changes_json" in batch for batch in frozen.evidence[alias]["batches"]))
                for frozen, request in zip(scoped, selected_requests, strict=True)))
        if range_supported and not summaries_only:
            # Admit only these actual scoped Sources. Optional exact summaries
            # may belong to a much wider Source and are unnecessary for this
            # proof. Charge both metadata projections before constructing tokens.
            budget = (context.options if context is not None else ExecutionOptions()).max_buffer_bytes // 4
            charge = 0
            for frozen, request in zip(scoped, selected_requests, strict=True):
                charge += 16 * len(_json(input_payload(request)).encode())
                # Audit-only history and producer declarations do not enter a
                # consumed range token. Keep them lazy and admit the channels
                # actually used by the range/schema and witness comparisons.
                evidence = frozen.evidence[alias]
                for key in ("definition_hash", "schema_hash", "schema_ipc", "batches", "checks",
                        "record_checks", "empty_checks", "empty_item_build", "parent_receipts"):
                    value = evidence[key]
                    rows = value if isinstance(value, (list, tuple, immutable_metadata.LazyMetadataSequence)) else (value,)
                    for row in rows:
                        if context is not None and context.check_canceled is not None:
                            context.check_canceled()
                        charge += 16 * len(_json(row).encode())
                        if charge > budget:
                            return None
            # An attestation can give an older value a later availability date,
            # changing snapshot precedence without adding a value segment.
            # Bind the associated check's eligibility and ingestion ordering too.
            witnesses = []
            for frozen, request in zip(scoped, selected_requests, strict=True):
                evidence = frozen.evidence[alias]
                events = _range_events(evidence, evidence["batches"], request, frozen.information_cutoff)
                identifiers = {row["check_id"] for row in events}
                witnesses.append((events, [dict(row) for row in evidence["checks"] if row["id"] in identifiers]))
            range_supported = witnesses[0] == witnesses[1]
            if range_supported:
                tokens = [self._range_selection(frozen, alias, _scalar_value_request(request),
                    frozen.information_cutoff, values_only=True)["identity"]
                    for frozen, request in zip(scoped, selected_requests, strict=True)]
                if tokens[0] == tokens[1]:
                    return True
        if records_only:
            return None
        # A range token does not encode which rows of a partial batch have
        # become visible. Even equal cutoffs cannot justify ignoring changed
        # timing metadata. Require existing exact summaries in that case.
        exact_summaries = summaries_only or scalar_snapshot and not range_supported
        old_request, old_cutoff = self._selection_scope(before, alias, start=start, end=end, as_of=as_of, view=view, strict=strict)
        new_request, new_cutoff = self._selection_scope(after, alias, start=start, end=end, as_of=current_as_of, view=view, strict=strict)
        old_selection = self._indexed_selection(before, alias, request=old_request, cutoff=old_cutoff,
            values_only=values_only, causal_versions=causal_versions, summaries_only=summaries_only,
            coordinates=coordinates, fields=fields,exact_summaries=exact_summaries)
        if old_selection is None:
            return None
        new_selection = self._indexed_selection(after, alias, request=new_request, cutoff=new_cutoff,
            values_only=values_only, causal_versions=causal_versions, summaries_only=summaries_only,
            coordinates=coordinates, fields=fields,exact_summaries=exact_summaries)
        return None if new_selection is None else old_selection["identity"] == new_selection["identity"]

    def _indexed_selection(self, frozen, alias, *, request=None, cutoff=None, db=None, recompute=False, options=None, values_only=False,
                           coordinates=None, causal_versions=False, summaries_only=False, originals=False, check_canceled=None, fields=None,
                           coordinate_key=None, metadata=None, exact_summaries=False):
        from bagelquant_data import input_index
        request = frozen.requests[alias] if request is None else request
        cutoff = frozen.information_cutoff if cutoff is None else cutoff
        # Resolve the connection before hashing: the former recursive entry
        # computed the same typed selector identity twice per public call.
        if db is None:
            with self._store.connect() as connection:
                return self._indexed_selection(frozen, alias, request=request, cutoff=cutoff, db=connection,
                    recompute=recompute, options=options, values_only=values_only, coordinates=coordinates,
                    causal_versions=causal_versions, summaries_only=summaries_only, originals=originals,
                    check_canceled=check_canceled, fields=fields, coordinate_key=coordinate_key, metadata=metadata,exact_summaries=exact_summaries)
        coordinate_identity = coordinate_key if coordinate_key is not None else (
            None if coordinates is None else input_index.identity(coordinates))
        key = _selection_key(request, cutoff, values_only=values_only,
            causal_versions=causal_versions, coordinates=coordinate_identity, fields=fields)
        context = self._read_context()
        options = options or (context.options if context is not None else ExecutionOptions())
        check = check_canceled or (None if context is None else context.check_canceled)
        if check is not None:
            check()
        memo_key = (frozen.digest, alias, key, summaries_only,recompute,originals,exact_summaries)
        if not recompute and context is not None and context.memo_available and memo_key in context.selections:
            cached = context.selections[memo_key][0]
            proof = cached.get("proof_source")
            if proof is not None and (frozen.digest if proof["id"] == frozen.receipt_id else self.get(proof["id"]).digest) != proof["digest"]:
                raise RuntimeError("Selection summary's original source receipt changed")
            if metadata is not None:
                descriptor = cached.get("_descriptor", {})
                if not descriptor:
                    projection_cutoff = _scalar_complete_cutoff(frozen, alias, request, cutoff, fields=fields)
                    descriptor = {"canonical_cutoff": None if projection_cutoff is None else str(projection_cutoff),
                        "request_key": _selection_key(request, projection_cutoff, values_only=values_only,
                            causal_versions=causal_versions, coordinates=coordinate_identity, fields=fields),
                        "evidence_key": _scalar_selection_evidence(frozen, alias, request, projection_cutoff, fields=fields)}
                    cached = {**cached, "_descriptor": descriptor, "proof_source": proof or {
                        "id": frozen.receipt_id, "digest": frozen.digest, "alias": alias}}
                    self._memo_selection(context, memo_key, cached, options)
                metadata.update(descriptor)
            return cached
        descriptor = {}
        def remember(value, *, source=None):
            result = {**value, "proof_source": source or {
                "id": frozen.receipt_id, "digest": frozen.digest, "alias": alias}, "_descriptor": descriptor}
            if metadata is not None:
                metadata.update(descriptor)
            self._memo_selection(context, memo_key, result, options)
            return result
        if values_only and metadata is not None:
            projection_cutoff = _scalar_complete_cutoff(frozen, alias, request, cutoff, fields=fields)
            descriptor.update({"canonical_cutoff": None if projection_cutoff is None else str(projection_cutoff),
                "request_key": _selection_key(request, projection_cutoff, values_only=True,
                    causal_versions=causal_versions, coordinates=coordinate_identity, fields=fields),
                "evidence_key": _scalar_selection_evidence(frozen, alias, request, projection_cutoff, fields=fields)})
        saved = input_index.selection(db,frozen.digest,alias,key) if exact_summaries and not recompute else None
        if saved is not None:
            return remember(saved)
        if fields is None and values_only and coordinate_identity is None and isinstance(request, ItemInput) and not _selection_batches(
                frozen, alias, request, cutoff):
            return remember(self._empty_scalar_selection(frozen, alias, request, causal_versions))
        if not recompute and values_only:
            if not descriptor:
                projection_cutoff = _scalar_complete_cutoff(frozen, alias, request, cutoff, fields=fields)
                descriptor.update({"canonical_cutoff": None if projection_cutoff is None else str(projection_cutoff),
                    "request_key": _selection_key(request, projection_cutoff, values_only=True,
                        causal_versions=causal_versions, coordinates=coordinate_identity, fields=fields),
                    "evidence_key": _scalar_selection_evidence(frozen, alias, request, projection_cutoff, fields=fields)})
            if metadata is not None:
                metadata.update(descriptor)
            summary = input_index.projection(db, descriptor["evidence_key"], descriptor["request_key"],
                max_bytes=options.max_buffer_bytes // 16)
            if summary is not None and summary.get("kind") == (input_index.VERSION if exact_summaries else "segment_range.v1") and (coordinate_identity is None or input_index.selector_header(
                    db, coordinate_identity, max_bytes=options.max_buffer_bytes // 16) is not None):
                source = self._summary_source(summary["source_receipt_id"], max_bytes=options.max_buffer_bytes)
                if source.digest != summary["source_receipt_digest"]:
                    raise RuntimeError("Scalar selection summary's original source receipt changed")
                return remember(input_index.selection_value(summary),
                    source={"id": source.receipt_id, "digest": source.digest, "alias": summary["alias"]})
        if exact_summaries and not recompute and not originals:
            return None
        if not recompute and not originals:
            if coordinates is None and coordinate_identity is not None:
                if input_index.selector_header(db,coordinate_identity,max_bytes=options.max_buffer_bytes // 16) is not None:
                    coordinates = input_index.read_selector(db,coordinate_identity,max_bytes=options.max_buffer_bytes // 4,
                        check_canceled=check)
            value = self._range_selection(frozen,alias,request,cutoff,
                coordinates=coordinates, fields=fields, values_only=values_only,
                causal_versions=causal_versions,coordinate_key=coordinate_identity)
            if values_only and descriptor:
                earlier = db.execute("select evidence_key,request_key from input_projection_index "
                    "where version='segment_range.v1' and identity=? and request_key=? order by rowid limit 1",
                    (value["identity"],descriptor["request_key"])).fetchone()
                if earlier is not None:
                    original = input_index.projection(db,earlier[0],earlier[1],max_bytes=options.max_buffer_bytes // 16)
                    if original is not None:
                        source = self._summary_source(original["source_receipt_id"],max_bytes=options.max_buffer_bytes)
                        if source.digest != original["source_receipt_digest"]:
                            raise RuntimeError("Original range summary source checksum mismatch")
                        return remember(value,source={"id":source.receipt_id,"digest":source.digest,"alias":original["alias"]})
            return remember(value)
        pieces = []
        if coordinates is not None and coordinates.estimated_size() > options.max_buffer_bytes // 4:
            return None
        buffered = 0 if coordinates is None else coordinates.estimated_size() * 2
        for batch in _selection_batches(frozen, alias, request, cutoff, fields=fields):
            if check is not None:
                check()
            lower, upper = request.start, request.end
            if isinstance(request, RawInput):
                lower = max((_date_value(item) for item in (lower, request.observation_start) if item is not None), default=None)
                upper = min((_date_value(item) for item in (upper, request.observation_end) if item is not None), default=None)
            if originals:
                if not recompute:
                    raise RuntimeError("Original summary reads require explicit recomputation")
                remaining = options.max_buffer_bytes // 2 - buffered
                if remaining <= 0:
                    return None
                frame = input_index.project_original(self._store, batch, max_bytes=remaining,
                    check_canceled=check, values=values_only and isinstance(request, ItemInput)
                        and (fields is None or "value" in fields),
                    start=lower, end=upper, fields=_original_projection_fields(request, fields))
                if values_only and fields is None and isinstance(request, ItemInput):
                    from bagelquant_data.pipeline.versions import _payload_hashes
                    if check is not None:
                        check()
                    frame = frame.with_columns(_payload_hashes(frame, ["value"]).rename("_value_hash")).drop("value")
                    if check is not None:
                        check()
            else:
                frame = input_index.project_original(self._store,batch,
                    max_bytes=max(1,options.max_buffer_bytes // 2-buffered),check_canceled=check,
                    values=values_only and isinstance(request,ItemInput),start=lower,end=upper,
                    fields=_original_projection_fields(request,fields))
            if frame is None:
                return None
            # Attestation joins and concatenation also need headroom.
            buffered += frame.estimated_size() * 2
            if buffered > options.max_buffer_bytes // 2:
                # An incomplete index query never becomes a positive proof.
                return None
            if frame.width:
                pieces.append(frame)
        value = self._reduce_selection(frozen, alias, request, cutoff, pieces, options,
            values_only=values_only, causal_versions=causal_versions, coordinates=coordinates, fields=fields)
        if value is not None:
            self._memo_selection(context, memo_key, value, options)
        return value

    @staticmethod
    def _memo_selection(context, key, value, options):
        """Retain only finite metadata in this unchanged SQLite read view."""
        if context is None or not context.memo_available:
            return
        limit = options.max_buffer_bytes // 64
        charge = 512 + len(_json(key).encode()) * 4 + len(_json(value).encode()) * 8
        if charge > limit:
            return
        total = sum(item[1] for item in context.selections.values())
        if key in context.selections:
            total -= context.selections.pop(key)[1]
        while context.selections and (total + charge > limit or len(context.selections) >= 4096):
            _, removed = context.selections.pop(next(iter(context.selections)))
            total -= removed
        context.selections[key] = value, charge

    def _empty_scalar_selection(self, frozen, alias, request, causal_versions):
        evidence = frozen.evidence[alias]
        body = {"version": "causal_value_versions.v1" if causal_versions else "value.v1",
            "definition": evidence["definition_hash"], "request": input_payload(request), "rows": "empty"}
        if causal_versions:
            body["unknown_empty_baseline"] = [dict(row) for row in evidence["empty_checks"] if row["baseline"]]
        empty = evidence["empty_item_build"]
        parent = None if empty is None else empty["frozen_receipt_id"]
        return {"identity": hashlib.sha256(_json(body).encode()).hexdigest(),
            "parents": [] if parent is None else [parent], "has_rows": False}

    def _reduce_selection(self, frozen, alias, request, cutoff, pieces, options, *,
                          values_only=False, causal_versions=False, coordinates=None, fields=None):
        from bagelquant_data import input_index
        evidence = frozen.evidence[alias]
        coordinate_identity = None if coordinates is None else input_index.identity(coordinates)
        frame = concat_compatible_frames(pieces) if pieces else pl.DataFrame()
        if fields is not None and not frame.width:
            # An empty consumed projection still has the original declared
            # types. This uses receipt metadata, never original batch bytes.
            frame = pl.DataFrame(schema=self._selection_schema(frozen, alias, request=request, cutoff=cutoff))
        witnesses = len(evidence["record_checks"]) + frame.height * len(evidence.get("full_commit_checks", ()))
        if witnesses * max(128, frame.estimated_size() // max(1, frame.height)) > options.max_buffer_bytes // 2:
            return None
        from bagelquant_data.query.raw import _attested_versions
        if frame.width:
            frame = _attested_versions(frame.lazy(), evidence["checks"], evidence["record_checks"],
                as_of_date=cutoff, max_check_id=frozen.max_check_id,
                full_commit_checks=evidence.get("full_commit_checks", ()),event_paths=self._lake._paths).collect()
        selected_general = None
        if evidence["general_snapshots"]:
            from bagelquant_data.items.pit import _general_snapshot_identity
            selected_general = _general_snapshot_identity(evidence["general_snapshots"], cutoff,
                checks=evidence["checks"], strict=request.strict,
                include_historical_baseline=isinstance(request, RawInput) and request.include_historical_baseline)
        metadata_evidence = {**evidence, "general_snapshots": [
            {**item, "schema_ipc": None} for item in evidence["general_snapshots"]]}
        selector = replace(request, fields=()) if isinstance(request, RawInput) else request
        if selector.view == "snapshot" and cutoff is None and frame.height:
            cutoff = cast(date, frame["time"].max())
        selected = self._select(frame, selector, cutoff, evidence=metadata_evidence) if frame.width else frame
        if coordinates is not None and selected.height:
            # Raw request windows and selector keys address observations. Keep
            # physical version availability in its original `time` field; it
            # may independently participate in an explicit consumed projection.
            keys = coordinates.rename({"time": "source_time"}) if isinstance(request, RawInput) else coordinates
            selected = selected.join(keys, on=["source_time" if isinstance(request, RawInput) else "time", "asset_id"],
                how="semi", maintain_order="left")
        commits = {int(batch["commit_seq"]): batch["input_receipt_id"] for batch in evidence["batches"]}
        checks = {int(item["id"]): item["input_receipt_id"] for item in evidence["checks"]}
        parents = set()
        if selected.height and "_commit_seq" in selected.columns:
            for commit, attestation in selected.select("_commit_seq", "_attestation_id").unique().iter_rows():
                parent = commits.get(commit) if attestation is None else checks.get(attestation)
                if parent is not None:
                    parents.add(str(parent))
        empty = evidence["empty_item_build"] if not selected.height else None
        if empty is not None and empty["frozen_receipt_id"] is not None:
            parents.add(empty["frozen_receipt_id"])
        if fields is not None:
            general = _general_fields(frozen, alias, fields)
            if causal_versions and selected.height:
                selected = selected.sort("version_available_date", "ingested_at", "_commit_seq").unique(
                    ["time", "asset_id", "version_available_date"], keep="last", maintain_order=True)
            axes = () if general else ("time", "asset_id", "version_available_date", "_baseline") if causal_versions else (
                ("time", "asset_id") if isinstance(request, ItemInput) else ("source_time", "asset_id"))
            columns = tuple(dict.fromkeys((*axes, *fields)))
            missing = set(columns) - set(selected.columns)
            if missing:
                raise ValueError(f"Frozen input is missing requested fields: {sorted(missing)}")
            projected = selected.select(columns)
            original_schema = self._selection_schema(frozen, alias, request=request, cutoff=cutoff)
            if isinstance(request,ItemInput):
                original_schema = {"version_available_date" if name == "time" else "time" if name == "source_time" else name:dtype
                    for name,dtype in original_schema.items()}
            projected = projected.cast({name:original_schema[name] for name in columns if name in original_schema})
            body = {"version": "general_fields.v1" if general else "causal_fields_versions.v1" if causal_versions else "fields.v1",
                "definition": evidence["definition_hash"], "request": input_payload(request),
                "fields": fields, "schema": [(name, str(dtype)) for name, dtype in projected.schema.items()],
                "rows": input_index.identity(projected)}
            if general:
                body["selected_snapshot"] = selected_general is not None
            elif not selected.height:
                body["unknown_empty_baseline"] = [dict(row) for row in evidence["empty_checks"] if row["baseline"]]
            if coordinates is not None:
                body["coordinates"] = coordinate_identity
        elif values_only:
            # The selector already applied visibility, strictness and version
            # precedence. Its consumed scalar values are independent of creator
            # commits and dependency receipts; those remain provenance above.
            if causal_versions and selected.height:
                # PIT resolution chooses the last creator at each availability
                # for a scalar coordinate. Retain every distinct availability
                # event and baseline flag; redundant creators add no value event.
                selected = selected.sort("version_available_date", "ingested_at", "_commit_seq").unique(
                    ["time", "asset_id", "version_available_date"], keep="last", maintain_order=True)
                scalar = selected.select("time", "asset_id", "version_available_date", "_baseline", "_value_hash").sort(
                    "time", "asset_id", "version_available_date")
            elif isinstance(request, RawInput):
                # Original canonical payloads exclude partition/provenance fields.
                # Actual visibility and precedence were selected above; the
                # consumed observation/value channel excludes timing metadata.
                scalar = selected.select(pl.col("source_time").alias("time"), "asset_id", "_payload_hash").sort(
                    "time", "asset_id") if selected.height else selected
            else:
                scalar = selected.select("time", "asset_id", "_value_hash") if selected.height else selected
            value_request = _scalar_value_request(request)
            body = {"version": "raw_payload_values.v1" if isinstance(request, RawInput)
                    else "causal_value_versions.v1" if causal_versions else "value.v1",
                "definition": evidence["definition_hash"],
                "request": input_payload(value_request), "rows": input_index.identity(
                    scalar) if selected.height else "empty"}
            if isinstance(request, RawInput):
                body["schema"] = evidence["schema_hash"]
            if causal_versions and not selected.height:
                # Unknown empty baseline history remains timing authority. A
                # known empty completion's creator is not a scalar value event.
                body["unknown_empty_baseline"] = [dict(row) for row in evidence["empty_checks"] if row["baseline"]]
            if coordinates is not None:
                body["coordinates"] = coordinate_identity
        else:
            # Only complete-content proofs hash creator/provenance columns.
            # Consumed projections above hash precisely their selected channel.
            body = {"version": input_index.VERSION, "definition": evidence["definition_hash"],
                "schema": evidence["schema_hash"], "request": input_payload(request),
                "rows": input_index.identity(selected) if selected.height else "empty", "general": selected_general,
                "empty": None if empty is None else {k: v for k, v in empty.items()
                    if k not in {"input_commit", "frozen_receipt_id"}},
                "empty_checks": evidence["empty_checks"] if not selected.height else []}
        value = {"identity": hashlib.sha256(_json(body).encode()).hexdigest(),
                 "parents": sorted(parents), "has_rows": bool(selected.height)}
        return value

    def coordinate_equivalent(self, original: FrozenInputReceipt | str, current: FrozenInputReceipt | str | None,
                              alias: str, *, field: str = "time", selector: tuple[str, int] | None = None,
                              start: DateLike | None = None, end: DateLike | None = None,
                              current_as_of: DateLike | None = None,
                              include_historical_baseline: bool | None = None) -> bool | None:
        """Compare a General snapshot date projection without implicit maintenance.

        Identical selected atomic content proves identical coordinates, even
        when attestation timing differs. Otherwise original coordinate indexes
        are required. This says nothing about publication timing or other fields.
        """
        before = self.get(original)
        request = before.requests[alias]
        if not isinstance(request, RawInput) or before.evidence[alias]["definition"]["update_type"] != "general":
            raise ValueError("Coordinate equivalence requires a General Raw input")
        subset = replace(before, requests={alias: request}, evidence={alias: before.evidence[alias]})
        after = (self._current_selection_receipt(subset,
            cutoff=before.information_cutoff if current_as_of is None else _date_value(current_as_of))
            if current is None else self.get(current))
        from bagelquant_data.items.pit import _general_snapshot_identity
        def content(frozen):
            candidate = frozen.requests[alias]
            if not isinstance(candidate, RawInput) or frozen.evidence[alias]["definition"]["update_type"] != "general":
                raise ValueError("Coordinate equivalence requires General Raw inputs")
            effective, _ = self._selection_scope(frozen, alias, start=start, end=end, as_of=None, view="snapshot", strict=None)
            assert isinstance(effective, RawInput)
            effective = replace(effective, fields=(), include_historical_baseline=(effective.include_historical_baseline
                if include_historical_baseline is None else include_historical_baseline))
            self._coordinate_projection(replace(frozen, requests={alias: effective}), alias, field, selector)
            contract = input_payload(effective)
            evidence = frozen.evidence[alias]
            selected = _general_snapshot_identity(evidence["general_snapshots"], frozen.information_cutoff,
                checks=evidence["checks"], strict=candidate.strict,
                include_historical_baseline=(candidate.include_historical_baseline
                    if include_historical_baseline is None else include_historical_baseline))
            if selected is None:
                return {"definition": evidence["definition_hash"], "request": contract, "empty": True}
            return {"definition": evidence["definition_hash"], "request": contract, "schema": selected["schema_ipc"],
                "rows": selected["row_count"], "batches": [{key: batch[key] for key in
                    ("commit_seq", "partition_path", "content_hash", "row_count")}
                    for batch in evidence["batches"] if batch["commit_seq"] == selected["seq"]]}
        if content(before) == content(after):
            return True
        after_request = after.requests[alias]
        assert isinstance(after_request, RawInput)
        if current is None or include_historical_baseline is not None and (
                request.include_historical_baseline != include_historical_baseline
                or after_request.include_historical_baseline != include_historical_baseline):
            return None
        old = self.coordinate_identity(before, alias, field=field, selector=selector, start=start, end=end)
        new = self.coordinate_identity(after, alias, field=field, selector=selector, start=start, end=end)
        return None if old is None or new is None else old == new

    def coordinate_identity(self, receipt: FrozenInputReceipt | str, alias: str, *,
                            field: str = "time", selector: tuple[str, int] | None = None,
                            start: DateLike | None = None, end: DateLike | None = None) -> str | None:
        """Bind a selected General snapshot's complete date axis from records.

        The index is explicitly maintained from this original frozen snapshot.
        Scope clipping changes only the consumed coordinate projection; this
        proves no scalar, source-row or availability equivalence.
        """
        context = self._read_context()
        options = context.options if context is not None else ExecutionOptions()
        frozen = self._summary_source(receipt,max_bytes=options.max_buffer_bytes)
        request, evidence = frozen.requests[alias], frozen.evidence[alias]
        if not isinstance(request, RawInput) or evidence["definition"]["update_type"] != "general":
            raise ValueError("Coordinate identity requires a General Raw input")
        if _selected_general(frozen,alias,request,frozen.information_cutoff) is None:
            return None
        effective, _ = self._selection_scope(frozen, alias, start=start, end=end, as_of=None, view="snapshot", strict=None)
        lower, upper = (None if effective.start is None else _date_value(effective.start),
            None if effective.end is None else _date_value(effective.end))
        if lower is not None and upper is not None and lower > upper:
            raise ValueError("end precedes start")
        for bound, supplied, minimum in ((request.start, lower, True), (request.end, upper, False)):
            if bound is not None and (supplied is None or (supplied < _date_value(bound) if minimum else supplied > _date_value(bound))):
                raise ValueError("Coordinate identity cannot widen the frozen observation window")
        context = self._read_context()
        if context is not None and context.check_canceled is not None:
            context.check_canceled()
        budget = (context.options if context is not None else ExecutionOptions()).max_buffer_bytes // 4
        with self._store.connect() as db:
            coordinates, _ = self._coordinate_record(frozen, alias, field, selector, db=db, max_bytes=budget)
        if coordinates is None:
            return None
        selected = [value for value in coordinates if (lower is None or _date_value(value) >= lower)
            and (upper is None or _date_value(value) <= upper)]
        return hashlib.sha256(_json({"version": "general_coordinates.v1", "definition": evidence["definition_hash"],
            "request": input_payload(effective),
            "field": field, "selector": selector, "coordinates": selected}).encode()).hexdigest()

    def _coordinate_projection(self, frozen, alias, field, selector):
        """Bind original atomic selection and read scope, never enclosing counters."""
        request, evidence = frozen.requests[alias], frozen.evidence[alias]
        if not isinstance(request, RawInput) or evidence["definition"]["update_type"] != "general":
            raise ValueError("Coordinate identity requires a General Raw input")
        if (not isinstance(field, str) or not field.strip()
                or selector is not None and (not isinstance(selector, tuple) or len(selector) != 2
                    or not isinstance(selector[0], str) or not selector[0].strip() or type(selector[1]) is not int)):
            raise ValueError("Coordinate projection requires a field and optional (field, integer) selector")
        effective = replace(request, view="snapshot", fields=())
        selected = _selected_general(frozen, alias, effective, frozen.information_cutoff)
        if selected is None:
            return None
        fields = tuple(dict.fromkeys((field, *((selector[0],) if selector is not None else ()))))
        self._validate_selection_fields(frozen, alias, fields, request=effective, cutoff=frozen.information_cutoff)
        cutoff = frozen.information_cutoff
        if cutoff is not None and selected["snapshot_date"] <= cutoff:
            cutoff = selected["snapshot_date"]
        body = {"version": "general_coordinate_evidence.v1", "definition": evidence["definition_hash"],
            "selected": selected, "batches": [{key: value for key, value in batch.items()
                if key not in {"min_available", "max_available", "min_observation", "max_observation"}}
                for batch in evidence["batches"] if batch["commit_seq"] == selected["seq"]]}
        if selected["attestation_id"] is not None:
            body["check"] = next(dict(row) for row in evidence["checks"] if row["id"] == selected["attestation_id"])
        return hashlib.sha256(_json(body).encode()).hexdigest(), _json({"projection": _COORDINATE_PROJECTION,
            "request": input_payload(effective), "cutoff": cutoff, "field": field, "selector": selector})

    def _coordinate_record(self, frozen, alias, field, selector, *, db, max_bytes, legacy=True, options=None):
        """Resolve a checked locator to its sole original coordinate payload."""
        from bagelquant_data import input_index
        locator = self._coordinate_projection(frozen, alias, field, selector)
        if locator is None:
            return None, None
        summary = input_index.projection(db, *locator, max_bytes=max_bytes)
        if summary is None:
            dates = (input_index.read_coordinates(db, frozen.digest, alias, field, selector, max_bytes=max_bytes)
                if legacy else None)
            return dates, frozen if dates is not None else None
        context = self._read_context()
        options = options or (context.options if context is not None else ExecutionOptions())
        source = self._summary_source(summary["source_receipt_id"], max_bytes=options.max_buffer_bytes)
        if (source.digest != summary["source_receipt_digest"]
                or summary["alias"] not in source.requests
                or self._coordinate_projection(source, summary["alias"], field, selector) != locator):
            raise RuntimeError("Coordinate projection's original source binding changed")
        dates = input_index.read_coordinates(db, source.digest, summary["alias"], field, selector, max_bytes=max_bytes)
        if dates is None:
            return None, None
        if (summary["identity"] != input_index.coordinate_content_identity(dates)
                or summary["parents"] or summary["has_rows"] != bool(dates)):
            raise RuntimeError("Coordinate projection differs from its original payload")
        return dates, source

    def _summary_source(self, receipt, *, max_bytes):
        """Admit summary source metadata before decoding its immutable JSON."""
        key = receipt.receipt_id if isinstance(receipt, FrozenInputReceipt) else receipt
        context = self._read_context()
        if context is not None and context.check_canceled is not None:
            context.check_canceled()
        with self._store.connect() as db:
            row = db.execute("SELECT octet_length(payload_json) AS bytes FROM frozen_inputs WHERE receipt_id=?", (key,)).fetchone()
            if row is not None and row["bytes"] * 16 > max_bytes // 4:
                raise MemoryError("Scalar summary original source metadata exceeds max_buffer_bytes; use an admitted buffer or narrower frozen request")
        return self.get(receipt)

    def index_selection(self, receipt: FrozenInputReceipt | str, alias: str, *,
                        start: DateLike | None = None, end: DateLike | None = None,
                        as_of: DateLike | None = None, view: str | None = None, strict: bool | None = None,
                        causal_versions: bool = False, coordinates: pl.DataFrame | None = None,
                        fields: tuple[str, ...] | list[str] | None = None,
                        config: ExecutionOptions | None = None,
                        rebuild: bool = False, check_canceled: Callable[[], None] = lambda: None) -> str | None:
        """Explicitly build a scalar summary from bounded original typed evidence.

        Persist only the exact summary and shared coordinate selector; do not
        construct or persist per-row indexes. The summary binds selected immutable headers, witnesses,
        effective request and cutoff, plus its original registered source.
        Unrelated counters/aliases and known empty covers do not invalidate it.
        No producer or provider runs and no original receipt is modified.
        ``fields`` records an exact typed consumed projection with keys and
        projected empty schema, through the same selector and summary store.
        """
        return self.index_selections(receipt, [InputSelectionScope(alias, start, end, as_of, view,
            strict, causal_versions, coordinates, fields=_selection_fields(fields))], config=config, rebuild=rebuild,
            check_canceled=check_canceled)[0]

    def coordinate_selector_identity(self, coordinates: pl.DataFrame, *,
                                     config: ExecutionOptions | None = None,
                                     check_canceled: Callable[[], None] = lambda: None) -> str:
        """Return the canonical typed selector identity without registration or I/O.

        This identifies coordinates only; no Source or consumed-value equality
        is granted. index_selections validates each requested observation scope.
        """
        from bagelquant_data import input_index
        context = self._read_context()
        options = config or (context.options if context is not None else ExecutionOptions())
        if context is not None and context.check_canceled is not None:
            context.check_canceled()
        check_canceled()
        if not isinstance(coordinates, pl.DataFrame):
            raise ValueError("coordinates require a Date/String DataFrame")
        if 8 * coordinates.estimated_size() > options.max_buffer_bytes // 4:
            raise MemoryError("Coordinate selector identity admission exceeds max_buffer_bytes")
        normalized = self._selection_coordinates(ItemInput("_coordinate_selector"), coordinates, options=options)
        assert normalized is not None
        result = input_index.identity(normalized)
        check_canceled()
        return result

    def prepare_range_selections(self, receipt: FrozenInputReceipt | str,
                                 scopes: Sequence[InputSelectionScope], *,
                                 config: ExecutionOptions | None = None,
                                 check_canceled: Callable[[],None] = lambda: None) -> tuple[dict[str,Any],...]:
        """Save compact metadata range summaries during explicit Snapshot preparation.

        The writer retains immutable parent-ID tuples only for consumed aliases
        in each verified proof Source; entry and closing checks remain fresh.
        """
        from bagelquant_data import input_index
        self._store.ensure_writable()
        if self._read_context() is not None:
            raise RuntimeError("Range summary publication requires an independent writer")
        frozen = self.get(receipt)
        descriptors = self.selection_descriptors(frozen,scopes,config=config,check_canceled=check_canceled)
        aliases_by_source: dict[tuple[str, str], set[str]] = {}
        for descriptor in descriptors:
            source = descriptor["proof_source"]
            aliases_by_source.setdefault((source["id"], source["digest"]), set()).add(source["alias"])
        with self._store.connect() as db:
            db.execute("begin immediate")
            parents_by_source: dict[tuple[str, str], Mapping[str, tuple[str, ...]]] = {}
            for descriptor in descriptors:
                check_canceled()
                source = descriptor["proof_source"]
                source_key = source["id"], source["digest"]
                if source_key not in parents_by_source:
                    authority = self.get(source["id"])
                    if authority.digest != source["digest"]:
                        raise RuntimeError("Frozen input receipt checksum mismatch")
                    parents_by_source[source_key] = MappingProxyType({
                        alias: tuple(sorted(authority.evidence[alias]["parent_receipts"]))
                        for alias in sorted(aliases_by_source[source_key])})
                    del authority
                input_index.save_projection(db,descriptor["evidence_key"],descriptor["request_key"],
                    {"kind":"segment_range.v1","identity":descriptor["content_token"],"parents":parents_by_source[source_key][source["alias"]],
                     "has_rows":descriptor["has_rows"]}, receipt_id=source["id"],
                    receipt_digest=source["digest"],alias=source["alias"])
        # Return the same public descriptor shape as future metadata reads.
        return self.selection_descriptors(frozen,scopes,config=config,check_canceled=check_canceled)

    def prepare_coordinate_selector(self, coordinates: pl.DataFrame, *,
                                    config: ExecutionOptions | None = None,
                                    check_canceled: Callable[[],None] = lambda: None) -> str:
        """Publish a small caller-supplied Domain selector, without reading input rows."""
        from bagelquant_data import input_index
        self._store.ensure_writable()
        if self._read_context() is not None:
            raise RuntimeError("Coordinate selector publication requires an independent writer")
        options = config or ExecutionOptions()
        key = self.coordinate_selector_identity(coordinates,config=options,check_canceled=check_canceled)
        with self._store.connect() as db:
            db.execute("begin immediate")
            check_canceled()
            input_index.save_selector(db,coordinates.sort("time","asset_id"),max_bytes=options.max_buffer_bytes)
        return key

    def prepare_general_projections(self, original: FrozenInputReceipt | str, alias: str,
                                    fields: tuple[str, ...] | list[str], *, through: DateLike,
                                    key_anchors: tuple[str, ...] = (),
                                    completed_start: DateLike | None = None,
                                    completed_end: DateLike | None = None,
                                    config: ExecutionOptions | None = None,
                                    check_canceled: Callable[[], None] = lambda: None,
                                    progress: Callable[[dict[str, Any]], None] | None = None) -> dict[str, Any]:
        """Explicitly prepare a producer's original and newly visible atomic fields.

        Shares the planner's exact state enumeration, including same-day commit
        ceilings. Only missing canonical scopes use index_selections; known
        scopes preserve their original proof Sources without input-byte reads.
        Default scalar summaries prove consumed equality only. Optional
        key_anchors and completed_start/completed_end additionally prepare pure
        typed General asset additions absent from every original/current
        completed-prefix Raw anchor, including NULL withdrawals and extra
        natural keys. Native admission also requires a registered asset-local
        policy; this method alone changes no producer or completion. Known
        nonbaseline timing cannot regress, and every new atomic commit is
        checked, including commits at the original PIT day. Unsupported states
        remain actionable for the ordinary plan.
        """
        from .items.incremental import general_projection_states
        self._store.ensure_writable()
        if (self._read_context() is not None or self._boundary is not None
                or _boundary_record(self._store.data_meta_path) is not None
                or getattr(self._store._thread_state, "writer_connection", None) is not None):
            raise RuntimeError("General projection preparation requires an independent unfrozen writer")
        options = config or ExecutionOptions()
        normalized_fields = _selection_fields(fields)
        if normalized_fields is None:
            raise ValueError("General projection preparation requires explicit consumed fields")
        fields = tuple(normalized_fields)
        check_canceled()
        receipt = self._summary_source(original, max_bytes=options.max_buffer_bytes)
        request = receipt.requests[alias]
        effective = replace(request, view="snapshot")
        self._validate_selection_projection(receipt, alias, effective, fields=fields, values_only=True,
            causal_versions=False)
        if not _general_fields(receipt, alias, fields) or receipt.information_cutoff is None:
            raise ValueError("General preparation requires an original whole General Source with a fixed cutoff")
        end = _date_value(through)
        if end < receipt.information_cutoff:
            raise ValueError("General preparation through cannot precede the original information cutoff")
        if (not isinstance(key_anchors, tuple)
                or bool(key_anchors) != (completed_start is not None and completed_end is not None)
                or not key_anchors and (completed_start is not None or completed_end is not None)):
            raise ValueError("General key preparation requires key_anchors and both completed bounds")
        jobs, seen = [], set()
        work = {"stage": "general_projection_preparation", "alias": alias, "through": str(end),
            "fields": list(fields), "reused": 0, "prepared": 0, "unsupported": 0, "groups": 0,
            "batches": 0, "original_rows": 0, "hash_rows": 0, "owned_transport_bytes": 0, "new_sources": 0}
        states = []
        def report():
            check_canceled()
            if progress is not None:
                progress(dict(work))
        with self.read_context(receipt, config=options, check_canceled=check_canceled):
            with self._store.connect() as db:
                payload = self._capture(db, {alias: request}, cutoff=end,
                    compact_aliases={alias} if "full_commit_checks" in receipt.evidence[alias] else set(),
                    scoped_aliases=set(), retained_bounds_aliases={alias}
                        if receipt.evidence[alias].get("retained_batch_bounds") else set(),
                    max_buffer_bytes=options.max_buffer_bytes)
            charge = len(_json(payload).encode()) * 16
            if charge > options.max_buffer_bytes // 4:
                raise MemoryError("General preparation metadata exceeds max_buffer_bytes")
            for candidate, cutoff, ceiling, captured in general_projection_states(self, receipt, payload, alias=alias, end=end):
                for source, selected_cutoff, source_payload in ((receipt, min(cutoff, receipt.information_cutoff), None),
                        (candidate, cutoff, captured)):
                    check_canceled()
                    selected = _selected_general(source, alias, effective, selected_cutoff)
                    if selected is None:
                        empty_key = source.digest, selected_cutoff
                        if empty_key not in seen:
                            seen.add(empty_key)
                            work["unsupported"] += 1
                        continue
                    self._validate_selection_fields(source, alias, fields, request=effective, cutoff=selected_cutoff)
                    metadata: dict[str, Any] = {}
                    value = self._indexed_selection(source, alias, request=effective, cutoff=selected_cutoff,
                        values_only=True, summaries_only=True, fields=fields, metadata=metadata,exact_summaries=True)
                    locator = metadata["evidence_key"], metadata["request_key"]
                    if locator in seen:
                        continue
                    seen.add(locator)
                    state = {"max_commit": source.max_commit, "max_check_id": source.max_check_id,
                        "as_of": str(selected_cutoff), "original_commit_ceiling": ceiling if source_payload is not None else None,
                        **metadata, "status": "known" if value is not None else "unknown",
                        "content_token": None if value is None else value["identity"],
                        "proof_source": None if value is None else value.get("proof_source")}
                    states.append(state)
                    if value is not None:
                        work["reused"] += 1
                    else:
                        charge += len(_json(source_payload).encode()) * 16 if source_payload is not None else 2048
                        if charge > options.max_buffer_bytes // 4:
                            raise MemoryError("Aggregate General preparation states exceed max_buffer_bytes")
                        jobs.append((source, source_payload, selected_cutoff, state))
        report()
        for source, captured, cutoff, state in jobs:
            check_canceled()
            if captured is not None:
                with self._store.connect() as db:
                    db.execute("begin immediate")
                    self.get(receipt)
                    check_canceled()
                    source = self._register_capture(db, captured)
                work["new_sources"] += 1
            scope = InputSelectionScope(alias, as_of=cutoff, view="snapshot", fields=fields)
            group_work = []
            tokens = self.index_selections(source, [scope], config=options,
                check_canceled=check_canceled, progress=group_work.append)
            last = group_work[-1]
            work["groups"] += 1
            work["reused"] += last["skipped"]
            work["prepared"] += last["completed"] - last["skipped"]
            for name in ("batches", "original_rows", "hash_rows", "owned_transport_bytes"):
                work[name] += last[name]
            descriptor, = self.selection_descriptors(source, [scope], config=options, check_canceled=check_canceled)
            state.update(status=descriptor["status"], content_token=tokens[0], proof_source=descriptor["proof_source"])
            report()
        result = {"original_source": {"id": receipt.receipt_id, "digest": receipt.digest},
            "alias": alias, "fields": list(fields), "through": str(end), "states": states, "work": work}
        if key_anchors:
            assert completed_start is not None and completed_end is not None
            from .general_key_appends import prepare
            result["key_appends"] = prepare(self, receipt, alias, fields, anchors=key_anchors,
                first=_date_value(completed_start), last=_date_value(completed_end), end=end,
                options=options, check=check_canceled, progress=progress)
        return result

    def index_selections(self, receipt: FrozenInputReceipt | str, scopes: Sequence[InputSelectionScope], *,
                         config: ExecutionOptions | None = None, rebuild: bool = False,
                         check_canceled: Callable[[], None] = lambda: None,
                         progress: Callable[[dict[str, Any]], None] | None = None) -> tuple[str | None, ...]:
        """Explicitly build one alias's finite admitted group of exact summaries.

        At most 16 scopes are accepted. A caller owns grouping and cannot pass a
        generator/all-alias lifecycle. Existing complete scopes are reused; one
        group is atomically published. No row indexes or originals are changed.
        """
        from bagelquant_data import input_index
        self._store.ensure_writable()
        if (self._boundary is not None or _boundary_record(self._store.data_meta_path) is not None
                or self._read_context() is not None
                or getattr(self._store._thread_state, "writer_connection", None) is not None):
            raise RuntimeError("Scalar summary maintenance requires an unfrozen writer outside a read/write context")
        if not isinstance(scopes, Sequence) or isinstance(scopes, (str, bytes)) or not 1 <= len(scopes) <= 16:
            raise ValueError("Scalar bulk maintenance requires a finite group of 1..16 scopes")
        if any(not isinstance(scope, InputSelectionScope) for scope in scopes):
            raise TypeError("Scalar bulk scopes require InputSelectionScope")
        if len({scope.alias for scope in scopes}) != 1:
            raise ValueError("Scalar bulk maintenance admits only one alias per group")
        options = config or ExecutionOptions()
        budget = options.max_buffer_bytes
        # Charge every supplied frame before any coordinate sort/hash/decode.
        scope_charge = 2048 * len(scopes) + sum(8 * scope.coordinates.estimated_size()
            for scope in scopes if isinstance(scope.coordinates, pl.DataFrame))
        if scope_charge > budget // 4:
            raise MemoryError("Aggregate scalar scope/coordinate admission exceeds max_buffer_bytes")
        check_canceled()
        frozen = self._summary_source(receipt, max_bytes=budget)
        work = {"stage": "scalar_summary_group", "scopes": len(scopes), "skipped": 0,
            "batches": 0, "original_rows": 0, "hash_chunks": 0, "hash_rows": 0, "owned_transport_bytes": 0, "completed": 0}
        def report():
            check_canceled()
            if progress is not None:
                progress(dict(work))
        prepared, results, selectors = {}, [None] * len(scopes), {}
        with self.read_context(frozen, config=options, check_canceled=check_canceled):
            for position, scope in enumerate(scopes):
                check_canceled()
                request, cutoff = self._selection_scope(frozen, scope.alias, start=scope.start, end=scope.end,
                    as_of=scope.as_of, view=scope.view, strict=scope.strict)
                fields = _selection_fields(scope.fields)
                self._validate_selection_projection(frozen, scope.alias, request, fields=fields,
                    values_only=True, causal_versions=scope.causal_versions, coordinates=scope.coordinates,
                    coordinate_selector=scope.coordinate_selector)
                self._validate_selection_fields(frozen, scope.alias, fields, request=request, cutoff=cutoff)
                if fields is None:
                    _validate_raw_payload_projection(request)
                if (request.view == "versions") != scope.causal_versions:
                    raise ValueError("Value summaries require selected Item/by-date Raw snapshots/history; causal_versions requires an Item")
                if scope.coordinates is not None and scope.coordinate_selector is not None:
                    raise ValueError("Supply actual coordinates or a registered selector, never both")
                coords = scope.coordinates
                if scope.coordinate_selector is not None:
                    if not isinstance(scope.coordinate_selector, str) or not scope.coordinate_selector:
                        raise ValueError("coordinate_selector must be a registered nonempty identity")
                    if scope.coordinate_selector in selectors:
                        coords = selectors[scope.coordinate_selector]
                    else:
                        with self._store.connect() as db:
                            header = input_index.selector_header(db, scope.coordinate_selector, max_bytes=budget // 16)
                            if header is None:
                                raise RuntimeError("Coordinate selector is missing; explicit index_selection maintenance required")
                            scope_charge += 8 * max(header["stored_bytes"], header["raw_bytes"])
                            if scope_charge > budget // 4:
                                raise MemoryError("Aggregate selector decode admission exceeds max_buffer_bytes")
                            coords = input_index.read_selector(db, scope.coordinate_selector, max_bytes=budget // 4,
                                check_canceled=check_canceled)
                        selectors[scope.coordinate_selector] = coords
                if scope.coordinate_selector is not None:
                    assert coords is not None
                    # Each scope retains its own normalized/sorted frame. Shared
                    # decode does not make those per-scope buffers free.
                    scope_charge += 8 * coords.estimated_size()
                    if scope_charge > budget // 4:
                        raise MemoryError("Aggregate normalized selector admission exceeds max_buffer_bytes")
                coords = self._selection_coordinates(request, coords, fields=fields)
                if coords is not None and scope.causal_versions:
                    raise ValueError("causal_versions does not accept coordinates")
                cutoff = _scalar_complete_cutoff(frozen, scope.alias, request, cutoff, fields=fields)
                coordinate = None if coords is None else input_index.identity(coords)
                key = _selection_key(request, cutoff, values_only=True,
                    causal_versions=scope.causal_versions, coordinates=coordinate, fields=fields)
                scope_charge += len(key.encode()) * 16
                if scope_charge > budget // 4:
                    raise MemoryError("Aggregate scalar request metadata exceeds max_buffer_bytes")
                evidence_key = _scalar_selection_evidence(frozen, scope.alias, request, cutoff, fields=fields)
                locator = evidence_key, key
                if locator in prepared:
                    prepared[locator]["positions"].append(position)
                    continue
                with self._store.connect() as db:
                    saved = None if rebuild else input_index.projection(db, evidence_key, key, max_bytes=budget // 16)
                    selector_ready = coordinate is None or input_index.selector_header(db, coordinate, max_bytes=budget // 16) is not None
                if saved is not None and saved.get("kind") == input_index.VERSION and selector_ready:
                    if self._summary_source(saved["source_receipt_id"], max_bytes=budget).digest != saved["source_receipt_digest"]:
                        raise RuntimeError("Scalar selection summary's original source receipt changed")
                    results[position] = saved["identity"]
                    work["skipped"] += 1
                    continue
                batches = _selection_batches(frozen, scope.alias, request, cutoff, fields=fields)
                prepared[locator] = {"positions": [position], "request": request, "cutoff": cutoff,
                    "coordinates": coords, "causal": scope.causal_versions, "batches": batches,
                    "alias": scope.alias, "value": None, "fields": fields}
            self._build_selection_group(frozen, prepared, options, work, check_canceled, report)
        report()
        if prepared:
            with self._store.connect() as db:
                db.execute("begin immediate")
                check_canceled()
                if self._summary_source(frozen, max_bytes=budget).digest != frozen.digest:
                    raise RuntimeError("Scalar summary original source changed")
                for (evidence_key, key), scope in prepared.items():
                    if scope["value"] is None:
                        continue
                    if scope["coordinates"] is not None:
                        input_index.save_selector(db, scope["coordinates"], max_bytes=budget // 4, rebuild=rebuild)
                    input_index.save_projection(db, evidence_key, key, scope["value"], receipt_id=frozen.receipt_id,
                        receipt_digest=frozen.digest, alias=scope["alias"])
                    check_canceled()
        for scope in prepared.values():
            token = None if scope["value"] is None else scope["value"]["identity"]
            for position in scope["positions"]:
                results[position] = token
        work["completed"] = sum(value is not None for value in results)
        report()
        return tuple(results)

    def _build_selection_group(self, frozen, prepared, options, work, check_canceled, report):
        """One original acquisition feeds the existing canonical reducer."""
        from bagelquant_data import input_index
        budget = options.max_buffer_bytes
        union, windows = {}, []
        for scope in prepared.values():
            request = scope["request"]
            lower, upper = request.start, request.end
            if isinstance(request, RawInput):
                lower = max((_date_value(day) for day in (lower, request.observation_start) if day is not None), default=None)
                upper = min((_date_value(day) for day in (upper, request.observation_end) if day is not None), default=None)
            windows.append((lower, upper))
            for batch in scope["batches"]:
                union.setdefault((batch["commit_seq"], batch["partition_path"], batch["content_hash"]), batch)
        frames, retained = {}, 0
        alias = next(iter(prepared.values()))["alias"] if prepared else None
        item = alias is not None and isinstance(frozen.requests[alias], ItemInput)
        values = item and any(scope["fields"] is None or "value" in scope["fields"] for scope in prepared.values())
        scalar = item and any(scope["fields"] is None for scope in prepared.values())
        fields = None if all(scope["fields"] is None for scope in prepared.values()) else tuple(dict.fromkeys(
            name for scope in prepared.values() for name in (_original_projection_fields(scope["request"], scope["fields"]) or ())))
        for key, batch in union.items():
            check_canceled()
            remaining = budget // 4 - retained
            if remaining <= 0:
                raise MemoryError("Aggregate scalar group original projection exceeds max_buffer_bytes")
            # project_original serializes its callback output to owned IPC;
            # no mmap/Arrow-backed callback frame survives original transport.
            frame = input_index.project_original(self._store, batch, max_bytes=remaining,
                check_canceled=check_canceled, values=values, scalar=scalar,
                observation_windows=windows, work=work, fields=fields)
            retained += frame.estimated_size() * 12
            if retained > budget // 4:
                raise MemoryError("Aggregate scalar group retained projection exceeds max_buffer_bytes")
            frames[key] = frame
            work["batches"] += 1
            work["original_rows"] += int(batch["row_count"])
            report()
        for scope in prepared.values():
            check_canceled()
            request, cutoff, coords = scope["request"], scope["cutoff"], scope["coordinates"]
            if scope["fields"] is None and isinstance(request, ItemInput) and coords is None and not scope["batches"]:
                value = self._empty_scalar_selection(frozen, scope["alias"], request, scope["causal"])
            else:
                pieces = []
                lower, upper = request.start, request.end
                if isinstance(request, RawInput):
                    lower = max((_date_value(day) for day in (lower, request.observation_start) if day is not None), default=None)
                    upper = min((_date_value(day) for day in (upper, request.observation_end) if day is not None), default=None)
                for batch in scope["batches"]:
                    frame = frames[(batch["commit_seq"], batch["partition_path"], batch["content_hash"])]
                    if "source_time" in frame.columns:
                        if lower is not None:
                            frame = frame.filter(pl.col("source_time") >= _date_value(lower))
                        if upper is not None:
                            frame = frame.filter(pl.col("source_time") <= _date_value(upper))
                    pieces.append(frame)
                # Shared owned frames, scope clipping, witness joins, sorting and
                # canonical Arrow output reserve are admitted together.
                scope_bytes = sum(frame.estimated_size() for frame in pieces) + (0 if coords is None else coords.estimated_size())
                evidence = frozen.evidence[scope["alias"]]
                row_count = sum(frame.height for frame in pieces)
                witness_count = len(evidence["record_checks"]) + row_count * len(evidence.get("full_commit_checks", ()))
                witness_charge = witness_count * max(128, scope_bytes // max(1, row_count)) * 8
                if retained + scope_bytes * 12 + witness_charge > budget // 2:
                    raise MemoryError("Aggregate scalar canonical reduction exceeds max_buffer_bytes")
                value = self._reduce_selection(frozen, scope["alias"], request, cutoff, pieces, options,
                    values_only=True, causal_versions=scope["causal"], coordinates=coords, fields=scope["fields"])
                del pieces
            if value is None:
                raise MemoryError("Scalar canonical reduction exceeds max_buffer_bytes")
            scope["value"] = value
            check_canceled()
        frames.clear()

    def index_coordinates(self, receipt: FrozenInputReceipt | str, alias: str, *,
                          field: str = "time", selector: tuple[str, int] | None = None,
                          config: ExecutionOptions | None = None) -> None:
        """Explicitly record a complete General coordinate projection from frozen inputs.

        This maintenance operation consumes typed values. Ordinary impact plans
        only read its receipt-bound metadata and never invoke maintenance.
        """
        from bagelquant_data import input_index
        self._store.ensure_writable()
        context = self._read_context()
        if (context is not None or self._boundary is not None or _boundary_record(self._store.data_meta_path) is not None):
            raise RuntimeError("Coordinate maintenance requires an unfrozen writer outside a read context")
        options = config or ExecutionOptions()
        frozen = self._summary_source(receipt, max_bytes=options.max_buffer_bytes)
        with self.read_context(frozen, config=options):
            locator = self._coordinate_projection(frozen, alias, field, selector)
            if locator is None:
                raise ValueError("Coordinate maintenance requires a selected atomic snapshot with original schema authority")
            with self._store.connect() as db:
                dates, source = self._coordinate_record(frozen, alias, field, selector, db=db,
                    max_bytes=options.max_buffer_bytes // 4, legacy=False)
                if dates is not None and source is not None:
                    return  # Its complete canonical locator retains the original payload.
                dates = input_index.read_coordinates(db, frozen.digest, alias, field, selector,
                    max_bytes=options.max_buffer_bytes // 4)
            if dates is None:
                frame = self.read(frozen, alias, view="snapshot", fields=()).collect()
                if frame.estimated_size() > options.max_buffer_bytes // 2:
                    raise MemoryError("Coordinate projection exceeds max_buffer_bytes")
                dates = input_index.coordinate_dates(frame, field, selector)
            if len(_json(dates).encode()) * 16 > options.max_buffer_bytes // 4:
                raise MemoryError("Coordinate projection metadata exceeds max_buffer_bytes")
        with self._store.connect() as db:
            db.execute("begin immediate")
            if self.get(frozen).digest != frozen.digest:
                raise RuntimeError("Coordinate projection's original source receipt changed")
            if input_index.read_coordinates(db, frozen.digest, alias, field, selector,
                    max_bytes=options.max_buffer_bytes // 4) is None:
                input_index.save_coordinate_dates(db, frozen.digest, alias, field, selector, dates)
            input_index.save_coordinate_projection(db, *locator, dates, receipt_id=frozen.receipt_id,
                receipt_digest=frozen.digest, alias=alias)

    def latest(self, receipt: FrozenInputReceipt | str, alias: str, *, through: DateLike,
               config: ExecutionOptions | None = None,
               validate_rows: Callable[[pl.DataFrame], pl.DataFrame] | None = None,
               check_canceled: Callable[[], None] = lambda: None) -> pl.DataFrame:
        """Consume each asset's last scalar observation at the frozen cutoff.

        Observation ``through`` does not replace the information cutoff. Read
        serial monthly windows and retain one typed observation per asset, even
        across long gaps. This numerical read is for initialization or explicit
        checkpoint maintenance; prerequisite checks never call it. Captured
        bounds or checked original index headers keep reads physically bounded.
        A consumer may validate/filter each typed window before latest-row
        reduction; its admission semantics stay with that consumer.
        """
        frozen = self.get(receipt)
        request, cutoff = self._selection_scope(frozen, alias, start=None, end=through,
            as_of=None, view="snapshot", strict=None)
        if not isinstance(request, ItemInput) or cutoff is None:
            raise ValueError("Latest scalar observations require an Item input with a fixed information cutoff")
        context = self._read_context()
        options = config or (context.options if context is not None else ExecutionOptions())
        result = None
        with self.read_context(frozen, config=options, check_canceled=check_canceled):
            if not self.window_read_supported(frozen, alias):
                raise ValueError("INCREMENTAL_PROOF_REQUIRED: latest observation reads need captured or indexed original batch bounds")
            batches = _selection_batches(frozen, alias, request, cutoff)
            lower = request.start
            if lower is None:
                from bagelquant_data.storage.full_commit_checks import validate_seal
                sealed = {}
                for seal in frozen.evidence[alias].get("full_commit_checks", ()):
                    validate_seal(seal)
                    for bound in seal["batch_bounds"]:
                        sealed[(bound["commit_seq"], bound["partition_path"], bound["content_hash"])] = bound
                observations = []
                with self._store.connect() as db:
                    for batch in batches:
                        bound, _ = self._original_window_bounds(db, frozen.evidence[alias], batch)
                        if bound is not None:
                            observations.append(_date_value(bound["min_observation"]))
                        elif (seal_bound := sealed.get((batch["commit_seq"], batch["partition_path"], batch["content_hash"]))) is not None:
                            observations.append(_date_value(seal_bound["observation_min"]))
                lower = min(observations, default=_date_value(through))
            first, last = _date_value(lower), _date_value(through)
            while first <= last:
                check_canceled()
                following = date(first.year + (first.month == 12), first.month % 12 + 1, 1)
                end = min(last, following - timedelta(days=1))
                frame = self.read(frozen, alias, start=first, end=end, view="snapshot").collect().select("time", "asset_id", "value")
                if validate_rows is not None:
                    frame = validate_rows(frame)
                if (frame.estimated_size() + (0 if result is None else result.estimated_size())) * 4 > options.max_buffer_bytes:
                    raise MemoryError("Latest scalar observation state exceeds max_buffer_bytes")
                result = frame if result is None else concat_compatible_frames([result, frame])
                result = result.sort("asset_id", "time").unique("asset_id", keep="last", maintain_order=True)
                first = following
            if result is None:
                result = self.read(frozen, alias, start=last, end=last, view="snapshot").collect().select("time", "asset_id", "value")
        return result.sort("time", "asset_id")

    def index_record_lookups(self, *, config: ExecutionOptions | None = None,
                             check_canceled: Callable[[], None] = lambda: None) -> dict[str, Any]:
        """Explicitly install optional completion/Source SQLite lookup indexes.

        Scan metadata only, atomically; never read values, run an upstream
        object, alter receipts or certify currentness. Existing populated
        databases retain normal record-only reads until maintenance is selected.
        """
        from bagelquant_data.storage.record_lookups import install_record_lookups
        self._store.ensure_writable()
        if (self._boundary is not None or _boundary_record(self._store.data_meta_path) is not None
                or self._read_context() is not None
                or getattr(self._store._thread_state, "writer_connection", None) is not None):
            raise RuntimeError("Record-lookup maintenance requires an unfrozen writer outside a read/write context")
        with self._store.connect() as db:
            return install_record_lookups(db, config=config or ExecutionOptions(), check_canceled=check_canceled)

    def max_commit(self) -> int:
        value = int(self._store._rows("select coalesce(max(seq),0) as seq from version_commits where status='committed'")[0]["seq"])
        return min(value, self._boundary.max_commit) if self._boundary else value

    def max_check_id(self) -> int:
        """Return the active unchanged-content evidence boundary."""
        value = int(self._store._rows("select coalesce(max(id),0) as id from version_checks")[0]["id"])
        return min(value, self._boundary.max_check_id) if self._boundary and self._boundary.max_check_id is not None else value

    def freeze(
        self, requests: Mapping[str, DataInput], *,
        information_cutoff: DateLike | None = None, as_of: DateLike | None = None,
        max_commit: int | None = None,
        retain_batch_bounds: bool = False,
    ) -> FrozenInputReceipt:
        """Atomically capture definitions, schemas, cutoff and batch references."""
        self._store.ensure_writable()
        if information_cutoff is not None and as_of is not None:
            raise ValueError("Specify information_cutoff once")
        cutoff = information_cutoff if information_cutoff is not None else as_of
        cutoff = _date_value(cutoff) if cutoff is not None else None
        if self._boundary:
            max_commit = self._boundary.max_commit if max_commit is None else min(max_commit, self._boundary.max_commit)
            if self._boundary.as_of is not None:
                cutoff = self._boundary.as_of if cutoff is None else min(cutoff, self._boundary.as_of)
        if max_commit is not None and max_commit < 0:
            raise ValueError("max_commit must be non-negative")
        with self._store.connect() as db:
            db.execute("begin immediate")
            payload = self._capture(db, requests, cutoff=cutoff, max_commit=max_commit, persist_checks=True,
                retained_bounds_aliases=set(requests) if retain_batch_bounds else set())
            registered = self._register_capture(db, payload)
        return registered

    def _register_capture(self, db, payload):
        """Publish one captured metadata body through the sole Source writer."""
        references = {name: immutable_metadata.save(db, value) for name, value in payload["evidence"].items()}
        aliases = {name: {"source": value["source"], "dataset": value["dataset"],
            "definition_hash": value["definition_hash"], "schema_hash": value["schema_hash"],
            "update_type": value["definition"].get("update_type", "by_date"), "source_ref": references[name]["ref"],
            "row_count": sum(batch["row_count"] for batch in value["batches"]),
            "batch_count": len(value["batches"]),
            "min_observation": min((batch["min_observation"] for batch in value["batches"] if batch["min_observation"] is not None), default=None),
            "max_observation": max((batch["max_observation"] for batch in value["batches"] if batch["max_observation"] is not None), default=None),
            "state": self._registered_source_state(db, value["source"], value["dataset"])}
            for name, value in payload["evidence"].items()}
        for name, header in aliases.items():
            first, last = [header["min_observation"]], [header["max_observation"]]
            if header["source"] == "items":
                request = payload["requests"][name]
                start = str(request["start"]) if request.get("start") is not None else None
                end = str(request["end"]) if request.get("end") is not None else None
                empty = db.execute("SELECT min(b.start_date),max(b.end_date) FROM item_builds b JOIN frozen_inputs f ON f.receipt_id=b.frozen_receipt_id WHERE b.name=? AND b.status='success' AND b.definition_hash=? AND b.result_commit IS NULL AND b.input_commit<=? AND cast(json_extract(f.payload_json,'$.max_check_id') AS integer)<=? AND (? IS NULL OR b.end_date>=?) AND (? IS NULL OR b.start_date<=?)", (header["dataset"], header["definition_hash"], payload["max_commit"], payload["max_check_id"], start, start, end, end)).fetchone()
                first.append(empty[0])
                last.append(empty[1])
            header["required_start"] = min((day for day in first if day is not None), default=None)
            header["required_end"] = max((day for day in last if day is not None), default=None)
        identities = {name: hashlib.sha256(_json({"version": "frozen_request.v1",
            "request": payload["requests"][name], "information_cutoff": payload["information_cutoff"],
            "evidence": _identity_evidence(value)}).encode()).hexdigest()
            for name, value in payload["evidence"].items()}
        header = {**payload, "format": 9, "evidence": references, "aliases": aliases,
            "request_identities": identities}
        serialized = _json(header)
        if len(serialized.encode()) > db.getlimit(sqlite3.SQLITE_LIMIT_LENGTH):
            raise MemoryError("Frozen input evidence exceeds SQLite's single-value size limit")
        digest = hashlib.sha256(serialized.encode()).hexdigest()
        existing = db.execute("SELECT receipt_id FROM frozen_inputs WHERE digest=?", (digest,)).fetchone()
        if existing is not None:
            return self.get(existing[0])
        receipt_id = uuid4().hex
        db.execute("insert into frozen_inputs values(?,?,?,?)", (receipt_id, serialized, digest, datetime.now(UTC).isoformat()))
        return self._receipt(receipt_id, header, digest)

    @staticmethod
    def _registered_source_state(db, source, dataset):
        """Small published state bound to a Source, independent of global counters."""
        row = db.execute("SELECT spec_hash,publication_revision,active FROM datasets WHERE source=? AND name=?", (source, dataset)).fetchone()
        if row is None:
            return None
        definition = row["spec_hash"]
        if source == "items":
            item = db.execute("SELECT spec_hash,active FROM item_definitions WHERE name=?", (dataset,)).fetchone()
            if item is None or not item["active"]:
                return None
            definition = item["spec_hash"]
        return {"definition_hash": definition, "publication_revision": row["publication_revision"],
            "active": bool(row["active"]),
            "commit": db.execute("SELECT coalesce(max(seq),0) FROM version_commits WHERE source=? AND dataset=? AND status='committed'", (source, dataset)).fetchone()[0],
            "check": db.execute("SELECT coalesce(max(id),0) FROM version_checks WHERE source=? AND dataset=?", (source, dataset)).fetchone()[0]}

    def _unchanged_alias_state(self, db, frozen, header):
        """Unchanged content headers do not establish producer freshness."""
        if frozen.receipt_id == "current" or self._boundary is not None:
            return False
        state = header["state"]
        return (state is not None and state["active"] and state["commit"] <= frozen.max_commit
            and state["check"] <= frozen.max_check_id
            and state == self._registered_source_state(db, header["source"], header["dataset"]))

    def _completed_source_current(self, db, frozen, alias_names: Sequence[str] | None = None,
                                  alias_headers: Mapping[str, Mapping[str, Any]] | None = None):
        """Admit unchanged Source headers under existing whole-range authorities.

        Raw has no producer dependency. Items additionally require current
        completed flags through the live declaration closure; unknown/partial
        authorities take the existing conservative record comparison path.
        """
        if frozen.receipt_id == "current" or self._boundary is not None:
            return None
        aliases = self.describe(frozen)["aliases"] if alias_headers is None else alias_headers
        checked, visiting = set(), set()
        context = self._read_context()
        memo = self._metadata_memo() if context is not None and context.database is db else None

        def completed(name):
            if name in visiting:
                raise RuntimeError("DataItem declaration dependency cycle")
            if name in checked:
                return True
            memo_key = ("completed_node", name, frozen.information_cutoff)
            if memo is not None and memo.enabled and memo.get(memo_key) is not None:
                checked.add(name)
                return True
            visiting.add(name)
            row = db.execute("SELECT f.*,d.update_incomplete,d.publication_revision AS active_revision,i.spec_hash AS active_definition,i.active FROM node_completions f JOIN datasets d ON d.source='items' AND d.name=? JOIN item_definitions i ON i.name=d.name WHERE f.node_key=?", (name, "items/" + name)).fetchone()
            if (row is None or not row["active"] or row["update_incomplete"]
                    or row["definition_hash"] != row["active_definition"]
                    or row["publication_revision"] != row["active_revision"]
                    or row["dependency_state_digest"] != self._store.dependency_state(db, "items", name)):
                return False
            from .items.completion import pending_dates
            try:
                build_id = int(row["result_ref"])
            except (TypeError, ValueError):
                return False
            if any(frozen.information_cutoff is None or _date_value(available) <= frozen.information_cutoff
                    for _, available in pending_dates(db, build_id)):
                return False
            for parent in db.execute("SELECT kind,source,dataset FROM item_dependencies WHERE item_name=?", (name,)):
                if parent["kind"] == "item" and not completed(parent["dataset"]):
                    return False
                if parent["kind"] != "item" and db.execute("SELECT 1 FROM datasets WHERE source=? AND name=? AND active=1", (parent["source"], parent["dataset"])).fetchone() is None:
                    return False
            visiting.remove(name)
            checked.add(name)
            if memo is not None and memo.enabled:
                memo.put(memo_key, True, 256 + 8 * len(name.encode()))
            return True

        selected = frozen.requests if alias_names is None else alias_names
        for alias in selected:
            request = frozen.requests[alias]
            header = aliases[alias]
            if not self._unchanged_alias_state(db, frozen, header):
                return None
            if isinstance(request, ItemInput):
                if frozen.information_cutoff is None or not completed(request.name):
                    return None
                flag = db.execute("SELECT completed_start,completed_end,checked_target FROM node_completions WHERE node_key=?", ("items/" + request.name,)).fetchone()
                lower = str(request.start) if request.start is not None else header["required_start"]
                upper = str(request.end) if request.end is not None else header["required_end"]
                if (flag["checked_target"] < str(frozen.information_cutoff)
                        or lower is None or upper is None or flag["completed_start"] is None
                        or flag["completed_end"] is None or flag["completed_start"] > lower
                        or flag["completed_end"] < upper):
                    return None
        return True

    def _capture_batch_headers(self, db, source, dataset, boundary, *, max_buffer_bytes=None):
        """Reuse immutable headers only inside the same admitted SQLite view."""
        context = self._read_context()
        if context is not None and (context.database is not db
                or max_buffer_bytes not in (None, context.options.max_buffer_bytes)):
            context = None
        if context is not None and context.check_canceled is not None:
            context.check_canceled()
        # A guard can expose a writable transaction. Its own writes invalidate
        # headers even if the highest committed sequence stays unchanged.
        key = (db, db.total_changes, source, dataset, boundary) if context is not None and context.memo_available else None
        if context is not None and key is not None and key in context.capture_batches:
            value = context.capture_batches.pop(key)
            context.capture_batches[key] = value
            return value[0]
        rows = tuple(db.execute(
            "select b.commit_seq,b.partition_path,b.content_hash,b.row_count,c.mode,c.spec_hash,c.pit_date,c.input_receipt_id,c.baseline,"
            "b.min_available,b.max_available,b.min_observation,b.max_observation,b.generation_path,b.field_changes_json,b.key_identity "
            "from version_segments b join version_commits c on c.seq=b.commit_seq "
            "where c.source=? and c.dataset=? and c.status='committed' and c.seq<=? order by b.commit_seq,b.partition_path",
            (source, dataset, boundary)))
        if context is not None and context.memo_available and key is not None:
            reserve = context.options.max_buffer_bytes // 16
            # Include retained Python cells/row tuples and conservative UTF-8
            # expansion. This optional cache never increases caller admission.
            charge = 4096 + 4 * (len(source.encode()) + len(dataset.encode()))
            for index, row in enumerate(rows):
                if index % 128 == 0 and context.check_canceled is not None:
                    context.check_canceled()
                charge += 256 + sum(sys.getsizeof(cell) +
                    (4 * len(cell.encode()) if isinstance(cell, str) else 0) for cell in row)
                if charge > reserve:
                    break
            if charge <= reserve:
                retained = sum(value[1] for value in context.capture_batches.values())
                while context.capture_batches and (retained + charge > reserve
                        or len(context.capture_batches) >= 128):
                    _, removed = context.capture_batches.pop(next(iter(context.capture_batches)))
                    retained -= removed
                context.capture_batches[key] = (rows, charge)
        return rows

    def _capture_parent_digest(self, db, parent_id, *, max_buffer_bytes=None):
        """Check an original metadata checksum once per admitted fixed view."""
        context = self._read_context()
        if context is not None and (context.database is not db
                or max_buffer_bytes not in (None, context.options.max_buffer_bytes)):
            context = None
        if context is not None and context.check_canceled is not None:
            context.check_canceled()
        key = (db, db.total_changes, parent_id) if context is not None and context.memo_available else None
        if context is not None and key is not None and key in context.capture_parents:
            value = context.capture_parents.pop(key)
            context.capture_parents[key] = value
            return value[0]
        parent = db.execute("select payload_json,digest from frozen_inputs where receipt_id=?", (parent_id,)).fetchone()
        if parent is None:
            raise RuntimeError("Committed input receipt evidence is missing")
        if hashlib.sha256(str(parent["payload_json"]).encode()).hexdigest() != parent["digest"]:
            raise RuntimeError("Committed input receipt checksum mismatch")
        digest = str(parent["digest"])
        if context is not None and context.memo_available and key is not None:
            charge = 1024 + 4 * (len(parent_id.encode()) + len(digest.encode()))
            reserve = context.options.max_buffer_bytes // 64
            if charge <= reserve:
                retained = sum(value[1] for value in context.capture_parents.values())
                while context.capture_parents and (retained + charge > reserve
                        or len(context.capture_parents) >= 512):
                    _, removed = context.capture_parents.pop(next(iter(context.capture_parents)))
                    retained -= removed
                context.capture_parents[key] = (digest, charge)
        return digest

    def _capture_record_matches(self, db, source, dataset, candidates, commits, *, max_buffer_bytes=None):
        retained = set(commits)
        return frozenset(row["check_id"] for row in self._store.visibility_headers(db,source,dataset)
            if row["check_id"] in candidates and retained.intersection(row["version_commits"]))

    def _capture(self, db: sqlite3.Connection, requests: Mapping[str, DataInput], *,
                 cutoff: date | None, max_commit: int | None = None, persist_checks: bool = False,
                 compact_aliases: set[str] | None = None,
                 scoped_aliases: set[str] | None = None,
                 retained_bounds_aliases: set[str] | None = None,
                 max_buffer_bytes: int | None = None) -> dict[str, Any]:
        """Capture definitions and immutable evidence in the caller's transaction."""
        payload: dict[str, Any] = {"requests": {}, "evidence": {}, "information_cutoff": None if cutoff is None else cutoff.isoformat()}
        latest = int(db.execute("select coalesce(max(seq),0) from version_commits where status='committed'").fetchone()[0])
        boundary = latest if max_commit is None else min(max_commit, latest)
        payload["max_commit"] = boundary
        max_check_id = int(db.execute("select coalesce(max(id),0) from version_checks").fetchone()[0])
        if self._boundary and self._boundary.max_check_id is not None:
            max_check_id = min(max_check_id, self._boundary.max_check_id)
        payload["max_check_id"] = max_check_id
        inline_bytes = 0
        for alias, request in sorted(requests.items()):
            if not alias or not isinstance(request, (RawInput, ItemInput)):
                raise TypeError("Frozen requests require non-empty aliases and RawInput/ItemInput definitions")
            source, dataset = (request.source, request.dataset) if isinstance(request, RawInput) else ("items", request.name)
            definition = db.execute("select spec_json,spec_hash,active from datasets where source=? and name=?", (source, dataset)).fetchone()
            if definition is None or not definition["active"]:
                raise KeyError(f"Unknown active input: {source}/{dataset}")
            item_definition = db.execute("select spec_json,spec_hash,active from item_definitions where name=?", (dataset,)).fetchone() if isinstance(request, ItemInput) else None
            if isinstance(request, ItemInput) and (item_definition is None or not item_definition["active"]):
                raise KeyError(f"Unknown active input: {dataset}")
            schema = db.execute("select s.schema_ipc,d.schema_hash from dataset_schemas d join immutable_schemas s on s.digest=d.schema_ref where source=? and dataset=?", (source, dataset)).fetchone()
            batches = self._capture_batch_headers(db, source, dataset, boundary,
                max_buffer_bytes=max_buffer_bytes)
            general = json.loads(definition["spec_json"])["update_type"] == "general"
            scoped = (scoped_aliases is None or alias in scoped_aliases) and not general and (
                request.start is not None or request.end is not None
                or isinstance(request, RawInput) and (request.observation_start is not None or request.observation_end is not None))
            audit_checks = db.execute(
                "select v.*,c.baseline as content_baseline from version_checks v left join version_commits c on c.seq=v.visible_commit "
                "where v.source=? and v.dataset=? and v.id<=? and (v.visible_commit is null or v.visible_commit<=?) "
                "and (c.seq is null or c.status='committed') order by v.id",
                (source, dataset, max_check_id, boundary),
            ).fetchall()
            selected_batches = batches
            if scoped:
                starts = [request.start]
                ends = [request.end]
                if isinstance(request, RawInput):
                    starts.append(request.observation_start)
                    ends.append(request.observation_end)
                lower = max((_date_value(value) for value in starts if value is not None), default=None)
                upper = min((_date_value(value) for value in ends if value is not None), default=None)
                selected_batches = [row for row in batches if
                    (lower is None or row["max_observation"] is None or _date_value(row["max_observation"]) >= lower)
                    and (upper is None or row["min_observation"] is None or _date_value(row["min_observation"]) <= upper)
                    and (cutoff is None or row["min_available"] is None or _date_value(row["min_available"]) <= cutoff)]
                retained_commits = {int(row["commit_seq"]) for row in selected_batches}
                # Correlate witnesses from the small captured check headers in
                # bounded batches. Re-preparing the same large commit set for
                # each individual check dominates metadata-only currentness.
                candidate_ids = [int(check["id"]) for check in audit_checks
                    if check["row_count"] and check["visible_commit"] is not None
                    and check["visible_commit"] not in retained_commits]
                matching_ids = set()
                for offset in range(0, len(candidate_ids), 512):
                    matching_ids.update(self._capture_record_matches(db, source, dataset,
                        candidate_ids[offset:offset + 512], retained_commits,
                        max_buffer_bytes=max_buffer_bytes))
                def relevant_check(check: sqlite3.Row) -> bool:
                    if check["row_count"] == 0:
                        scopes = [query["item_range"] for query in json.loads(check["request_json"]) if "item_range" in query]
                        return not scopes or any(
                            (lower is None or _date_value(scope["end"]) >= lower)
                            and (upper is None or _date_value(scope["start"]) <= upper) for scope in scopes)
                    if check["visible_commit"] is None or check["visible_commit"] in retained_commits:
                        return True
                    return check["id"] in matching_ids
                audit_checks = [check for check in audit_checks if relevant_check(check)]
            seals = []
            sealed_ids = {seal["binding"]["check"]["id"] for seal in seals}
            if scoped:
                proof_refs = {(row["commit_seq"], row["partition_path"], row["content_hash"])
                              for seal in seals for row in seal["binding"]["batches"]}
                selected_refs = {(row["commit_seq"], row["partition_path"], row["content_hash"]) for row in selected_batches} | proof_refs
                batches = [row for row in batches if (row["commit_seq"], row["partition_path"], row["content_hash"]) in selected_refs]

            retained_bounds = not general and alias in (retained_bounds_aliases or set())
            retained_bounds = not general
            record_checks = [row for row in self._store.visibility_headers(db,source,dataset,check_bound=max_check_id)
                if all(commit <= boundary for commit in row["version_commits"])
                and (not scoped or retained_commits.intersection(row["version_commits"]))]
            inline_bytes += len(_json(record_checks).encode())
            # Keep every audit event, but only unknown original timing can
            # gain new causal visibility from a same-content attestation.
            witnessed = {value["check_id"] for value in record_checks} | sealed_ids
            checks = [value for value in audit_checks if not value["baseline"] and (
                value["content_baseline"] if general else value["id"] in witnessed
            )]
            general_snapshots = []
            if general:
                # A full empty snapshot is still a content version. Keep
                # its identity independently of data rows so replay cannot
                # resurrect the preceding nonempty snapshot.
                general_snapshots = [dict(value) for value in db.execute(
                    "select c.seq,c.pit_date,c.ingested_at,c.mode,c.spec_hash,c.baseline,"
                    "coalesce(sum(b.row_count),0) as row_count,"
                    "(select hex(s.schema_ipc) from version_segments v join immutable_schemas s on s.digest=v.schema_ref where v.commit_seq=c.seq "
                    "order by v.partition_path limit 1) as schema_ipc "
                    "from version_commits c left join version_segments b on b.commit_seq=c.seq "
                    "where c.source=? and c.dataset=? and c.status='committed' and c.seq<=? "
                    "group by c.seq order by c.seq", (source, dataset, boundary),
                )]
            parent_receipts = {}
            empty_item_build = None
            if isinstance(request, ItemInput):
                assert item_definition is not None
                empty_item_build = db.execute(
                    "select definition_hash,dependency_digest,start_date,end_date,input_commit,"
                    "result_commit,frozen_receipt_id from item_builds b "
                    "join frozen_inputs f on f.receipt_id=b.frozen_receipt_id where name=? and status='success' "
                    "and definition_hash=? and input_commit<=? and (result_commit is null or result_commit<=?) "
                    "and cast(json_extract(f.payload_json,'$.max_check_id') as integer)<=? "
                    "and (? is null or start_date<=?) and (? is null or end_date>=?) "
                    "order by b.id desc limit 1", (dataset, item_definition["spec_hash"], boundary, boundary, max_check_id,
                    None if request.start is None else _date_value(request.start).isoformat(),
                    None if request.start is None else _date_value(request.start).isoformat(),
                    None if request.end is None else _date_value(request.end).isoformat(),
                    None if request.end is None else _date_value(request.end).isoformat()),
                ).fetchone()
            parent_ids = {
                str(value["input_receipt_id"]) for value in [*batches, *audit_checks]
                if value["input_receipt_id"] is not None
            }
            if empty_item_build is not None and empty_item_build["frozen_receipt_id"] is not None:
                parent_ids.add(str(empty_item_build["frozen_receipt_id"]))
            for parent_id in sorted(parent_ids):
                parent_receipts[parent_id] = self._capture_parent_digest(db, parent_id,
                    max_buffer_bytes=max_buffer_bytes)
            payload["requests"][alias] = input_payload(request)
            payload["evidence"][alias] = {
                "source": source, "dataset": dataset,
                "definition": json.loads(item_definition["spec_json"] if item_definition else definition["spec_json"]),
                "definition_hash": item_definition["spec_hash"] if item_definition else definition["spec_hash"],
                "schema_hash": schema["schema_hash"] if schema else None,
                "schema_ipc": bytes(schema["schema_ipc"]).hex() if schema else None,
                "batches": [dict(value) for value in batches],
                "checks": [dict(value) for value in checks],
                "audit_checks": [dict(value) for value in audit_checks],
                "empty_checks": [dict(value) for value in audit_checks if value["row_count"] == 0],
                "record_checks": [dict(value) for value in record_checks],
                "general_snapshots": general_snapshots,
                "parent_receipts": parent_receipts,
                "empty_item_build": None if empty_item_build is None else dict(empty_item_build),
            }
            if seals:
                payload["evidence"][alias]["full_commit_checks"] = seals
            if scoped:
                payload["evidence"][alias]["scoped_batches"] = True
            elif retained_bounds:
                payload["evidence"][alias]["retained_batch_bounds"] = True
        semantic_evidence = {}
        for alias, evidence in payload["evidence"].items():
            semantic = {key: value for key, value in _identity_evidence(evidence).items()
                        if key not in {"audit_checks", "parent_receipts", "empty_checks"}}
            if evidence.get("scoped_batches") and isinstance(requests[alias], ItemInput) and not evidence["batches"]:
                # A typed, proven empty Item window gains no values when a
                # different month first establishes the physical row schema.
                # Keep its captured schema for replay, and its typed declaration
                # and empty dependency proof for semantic currentness.
                semantic["schema_hash"] = None
                semantic["schema_ipc"] = None
            unknown_scopes = {}
            for event in evidence["empty_checks"]:
                if event["baseline"]:
                    unknown_scopes[event["request_json"]] = event["id"]
            semantic["empty_baseline_scopes"] = sorted(unknown_scopes.items())
            epoch = max(unknown_scopes.values(), default=0)
            range_proofs = {}
            for event in evidence["empty_checks"]:
                if event["baseline"] or event["id"] <= epoch:
                    continue
                for query in json.loads(event["request_json"]):
                    scope = query.get("item_range")
                    if scope is None:
                        continue
                    parent_digest = None
                    if event["input_receipt_id"] is not None:
                        row = db.execute("select payload_json from frozen_inputs where receipt_id=?", (event["input_receipt_id"],)).fetchone()
                        parent_digest = json.loads(row["payload_json"])["dependency_digest"]
                    key = _json({"scope": scope, "dependency_digest": parent_digest})
                    range_proofs[key] = min(range_proofs.get(key, event["pit_date"]), event["pit_date"])
            semantic["empty_range_proofs"] = sorted(range_proofs.items())
            for collection in ("batches", "checks"):
                semantic[collection] = [{key: value for key, value in event.items()
                                         if key != "input_receipt_id"}
                                        for event in semantic[collection]]
            if semantic["empty_item_build"] is not None:
                semantic["empty_item_build"] = {key: value for key, value in semantic["empty_item_build"].items()
                                                 if key not in {"frozen_receipt_id", "input_commit"}}
            semantic_evidence[alias] = semantic
        semantic_payload = {key: value for key, value in payload.items()
                            if key not in {"max_commit", "max_check_id", "evidence"}}
        semantic_payload["evidence"] = semantic_evidence
        payload["dependency_digest"] = hashlib.sha256(_json(semantic_payload).encode()).hexdigest()
        return payload

    def is_current(self, receipt: FrozenInputReceipt | str | Sequence[FrozenInputReceipt | str], *,
                   config: ExecutionOptions | None = None,
                   progress: Callable[[dict[str, Any]], None] | None = None) -> bool:
        """Compare current semantic evidence without creating a new receipt.

        The original request windows and information cutoff are retained.
        Revisions conservatively invalidate; pure verified rechecks do not.
        Item inputs follow the newest retained dependency proof at the cutoff,
        recursively checking upstream declarations and evidence in one read view.
        Archived inputs and changed declarations return false. Missing or corrupt
        frozen receipts fail explicitly; verify() separately checks retained bytes.
        A nonempty finite sequence checks every original root in the same read
        view. Record relationships and completed currentness are reused only
        within this admitted operation and the same database write generation.
        """
        single = isinstance(receipt, (FrozenInputReceipt, str))
        context = self._read_context()
        if context is None and getattr(self._store._thread_state, "writer_connection", None) is None:
            supplied = receipt if single else tuple(receipt)
            with self.read_context(supplied, config=config):
                return self._is_current(supplied, config=config, progress=progress)
        return self._is_current(receipt, config=config, progress=progress)

    def _is_current(self, receipt, *, config, progress):
        """Execute the record comparison once in the caller's actual view."""
        single = isinstance(receipt, (FrozenInputReceipt, str))
        context = self._read_context()
        roots = [self.get(receipt)] if single else [self.get(value) for value in receipt]
        if not roots:
            raise ValueError("Currentness requires at least one frozen input receipt")
        checked: dict[str, bool] = {}
        options = config or (context.options if context is not None else ExecutionOptions())
        memo_keys: dict[str, tuple[str, str, Path, InputReadBoundary | None, sqlite3.Connection, int, int]] = {}
        memo_path = self._store.data_meta_path.resolve()
        visiting: set[str] = set()
        with self._store.connect() as db:
            if not db.in_transaction:
                db.execute("begin")
            if context is not None and (context.database is not db
                    or context.options.max_buffer_bytes != options.max_buffer_bytes):
                context = None

            def current(value: FrozenInputReceipt) -> bool:
                if context is not None and context.check_canceled is not None:
                    context.check_canceled()
                if value.receipt_id in visiting:
                    raise RuntimeError("Frozen input receipt dependency cycle")
                if value.receipt_id in checked:
                    return checked[value.receipt_id]
                memo_key = (value.receipt_id, value.digest, memo_path, self._boundary,
                    db, db.total_changes, options.max_buffer_bytes)
                memo_keys[value.receipt_id] = memo_key
                if context is not None and context.memo_available and memo_key in context.currentness:
                    checked[value.receipt_id] = context.currentness[memo_key]
                    return checked[value.receipt_id]
                visiting.add(value.receipt_id)
                alias_headers = self.describe(value)["aliases"]
                if self._completed_source_current(db, value, None, alias_headers) is True:
                    checked[value.receipt_id] = True
                    visiting.remove(value.receipt_id)
                    if progress is not None:
                        for alias, request in value.requests.items():
                            progress({"phase": "currentness_completed_source", "receipt_id": value.receipt_id,
                                "alias": alias, "current": True, "completion_metadata_equal": True,
                                "item_versions_metadata_equal": isinstance(request, ItemInput)})
                    return True
                # Check size/count alone cannot establish changed selection:
                # even a large new attestation can be outside this fixed cutoff.
                # Reuse registered proofs and admit any inline representation
                # before the actual cutoff-aware metadata comparison.
                completed_aliases = set()
                def alias_current(alias):
                    if self._completed_source_current(db, value, (alias,), alias_headers) is True:
                        completed_aliases.add(alias)
                        return True
                    if self._unchanged_alias_state(db, value, alias_headers[alias]):
                        # The captured content is identical, even when a monthly
                        # producer cannot certify this Source's wider range or
                        # later cutoff. Its original creator/pending/completion
                        # checks below still establish producer freshness.
                        if progress is not None:
                            progress({"phase": "currentness_selection", "receipt_id": value.receipt_id,
                                "alias": alias, "current": True, "registered_state_equal": True})
                        return True
                    original_alias = replace(value, requests={alias: value.requests[alias]},
                        evidence={alias: value.evidence[alias]})
                    try:
                        captured = replace(self._capture_current_receipt(original_alias,
                            cutoff=value.information_cutoff, config=options), receipt_id="current")
                    except KeyError:
                        return False
                    if len(value.requests) == 1 and captured.dependency_digest == value.dependency_digest:
                        return True
                    else:
                        general = self._currentness_selection(value, alias, general=True)
                        general_equal = general is not None and general == self._currentness_selection(captured, alias, general=True)
                        content = self._currentness_selection(value, alias)
                        content_equal = content is not None and content == self._currentness_selection(captured, alias)
                        raw_equal = content_equal and isinstance(value.requests[alias], RawInput)
                        item_equal = content_equal and isinstance(value.requests[alias], ItemInput)
                        record_equal = (not general_equal and not content_equal
                            and self._selection_equivalent(value, captured, alias, records_only=True) is True)
                        metadata_equal = general_equal or content_equal or record_equal
                        original = latest = None
                        if not metadata_equal:
                            original = self._indexed_selection(value, alias)
                            latest = self._indexed_selection(captured, alias) if original is not None else None
                        same = metadata_equal or (original is not None and latest is not None
                            and original["identity"] == latest["identity"])
                        if progress is not None:
                            before, after = value.evidence[alias], captured.evidence[alias]
                            progress({"phase": "currentness_selection", "receipt_id": value.receipt_id,
                                "alias": alias, "current": same, "general_metadata_equal": general_equal,
                                "raw_metadata_equal": raw_equal,
                                "item_versions_metadata_equal": item_equal,
                                "selection_records_equal": record_equal,
                                "original_projection_known": original is not None,
                                "current_projection_known": latest is not None,
                                "original_schema": before["schema_hash"], "current_schema": after["schema_hash"],
                                "definition_equal": before["definition_hash"] == after["definition_hash"]})
                        return same
                result = all(alias_current(alias) for alias in value.requests)
                if result:
                    for alias, request in value.requests.items():
                        if alias in completed_aliases or not isinstance(request, ItemInput):
                            continue
                        evidence = value.evidence[alias]
                        if self._has_pending_item_outputs(value, alias):
                            if progress is not None:
                                progress({"phase": "currentness_pending_outputs", "receipt_id": value.receipt_id,
                                    "alias": alias, "current": False})
                            result = False
                            break
                        if self._completed_source_current(db, value, (alias,), alias_headers) is True:
                            # A sibling may need conservative provenance checks
                            # while this alias has exact current whole-range
                            # authority, including composed empty publications.
                            # Reuse the same frozen-state/coverage fences without
                            # bypassing pending outputs or validating siblings.
                            continue
                        proof = evidence["empty_item_build"]
                        covers = proof is not None and self._completion_covers_request(value, alias, proof)
                        empty_parents = self._uncovered_empty_parents(value, alias, proof if covers else None)
                        parent_ids = set(evidence["parent_receipts"]) | empty_parents
                        if (not evidence["definition"]["inputs"] and not parent_ids
                                and _has_visible_item_batch(value, alias)):
                            # Exact alias equality and the pending-output veto
                            # already hold. A visible terminal Item has no
                            # upstream creator needing a numerical selection
                            # index. Empty/unknown/strict/future cases retain
                            # ordinary completion and selection requirements.
                            continue
                        # Capture includes every batch/check parent and the
                        # covering build proof. If all are current, every
                        # possible selected row or empty build is current.
                        # Any stale parent still needs exact cutoff selection;
                        # an unselected historical parent may be stale safely.
                        if proof is not None and proof["frozen_receipt_id"] in parent_ids:
                            if (covers
                                    and all(current(self.get(parent)) for parent in empty_parents)
                                    and all(batch["commit_seq"] <= (proof["result_commit"] or 0)
                                            for batch in evidence["batches"])
                                    and all(check["visible_commit"] is not None
                                            and check["visible_commit"] <= (proof["result_commit"] or 0)
                                            for check in evidence["audit_checks"])
                                    and current(self.get(proof["frozen_receipt_id"]))):
                                # A successful build covering the complete
                                # requested range certifies planned repairs and
                                # historical reuse. Creator receipts remain
                                # immutable provenance for reused older rows.
                                continue
                            if not covers and _has_visible_item_batch(value, alias):
                                overridden = self._completed_parent_overrides(value, alias, proof)
                                if (overridden and current(self.get(proof["frozen_receipt_id"]))
                                        and all(current(self.get(parent)) for parent in parent_ids - overridden)):
                                    # A native completion may recertify just the
                                    # latest month of a much wider Item input.
                                    # Only wholly covered creator occurrences
                                    # can borrow its current-source authority.
                                    continue
                        if (parent_ids and (proof is not None and proof["frozen_receipt_id"] in parent_ids
                                or _has_visible_item_batch(value, alias))
                                and all(current(self.get(parent_id)) for parent_id in parent_ids)):
                            # Monthly/discrete completions need not have one
                            # certificate spanning the consumer's whole input.
                            # All captured creators and empty-build sources
                            # being current proves creator freshness. A visible
                            # nonempty batch (or original completion) separately
                            # preserves empty-selection completion requirements.
                            continue
                        if not covers and parent_ids and _has_visible_item_batch(value, alias):
                            replacements = self._native_parent_replacements(value, alias, db=db, options=options)
                            if replacements and all(
                                    all(current(self.get(source)) for source in replacements[parent])
                                    if parent in replacements else current(self.get(parent)) for parent in parent_ids):
                                # Separate native ranges can cover distinct
                                # captured creators and empty checks.
                                # A captured partial build does not prevent
                                # composing other native ranges. This remains
                                # freshness of those exact occurrences, never
                                # an invented whole-range completion or source
                                # equivalence for the original receipt.
                                continue
                        selected = self._selected_item_parents(value, alias, options)
                        if selected is None:
                            if progress is not None:
                                progress({"phase": "currentness_selection_unknown", "receipt_id": value.receipt_id,
                                    "alias": alias, "current": False})
                            result = False
                            break
                        selected_rows, parents = selected
                        parents.update(empty_parents)
                        if not selected_rows:
                            if evidence["empty_item_build"] is not None:
                                parent_id = evidence["empty_item_build"]["frozen_receipt_id"]
                                if parent_id is not None:
                                    parents.add(str(parent_id))
                            elif evidence["definition"]["inputs"]:
                                result = False
                        if not all(current(self.get(parent_id)) for parent_id in parents):
                            result = False
                        if not result:
                            break
                visiting.remove(value.receipt_id)
                checked[value.receipt_id] = result
                if progress is not None:
                    progress({"phase": "currentness", "receipt_id": value.receipt_id, "current": result})
                return result

            # Visit every root even if another is stale; missing/corrupt/cyclic
            # evidence must not be hidden by a short-circuiting aggregate.
            results = [current(value) for value in roots]
            if context is not None and context.memo_available:
                # Publish only after every original root completed successfully.
                # Bound operation-local bookkeeping; no frames/proofs persist.
                for receipt_id, result in checked.items():
                    key = memo_keys[receipt_id]
                    if key not in context.currentness and len(context.currentness) >= 4096:
                        context.currentness.pop(next(iter(context.currentness)))
                    context.currentness[key] = result
            return all(results)

    def _selected_item_parents(self, frozen: FrozenInputReceipt, alias: str,
                               options: ExecutionOptions) -> tuple[bool,set[str]] | None:
        """Resolve complete equal-key segment supersession from compact metadata."""
        evidence = frozen.evidence[alias]
        request = frozen.requests[alias]
        cutoff = frozen.information_cutoff
        candidates = _selection_batches(frozen,alias,request,cutoff)
        latest: dict[str,Mapping[str,Any]] = {}
        retained = []
        eligible_events = [row for row in evidence["record_checks"] if row.get("event")
            and (cutoff is None or _date_value(row["pit_date"]) <= cutoff)]
        witnessed_commits = {seq for event in eligible_events for seq in event["version_commits"]}
        for batch in candidates:
            if not batch["row_count"]:
                continue
            if cutoff is not None and batch.get("min_available") is not None and _date_value(batch["min_available"]) > cutoff:
                continue
            if request.strict and batch["baseline"] and batch["commit_seq"] not in witnessed_commits:
                continue
            key = batch.get("key_identity")
            complete = key and batch.get("max_available") is not None and (cutoff is None or _date_value(batch["max_available"]) <= cutoff)
            if not complete:
                retained.append(batch)
                continue
            previous = latest.get(key)
            rank = (batch["max_available"],batch["commit_seq"])
            if previous is None or rank > (previous["max_available"],previous["commit_seq"]):
                latest[key] = batch
        retained.extend(latest.values())
        parents = set()
        for batch in retained:
            parent = batch.get("input_receipt_id")
            for check in evidence["audit_checks"]:
                if not check.get("input_receipt_id") or cutoff is not None and _date_value(check["pit_date"]) > cutoff:
                    continue
                if any(scope["commit_seq"] == batch["commit_seq"] and scope["key_identity"] == batch["key_identity"]
                        and scope["row_count"] == batch["row_count"] for scope in json.loads(check["content_scopes_json"])):
                    parent = check["input_receipt_id"]
            if parent:
                parents.add(str(parent))
        relevant_checks = {event["check_id"] for event in eligible_events
            if set(event["version_commits"]).intersection(batch["commit_seq"] for batch in retained)}
        parents.update(str(check["input_receipt_id"]) for check in evidence["audit_checks"]
            if check["id"] in relevant_checks and check.get("input_receipt_id"))
        return bool(retained),parents

    def _completed_parent_overrides(self, frozen, alias, proof) -> set[str]:
        """Find creator occurrences wholly covered by a native range completion.

        Unknown bounds, foreign/non-native authority, later output/checks and
        sources also used outside the certified interval retain their original
        currentness requirements. No row index or original value is consumed.
        """
        from .items.completion import output_check_bound, verified_completion_digest
        evidence = frozen.evidence[alias]
        source = self.get(proof["frozen_receipt_id"])
        if (frozen.information_cutoff is None or source.information_cutoff is None
                or source.information_cutoff > frozen.information_cutoff):
            return set()
        with self._store.connect() as db:
            if not db.execute("select 1 from sqlite_master where name='item_completion_authorities'").fetchone():
                return set()
            rows = db.execute("select b.id,a.payload_json from item_builds b "
                "join item_completion_authorities a on a.build_id=b.id "
                "where b.name=? and b.definition_hash=? and b.start_date=? and b.end_date=? "
                "and b.frozen_receipt_id=? and b.result_commit is ? and b.status='success'",
                (frozen.requests[alias].name, proof["definition_hash"], proof["start_date"], proof["end_date"],
                    proof["frozen_receipt_id"], proof["result_commit"])).fetchall()
            check_bounds = []
            for row in rows:
                verified_completion_digest(db, row["id"])
                authority = json.loads(row["payload_json"])
                bound = output_check_bound(db, row["id"], authority)
                if authority["kind"] == "native" and type(bound) is int and bound >= 0:
                    check_bounds.append(bound)
            if not check_bounds:
                return set()
            check_bound = max(check_bounds)
        first, last = _date_value(proof["start_date"]), _date_value(proof["end_date"])
        records = [*evidence["batches"], *evidence["audit_checks"], *evidence["empty_checks"]]
        candidates = {str(row["input_receipt_id"]) for row in records if row.get("input_receipt_id")}
        for batch in evidence["batches"]:
            parent = batch.get("input_receipt_id")
            lower, upper = batch.get("min_observation"), batch.get("max_observation")
            if (not batch["row_count"] or lower is None or upper is None
                    or not first <= _date_value(lower) <= _date_value(upper) <= last
                    or batch["commit_seq"] > (proof["result_commit"] or 0)):
                candidates.discard(str(parent))
        for check in [*evidence["audit_checks"], *evidence["empty_checks"]]:
            queries = json.loads(check["request_json"])
            scopes = [query["item_range"] for query in queries if "item_range" in query]
            if (not scopes or len(scopes) != len(queries) or check["id"] > check_bound or check["visible_commit"] is None
                    or check["visible_commit"] > (proof["result_commit"] or 0)
                    or any(not first <= _date_value(scope["start"]) <= _date_value(scope["end"]) <= last
                        for scope in scopes)):
                candidates.discard(str(check.get("input_receipt_id")))
        candidates -= self._uncovered_empty_parents(frozen, alias, proof)
        candidates.discard(proof["frozen_receipt_id"])
        return candidates

    def _native_parent_replacements(self, frozen, alias, *, db, options) -> dict[str, set[str]]:
        """Cover every captured creator occurrence using bounded native ranges.

        This handles a visible Item with no one covering completion. Original
        receipts remain unchanged. Unknown coordinates, later checks, foreign
        definitions or certificates beyond the consumer's capture stay stale.
        The caller checks every replacement Source through ordinary currentness.
        """
        from .items.completion import output_check_bound, verified_completion_digest
        if not db.execute("select 1 from sqlite_master where name='item_completion_authorities'").fetchone():
            return {}
        evidence = frozen.evidence[alias]
        if frozen.information_cutoff is None or frozen.max_check_id is None:
            return {}
        occurrences: dict[str, list[tuple[date, date, int | None, int | None] | None]] = {}
        def append(parent, occurrence):
            if parent is not None:
                occurrences.setdefault(str(parent), []).append(occurrence)
        for batch in evidence["batches"]:
            lower, upper = batch.get("min_observation"), batch.get("max_observation")
            append(batch.get("input_receipt_id"), None if not batch["row_count"] or lower is None or upper is None
                else (_date_value(lower), _date_value(upper), batch["commit_seq"], None))
        empty_ids = {check["id"] for check in evidence["empty_checks"] if check["row_count"] == 0}
        for kind in ("audit_checks", "empty_checks"):
            for check in evidence[kind]:
                queries = json.loads(check["request_json"])
                scopes = [query["item_range"] for query in queries if "item_range" in query]
                if (not scopes or len(scopes) != len(queries) or check["visible_commit"] is None
                        and check["id"] not in empty_ids):
                    append(check.get("input_receipt_id"), None)
                    continue
                for scope in scopes:
                    append(check.get("input_receipt_id"), (_date_value(scope["start"]),
                        _date_value(scope["end"]), check["visible_commit"], check["id"]))
        spans = [span for values in occurrences.values() for span in values if span is not None]
        if not spans:
            return {}
        first, last = min(span[0] for span in spans), max(span[1] for span in spans)
        rows = db.execute("select b.id,b.start_date,b.end_date,b.input_commit,b.result_commit,"
            "b.frozen_receipt_id,b.dependency_digest,length(a.payload_json) as payload_size "
            "from item_builds b join item_completion_authorities a on a.build_id=b.id "
            "where b.name=? and b.definition_hash=? and b.status='success' "
            "and b.input_commit<=? and (b.result_commit is null or b.result_commit<=?) "
            "and b.end_date>=? and b.start_date<=? "
            "and json_extract(a.payload_json,'$.kind')='native' order by b.id desc",
            (frozen.requests[alias].name, evidence["definition_hash"], frozen.max_commit,
                frozen.max_commit, str(first), str(last)))
        certificates = []
        admitted = 0
        for row in rows:
            admitted += int(row["payload_size"]) * 4 + 1024
            if admitted > options.max_buffer_bytes // 4:
                raise MemoryError("Native completion scope metadata exceeds max_buffer_bytes; use smaller declared input windows")
            verified_completion_digest(db, row["id"])
            authority = json.loads(db.execute("select payload_json from item_completion_authorities where build_id=?",
                (row["id"],)).fetchone()["payload_json"])
            bound = output_check_bound(db, row["id"], authority)
            if type(bound) is not int or not 0 <= bound <= frozen.max_check_id:
                continue
            source = self.get(row["frozen_receipt_id"])
            if (source.max_commit != row["input_commit"] or source.dependency_digest != row["dependency_digest"]
                    or source.information_cutoff is None or source.information_cutoff > frozen.information_cutoff
                    or source.max_check_id is None or source.max_check_id > frozen.max_check_id):
                continue
            certificates.append((row, bound))
        replacements = {}
        for parent, spans in occurrences.items():
            sources = set()
            for span in spans:
                if span is None:
                    break
                first, last, commit, check = span
                match = next((row for row, bound in certificates
                    if row["start_date"] <= str(first) <= str(last) <= row["end_date"]
                    and (commit is None or commit <= (row["result_commit"] or 0))
                    and (check is None or check <= bound)), None)
                if match is None:
                    break
                sources.add(match["frozen_receipt_id"])
            else:
                if sources:
                    replacements[parent] = sources
        return replacements

    def _completion_covers_request(self, frozen, alias, proof) -> bool:
        """Prove omitted bounds from captured/indexed coordinates, never rows."""
        from .items.completion import verified_completion_digest
        request = frozen.requests[alias]
        with self._store.connect() as db:
            if not db.execute("select 1 from sqlite_master where name='item_completion_proofs'").fetchone():
                return False
            matches = db.execute("select b.*,p.digest as proof_digest from item_builds b "
                "join item_completion_proofs p on p.build_id=b.id where name=? and definition_hash=? "
                "and start_date=? and end_date=? and frozen_receipt_id=? and result_commit is ?",
                (request.name, proof["definition_hash"], proof["start_date"], proof["end_date"],
                    proof["frozen_receipt_id"], proof["result_commit"])).fetchall()
            if not matches:
                return False
            for row in matches:
                verified_completion_digest(db, row["id"])
        if request.start is not None and request.end is not None:
            return True  # The captured covering-build SQL checked both bounds.
        from bagelquant_data import input_index
        first, last = _date_value(proof["start_date"]), _date_value(proof["end_date"])
        for check in frozen.evidence[alias]["empty_checks"]:
            ranges = [query["item_range"] for query in json.loads(check["request_json"]) if "item_range" in query]
            if not ranges or any(_date_value(scope["start"]) < first or _date_value(scope["end"]) > last for scope in ranges):
                return False
        with self._store.connect() as db:
            for batch in frozen.evidence[alias]["batches"]:
                if not batch["row_count"]:
                    continue
                lower, upper = batch.get("min_observation"), batch.get("max_observation")
                if lower is None or upper is None:
                    bounds = input_index.describe_bounds(db, batch)
                    if bounds is None or not bounds["record"]:
                        return False
                    lower, upper = bounds["lower"], bounds["upper"]
                if lower is None or upper is None or _date_value(lower) < first or _date_value(upper) > last:
                    return False
        return True

    def _has_pending_item_outputs(self, frozen, alias) -> bool:
        """Unpublished matured coordinates require producer work, not an audit."""
        from .items.completion import pending_dates
        request = frozen.requests[alias]
        with self._store.connect() as db:
            if not db.execute("select 1 from sqlite_master where name='item_completion_proofs'").fetchone():
                return False
            rows = db.execute("select b.*,p.digest as proof_digest from item_builds b "
                "join item_completion_proofs p on p.build_id=b.id join frozen_inputs f on f.receipt_id=b.frozen_receipt_id "
                "where name=? and definition_hash=? and status='success' and input_commit<=? "
                "and (result_commit is null or result_commit<=?) "
                "and cast(json_extract(f.payload_json,'$.max_check_id') as integer)<=? "
                "and (? is null or end_date>=?) and (? is null or start_date<=?) order by b.id desc",
                (request.name, frozen.evidence[alias]["definition_hash"], frozen.max_commit, frozen.max_commit,
                    frozen.max_check_id, request.start, request.start, request.end, request.end)).fetchall()
            covered = []
            for row in rows:
                body = {key: value for key, value in dict(row).items() if key != "proof_digest"}
                if hashlib.sha256(_json(body).encode()).hexdigest() != row["proof_digest"]:
                    raise RuntimeError("DataItem completion proof source binding differs")
                for day, available in pending_dates(db, row["id"]):
                    if (request.start is not None and day < str(request.start)
                            or request.end is not None and day > str(request.end)
                            or any(first <= day <= last for first, last in covered)):
                        continue
                    if frozen.information_cutoff is None or _date_value(available) <= frozen.information_cutoff:
                        return True
                covered.append((row["start_date"], row["end_date"]))
        return False

    def _uncovered_empty_parents(self, frozen, alias, proof) -> set[str]:
        """Empty native builds carry completed ranges even without any batches."""
        request = frozen.requests[alias]
        with self._store.connect() as db:
            rows = db.execute("select b.* from item_builds b join frozen_inputs f on f.receipt_id=b.frozen_receipt_id "
                "where name=? and definition_hash=? and status='success' and result_commit is null "
                "and input_commit<=? and cast(json_extract(f.payload_json,'$.max_check_id') as integer)<=? "
                "and (? is null or end_date>=?) and (? is null or start_date<=?)",
                (request.name, frozen.evidence[alias]["definition_hash"], frozen.max_commit, frozen.max_check_id,
                    request.start, request.start, request.end, request.end)).fetchall()
        return {row["frozen_receipt_id"] for row in rows if proof is None
            or row["start_date"] < proof["start_date"] or row["end_date"] > proof["end_date"]}

    def get(self, receipt: FrozenInputReceipt | str) -> FrozenInputReceipt:
        context = self._read_context()
        with context.lock if context is not None else nullcontext():
            return self._get(receipt)

    def _get(self, receipt: FrozenInputReceipt | str) -> FrozenInputReceipt:
        key = receipt.receipt_id if isinstance(receipt, FrozenInputReceipt) else receipt
        context = self._read_context()
        if context is not None and (context.closed or context.parent is not None and context.parent.closed):
            raise RuntimeError("Frozen metadata source has expired")
        if context is not None:
            if context.check_canceled is not None:
                context.check_canceled()
            cached = context.receipts.get(key) if context.memo_available else None
            if cached is not None:
                if isinstance(receipt, FrozenInputReceipt) and receipt.digest != cached.digest:
                    raise RuntimeError("Frozen input receipt checksum mismatch")
                return cached
        rows = self._store._rows("select payload_json,digest from frozen_inputs where receipt_id=?", (key,))
        if not rows:
            raise KeyError(f"Unknown frozen input receipt: {key}")
        serialized = str(rows[0]["payload_json"])
        digest = hashlib.sha256(serialized.encode()).hexdigest()
        if digest != rows[0]["digest"] or isinstance(receipt, FrozenInputReceipt) and digest != receipt.digest:
            raise RuntimeError("Frozen input receipt checksum mismatch")
        payload = json.loads(serialized)
        if payload.get("format") != 9:
            raise RuntimeError("Incompatible frozen input metadata format; rebuild the lake explicitly")
        result = self._receipt(key, payload, digest)
        if context is not None and context.memo_available:
            result = replace(result, requests=_readonly(result.requests))
            context.receipts[key] = result
        return result

    def _could_have_baseline(self, frozen: FrozenInputReceipt, alias: str,
                             seen: frozenset[str] = frozenset()) -> bool:
        """Prove verified timing from immutable flags, including empty parents."""
        if frozen.receipt_id in seen:
            raise RuntimeError("Frozen input receipt dependency cycle")
        evidence = frozen.evidence[alias]
        if any(batch["baseline"] for batch in evidence["batches"]) or any(
            snapshot["baseline"] for snapshot in evidence["general_snapshots"]
        ) or any(check["baseline"] for check in evidence.get("empty_checks", ())):
            return True
        build = evidence["empty_item_build"]
        if build is None:
            return isinstance(frozen.requests[alias], ItemInput) and bool(evidence["definition"]["inputs"])
        parent = self.get(build["frozen_receipt_id"])
        return any(self._could_have_baseline(parent, key, seen | {frozen.receipt_id})
                   for key in parent.requests)

    @staticmethod
    def _proven_baseline_at(frozen: FrozenInputReceipt, alias: str, cutoff: date, *,
                            max_commit: int | None = None,
                            max_check_id: int | None = None) -> bool | None:
        """Prove a visible baseline row; unknown remains inconclusive for planning.

        Uniform committed flags and entire coordinate containment establish
        a surviving row only when no visible witness can replace its timing.
        Fully visible batches also retain occupied observation endpoints when
        partial verification or revisions affect only other dates.
        This positive-only proof never substitutes for original byte integrity.
        """
        request, evidence = frozen.requests[alias], frozen.evidence[alias]
        if isinstance(request, RawInput) and evidence["general_snapshots"]:
            from bagelquant_data.items.pit import _general_snapshot_identity

            cutoff = min(cutoff, frozen.information_cutoff) if frozen.information_cutoff is not None else cutoff
            commit_bound = min(frozen.max_commit, max_commit) if max_commit is not None else frozen.max_commit
            check_bound = min(frozen.max_check_id, max_check_id) if max_check_id is not None else frozen.max_check_id
            selected = _general_snapshot_identity(
                [row for row in evidence["general_snapshots"] if row["seq"] <= commit_bound], cutoff,
                checks=[row for row in evidence["checks"] if row["id"] <= check_bound],
                strict=request.strict, include_historical_baseline=request.include_historical_baseline,
            )
            if selected is not None and selected["baseline"]:
                return True
            return None
        if (not (evidence.get("scoped_batches") or evidence.get("retained_batch_bounds")) or evidence["general_snapshots"]
                or request.strict or request.view not in {"snapshot", "versions"}):
            return None
        cutoff = min(cutoff, frozen.information_cutoff) if frozen.information_cutoff is not None else cutoff
        commit_bound = min(frozen.max_commit, max_commit) if max_commit is not None else frozen.max_commit
        check_bound = min(frozen.max_check_id, max_check_id) if max_check_id is not None else frozen.max_check_id
        batches = [batch for batch in evidence["batches"] if batch["commit_seq"] <= commit_bound]
        if not batches:
            return None
        from bagelquant_data.storage.full_commit_checks import validate_seal
        checks = list(evidence["checks"])
        sealed_commits = {}
        for seal in evidence.get("full_commit_checks", ()):
            validate_seal(seal)
            checks.append(seal["binding"]["check"])
            sealed_commits[seal["binding"]["check"]["id"]] = seal["binding"]["commit"]["seq"]
        eligible_checks = {check["id"]: check for check in checks if not check["baseline"]
            and check["id"] <= check_bound and _date_value(check["pit_date"]) <= cutoff}
        witnessed_checks = set(sealed_commits)
        affected_commits = {seq for key, seq in sealed_commits.items() if key in eligible_checks}
        events = []
        for witness in evidence["record_checks"]:
            if witness.get("event"):
                commits = witness.get("version_commits")
                if not commits:
                    continue
                witnessed_checks.add(witness["check_id"])
                if witness["check_id"] in eligible_checks:
                    first, last = witness.get("min_observation"), witness.get("max_observation")
                    if first is None or last is None or _date_value(first) > _date_value(last):
                        affected_commits.update(commits)
                    else:
                        events.append((frozenset(commits), _date_value(first), _date_value(last)))
                continue
            witnessed_checks.add(witness["check_id"])
            if witness["check_id"] in eligible_checks:
                affected_commits.add(witness.get("version_commit", -1))
        if any(check["row_count"] > 0 and key not in witnessed_checks for key, check in eligible_checks.items()):
            return None
        lower, upper = [request.start], [request.end]
        if isinstance(request, RawInput):
            lower.append(request.observation_start)
            upper.append(request.observation_end)
        candidates = []
        for batch in batches:
            if batch.get("min_observation") is None or batch.get("max_observation") is None:
                return None
            first, last = _date_value(batch["min_observation"]), _date_value(batch["max_observation"])
            if first > last:
                return None
            if (batch["baseline"] and batch["row_count"] > 0
                    and batch.get("min_available") is not None
                    and _date_value(batch["min_available"]) <= cutoff
                    and not any(value is not None and first < _date_value(value) for value in lower)
                    and not any(value is not None and last > _date_value(value) for value in upper)):
                candidates.append((first, last, batch["commit_seq"],
                    batch.get("max_available") is not None
                    and _date_value(batch["max_available"]) <= cutoff))
        for first, last, seq, fully_visible in candidates:
            if seq in affected_commits:
                continue
            # Visibility events retain natural-key bounds rather than a row
            # index. A single commit can contain many observation partitions;
            # only intersecting event ranges can replace a candidate's timing.
            overlaps = [(lower, upper) for commits, lower, upper in events
                        if seq in commits and lower <= last and upper >= first]
            overlaps.extend((_date_value(batch["min_observation"]), _date_value(batch["max_observation"]))
                for batch in batches if not batch["baseline"]
                and not (batch.get("min_available") is not None
                    and _date_value(batch["min_available"]) > cutoff)
                and _date_value(batch["min_observation"]) <= last
                and _date_value(batch["max_observation"]) >= first)
            if not overlaps:
                return True
            # Bounds name occupied dates, not every date between them. All
            # rows must be visible before an endpoint can witness survival;
            # a later-available endpoint cannot stand in for an earlier row.
            if fully_visible and any(not any(lower <= day <= upper for lower, upper in overlaps)
                                     for day in (first, last)):
                return True
        return None

    def _indexed_baseline_proof(self, frozen: FrozenInputReceipt, alias: str, cutoff: date, *,
                                max_commit: int | None, max_check_id: int | None) -> bool | None:
        """Use checked optional headers for a positive timing proof on legacy scopes."""
        from bagelquant_data import input_index
        evidence = frozen.evidence[alias]
        if evidence["general_snapshots"]:
            return None
        batches = []
        with self._store.connect() as db:
            for original in evidence["batches"]:
                batch = dict(original)
                if batch.get("min_observation") is None or batch.get("max_observation") is None:
                    summary = input_index.describe_bounds(db, batch)
                    if summary is None or not summary["record"]:
                        return None
                    batch.update(min_observation=summary["lower"], max_observation=summary["upper"],
                                 min_available=summary["available"])
                batches.append(batch)
        # This transient positive proof does not change frozen read scopes or
        # identities. Its bounds are bound to the original immutable batches.
        proof = replace(frozen, evidence={**frozen.evidence,
            alias: {**evidence, "scoped_batches": True, "batches": batches}})
        return self._proven_baseline_at(proof, alias, cutoff,
            max_commit=max_commit, max_check_id=max_check_id)

    def _baseline_at(self, frozen: FrozenInputReceipt, alias: str, cutoff: date, *,
                     frame: pl.DataFrame | None = None, max_commit: int | None = None,
                     max_check_id: int | None = None,
                     max_buffer_bytes: int | None = None,
                     seen: frozenset[str] = frozenset()) -> bool:
        """Select timing proof at one cutoff, following empty Item inputs."""
        if frozen.receipt_id in seen:
            raise RuntimeError("Frozen input receipt dependency cycle")
        if frame is None and self._proven_baseline_at(frozen, alias, cutoff,
                max_commit=max_commit, max_check_id=max_check_id) is True:
            return True
        if frame is None and self._indexed_baseline_proof(frozen, alias, cutoff,
                max_commit=max_commit, max_check_id=max_check_id) is True:
            return True
        request = frozen.requests[alias]
        evidence = frozen.evidence[alias]
        cutoff = min(cutoff, frozen.information_cutoff) if frozen.information_cutoff is not None else cutoff
        commit_bound = min(frozen.max_commit, max_commit) if max_commit is not None else frozen.max_commit
        check_bound = min(frozen.max_check_id, max_check_id) if max_check_id is not None else frozen.max_check_id
        if frame is None:
            from bagelquant_data import input_index
            from bagelquant_data.query.raw import _attested_versions
            pieces, buffered = [], 0
            context = self._read_context()
            budget = max_buffer_bytes or (context.options.max_buffer_bytes if context else ExecutionOptions().max_buffer_bytes)
            with self._store.connect():
                for batch in evidence["batches"]:
                    if context is not None and context.check_canceled is not None:
                        context.check_canceled()
                    projected = input_index.project_original(self._store,batch,
                        max_bytes=max(1,budget // 2-buffered),
                        check_canceled=None if context is None else context.check_canceled)
                    if projected is None:
                        raise ValueError("Timing index is unknown; run explicit dependency index maintenance")
                    buffered += projected.estimated_size() * 2
                    if projected.width:
                        pieces.append(projected)
            versions = concat_compatible_frames(pieces) if pieces else pl.DataFrame()
            witnesses = len(evidence["record_checks"]) + versions.height * len(evidence.get("full_commit_checks", ()))
            if witnesses * max(128, versions.estimated_size() // max(1, versions.height)) > budget // 2:
                raise MemoryError("Timing metadata exceeds max_buffer_bytes")
            if versions.width:
                versions = _attested_versions(versions.lazy(), evidence["checks"], evidence["record_checks"],
                    as_of_date=cutoff, max_check_id=check_bound,
                    full_commit_checks=evidence.get("full_commit_checks", ()),event_paths=self._lake._paths).collect()
        else:
            versions = frame
        if max_buffer_bytes is not None and versions.estimated_size() > max_buffer_bytes // 2:
            raise MemoryError("Empty Item timing evidence exceeds max_buffer_bytes; use smaller declared input windows")
        if "_commit_seq" in versions.columns:
            versions = versions.filter(pl.col("_commit_seq") <= commit_bound)
        if "_attestation_id" in versions.columns:
            versions = versions.filter(pl.col("_attestation_id").is_null() | (pl.col("_attestation_id") <= check_bound))
        selection = replace(request, view="snapshot") if request.view == "versions" else request
        if isinstance(selection, RawInput):
            selection = replace(selection, fields=())
        bounded = {**evidence,
                   "general_snapshots": [{**row, "schema_ipc": None if frame is None else row["schema_ipc"]}
                                         for row in evidence["general_snapshots"] if row["seq"] <= commit_bound],
                   "checks": [row for row in evidence["checks"] if row["id"] <= check_bound]}
        selected = self._select(versions, selection, cutoff, evidence=bounded) if versions.width else versions
        if "_baseline" in selected.columns and selected["_baseline"].fill_null(False).any():
            return True
        if isinstance(selection, RawInput) and bounded["general_snapshots"]:
            from bagelquant_data.items.pit import _general_snapshot_identity
            identity = _general_snapshot_identity(bounded["general_snapshots"], cutoff,
                checks=bounded["checks"], strict=selection.strict,
                include_historical_baseline=selection.include_historical_baseline)
            return identity is not None and bool(identity["baseline"])
        build = evidence["empty_item_build"]
        if selected.height or request.strict:
            return False
        unknown_empty = [check for check in evidence.get("empty_checks", ())
                         if check["baseline"] and check["id"] <= check_bound]
        if isinstance(request, RawInput):
            return bool(unknown_empty)
        if unknown_empty:
            # A dated empty response is not a whole-dataset attestation.
            # Only an explicit complete Item range can supersede it here.
            proven = []
            if request.start is not None and request.end is not None:
                for check in evidence.get("empty_checks", ()):
                    if check["baseline"] or check["id"] > check_bound or _date_value(check["pit_date"]) > cutoff:
                        continue
                    for query in json.loads(check["request_json"]):
                        scope = query.get("item_range")
                        if scope and _date_value(scope["start"]) <= _date_value(request.start) and _date_value(scope["end"]) >= _date_value(request.end):
                            proven.append(check["id"])
            if not proven or max(proven) < max(check["id"] for check in unknown_empty):
                return True
        if build is None:
            return bool(evidence["definition"]["inputs"])
        parent = self.get(build["frozen_receipt_id"])
        return any(self._baseline_at(parent, key, cutoff, max_commit=commit_bound,
                   max_check_id=check_bound, max_buffer_bytes=max_buffer_bytes,
                   seen=seen | {frozen.receipt_id})
                   for key in parent.requests)

    def _empty_baseline_boundaries(self, frozen: FrozenInputReceipt, alias: str,
                                  seen: frozenset[str] = frozenset()) -> set[date]:
        """Expose empty upstream proof revisions to ordinary producer execution."""
        if frozen.receipt_id in seen:
            raise RuntimeError("Frozen input receipt dependency cycle")
        build = frozen.evidence[alias]["empty_item_build"]
        if not self._could_have_baseline(frozen, alias):
            return set()
        days = {_date_value(row["pit_date"]) for row in frozen.evidence[alias].get("empty_checks", ())
                if not row["baseline"] and row["id"] <= frozen.max_check_id
                and any(query.get("item_range") for query in json.loads(row["request_json"]))}
        if build is None:
            return days
        parent = self.get(build["frozen_receipt_id"])
        for key, evidence in parent.evidence.items():
            days.update(_date_value(row["pit_date"]) for row in evidence["batches"]
                        if not row["baseline"] and row["commit_seq"] <= frozen.max_commit)
            days.update(_date_value(row["pit_date"]) for row in evidence["checks"]
                        if row["id"] <= frozen.max_check_id)
            days.update(self._empty_baseline_boundaries(parent, key, seen | {frozen.receipt_id}))
        return days

    def read(
        self, receipt: FrozenInputReceipt | str, alias: str, *,
        view: str | None = None, as_of: DateLike | None = None,
        fields: tuple[str, ...] | list[str] | None = None,
        strict: bool | None = None,
        observations: bool = False,
        start: DateLike | None = None, end: DateLike | None = None,
        date_range: tuple[str, DateLike, DateLike] | None = None,
    ) -> pl.LazyFrame:
        """Replay frozen evidence with an optional tighter information cutoff.

        View and projection overrides retain the original observation window,
        definition, commit boundary and check boundary after object archival.
        ``date_range=(extra_key, first, last)`` is an inclusive row-local
        secondary-date predicate for by-date Raw versions only. It preserves
        original keys/PIT witnesses; legacy string keys can require a full
        date-column lookup, reported by the read-context progress callback.
        """
        frozen = self.get(receipt)
        if alias not in frozen.requests:
            raise KeyError(f"Unknown frozen input alias: {alias}")
        request = frozen.requests[alias]
        lower, upper = request.start, request.end
        if isinstance(request, RawInput):
            lower = max((_date_value(value) for value in (lower, request.observation_start) if value is not None), default=None)
            upper = min((_date_value(value) for value in (upper, request.observation_end) if value is not None), default=None)
        if start is not None and lower is not None and _date_value(start) < _date_value(lower):
            raise ValueError("start cannot widen the frozen observation window")
        if end is not None and upper is not None and _date_value(end) > _date_value(upper):
            raise ValueError("end cannot widen the frozen observation window")
        request = replace(request, start=lower if start is None else start,
                          end=upper if end is None else end)
        if request.start is not None and request.end is not None and _date_value(request.start) > _date_value(request.end):
            raise ValueError("end precedes start")
        cutoff = frozen.information_cutoff if as_of is None else _date_value(as_of)
        if frozen.information_cutoff is not None and cutoff is not None and cutoff > frozen.information_cutoff:
            raise ValueError("as_of exceeds the frozen information cutoff")
        projection = request.fields if isinstance(request, RawInput) else ()
        if fields is not None:
            projection = tuple(fields)
        request = replace(request, view=request.view if view is None else view,
                          strict=request.strict if strict is None else strict)
        selected_dates = None
        if date_range is not None:
            column, first, last = date_range
            definition = frozen.evidence[alias]["definition"]
            if (not isinstance(column, str) or not isinstance(request, RawInput) or request.view != "versions"
                    or definition.get("update_type") != "by_date"
                    or column not in definition.get("primary_key_extra", ())
                    or column.startswith("_") or column in {"time", "source_time", "ingested_at", "asset_id", "snapshot_date", "available_date", "observation_date", "year", "month"}):
                raise ValueError("date_range requires an immutable extra Raw key and by-date versions view")
            selected_dates = (column, _date_value(first), _date_value(last))
            if selected_dates[1] > selected_dates[2]:
                raise ValueError("date_range end precedes start")
        if isinstance(request, RawInput):
            request = replace(request, fields=())
        frame = self._select(self._read_frame(frozen, alias, request=request, date_range=selected_dates), request, cutoff,
                             evidence=frozen.evidence[alias])
        if observations and isinstance(request, RawInput) and "source_time" in frame.columns:
            frame = frame.with_columns(pl.col("source_time").alias("time"))
        if projection:
            missing = set(projection) - set(frame.columns)
            if missing:
                raise ValueError(f"Frozen input is missing requested fields: {sorted(missing)}")
            frame = frame.select(projection)
        return frame.lazy()

    def window_read_supported(self, receipt: FrozenInputReceipt | str, alias: str) -> bool:
        """Whether captured evidence can safely prune physical observation reads.

        A metadata-only planning query. Original captured bounds, captured seals
        and optional index headers bound to original batch triples are admissible.
        Missing legacy headers return false; this never builds or decodes indexes,
        infers bounds from live batch metadata, or promotes General inputs.
        It does not verify input bytes or promise that a given window skips work.
        """
        frozen = self.get(receipt)
        if alias not in frozen.requests:
            raise KeyError(f"Unknown frozen input alias: {alias}")
        evidence = frozen.evidence[alias]
        if evidence["general_snapshots"] or evidence["definition"].get("update_type") == "general":
            return False
        from bagelquant_data.storage.full_commit_checks import validate_seal
        sealed: set[tuple[int, str, str]] = set()
        for seal in evidence.get("full_commit_checks", ()):
            validate_seal(seal)
            for bound in seal["batch_bounds"]:
                if bound["observation_min"] is not None and bound["observation_max"] is not None:
                    sealed.add((bound["commit_seq"], bound["partition_path"], bound["content_hash"]))
        with self._store.connect() as db:
            for batch in evidence["batches"]:
                if (batch["commit_seq"], batch["partition_path"], batch["content_hash"]) in sealed:
                    continue
                bound, _ = self._original_window_bounds(db, evidence, batch)
                if bound is None:
                    return False
        return True

    def _original_window_bounds(self, db, evidence, batch):
        """Resolve existing checked headers without changing original evidence."""
        context = self._read_context()
        if context is not None and context.check_canceled is not None:
            context.check_canceled()
        if (evidence.get("scoped_batches") or evidence.get("retained_batch_bounds")) and (
                batch.get("min_observation") is not None and batch.get("max_observation") is not None):
            return batch, False
        if evidence["general_snapshots"] or evidence["definition"].get("update_type") == "general":
            return None, False
        from bagelquant_data import input_index
        options = context.options if context is not None else ExecutionOptions()
        summary = input_index.describe_bounds(db, batch, max_bytes=options.max_buffer_bytes // 16)
        if summary is None or not summary["record"] or summary["lower"] is None or summary["upper"] is None:
            return None, False
        if _date_value(summary["lower"]) > _date_value(summary["upper"]):
            raise RuntimeError("Selection index observation bounds are damaged; run explicit index maintenance")
        return {**batch, "min_observation": summary["lower"], "max_observation": summary["upper"],
                "min_available": summary["available"]}, True

    def _read_frame(self, frozen: FrozenInputReceipt, alias: str, *,
                    timing_cutoff: date | None = None, request: DataInput | None = None,
                    date_range: tuple[str, date, date] | None = None) -> pl.DataFrame:
        """Load exact retained versions before view or field selection."""
        if alias not in frozen.requests:
            raise KeyError(f"Unknown frozen input alias: {alias}")
        request = frozen.requests[alias] if request is None else request
        evidence = frozen.evidence[alias]
        # Only exact immutable alias headers can share numerical preparation.
        # The enclosing receipt/alias name is deliberately not a cache key;
        # request, checks, schema and original Source are bound by the header.
        from bagelquant_data.items.preparation import current_preparation
        prepared = current_preparation(self._store.data_meta_path)
        preparation_key = None
        if prepared is not None and isinstance(evidence, immutable_metadata.LazyMetadataMapping):
            preparation_key = (evidence.reference["ref"], _json(input_payload(request)),
                frozen.information_cutoff, timing_cutoff, date_range)
            cached = prepared.get(preparation_key)
            if cached is not None:
                return cached
        observation_lower = [request.start]
        observation_upper = [request.end]
        if isinstance(request, RawInput):
            observation_lower.append(request.observation_start)
            observation_upper.append(request.observation_end)
        observation_start = max((_date_value(value) for value in observation_lower if value is not None), default=None)
        observation_end = min((_date_value(value) for value in observation_upper if value is not None), default=None)
        if evidence["general_snapshots"] or evidence["definition"].get("update_type") == "general":
            # A General input selects one complete event before row filters.
            observation_start = observation_end = None
        # Timing-only selection cannot observe future physical availability.
        # General snapshots and missing legacy headers keep their broad reads.
        # Existing index metadata may prune original batches; no index rows or
        # original values are decoded to decide which batches are needed.
        effective_cutoff = frozen.information_cutoff
        if timing_cutoff is not None:
            effective_cutoff = timing_cutoff if effective_cutoff is None else min(timing_cutoff, effective_cutoff)
        from bagelquant_data.storage.full_commit_checks import validate_seal
        bounds = {}
        for seal in evidence.get("full_commit_checks", ()):
            validate_seal(seal)
            for batch in seal["batch_bounds"]:
                bounds[(batch["commit_seq"], batch["partition_path"], batch["content_hash"])] = batch
        restored = False
        def needed(db, batch: Mapping[str, Any]) -> bool:
            nonlocal restored
            registered, indexed = self._original_window_bounds(db, evidence, batch)
            restored |= indexed
            if registered is not None:
                lower, upper = [request.start], [request.end]
                if isinstance(request, RawInput):
                    lower.append(request.observation_start)
                    upper.append(request.observation_end)
                if effective_cutoff is not None and registered["min_available"] is not None and _date_value(registered["min_available"]) > effective_cutoff:
                    return False
                if any(value is not None and registered["max_observation"] is not None and _date_value(registered["max_observation"]) < _date_value(value) for value in lower):
                    return False
                if any(value is not None and registered["min_observation"] is not None and _date_value(registered["min_observation"]) > _date_value(value) for value in upper):
                    return False
            bound = bounds.get((batch["commit_seq"], batch["partition_path"], batch["content_hash"]))
            if bound is None:
                return True
            return (request.start is None or _date_value(bound["observation_max"]) >= _date_value(request.start)) and (request.end is None or _date_value(bound["observation_min"]) <= _date_value(request.end))
        with self._store.connect() as db:
            selected = [value for value in _selection_batches(frozen, alias, request, effective_cutoff) if needed(db, value)]
        context = self._read_context()
        if restored and (request.start is not None or request.end is not None):
            options = context.options if context is not None else ExecutionOptions()
            if sum(value["row_count"] for value in selected) * 256 > options.max_buffer_bytes:
                raise MemoryError("Original indexed window exceeds max_buffer_bytes; use a larger explicit read allocation or a smaller window")
        pieces = []
        retained_bytes = 0
        for value in selected:
            piece = read_batch(self._store, value["partition_path"], int(value["commit_seq"]), value["content_hash"],
                check_canceled=None if context is None else context.check_canceled,
                observation_start=observation_start, observation_end=observation_end,
                date_range=date_range,
                progress=None if context is None else context.progress,
                max_buffer_bytes=None if context is None else max(1, context.options.max_buffer_bytes - retained_bytes))
            retained_bytes += int(piece.estimated_size())
            if context is not None and retained_bytes > context.options.max_buffer_bytes:
                raise MemoryError("Frozen DataItem inputs exceed max_buffer_bytes; use smaller declared input windows")
            pieces.append(piece)
        if pieces:
            if date_range is None:
                frame = concat_compatible_frames(pieces)
            else:
                # Preserve the captured Originals' nullable types, including
                # typed empty pieces. A selected all-null payload is not a
                # declaration that the original field's type became Null.
                schema = compatible_schema(piece.schema for piece in pieces)
                frame = pl.concat([align_frame(piece, schema) for piece in pieces], how="vertical", rechunk=False)
        elif evidence["schema_ipc"]:
            schema = pa.ipc.read_schema(pa.BufferReader(bytes.fromhex(evidence["schema_ipc"])))
            frame = cast(pl.DataFrame, pl.from_arrow(pa.Table.from_batches([], schema=schema)))
        elif isinstance(request, ItemInput):
            from bagelquant_data.transforms import scalar_dtype
            frame = pl.DataFrame(schema={"time": pl.Date,"source_time": pl.Date,"asset_id": pl.String,"value": scalar_dtype(evidence["definition"]["value_dtype"])})
        else:
            frame = pl.DataFrame()
        from bagelquant_data.query.raw import _attested_versions
        frame = _attested_versions(frame.lazy(), evidence["checks"], evidence["record_checks"],
                                   as_of_date=effective_cutoff,
                                   max_check_id=frozen.max_check_id,
                                   full_commit_checks=evidence.get("full_commit_checks", ()),event_paths=self._lake._paths,
                                   observation_start=observation_start, observation_end=observation_end,
                                   date_range=date_range,
                                   max_buffer_bytes=None if context is None else context.options.max_buffer_bytes,
                                   progress=None if context is None else context.progress,
                                   check_canceled=None if context is None else context.check_canceled).collect()
        if prepared is not None and preparation_key is not None:
            prepared.source_rows += sum(piece.height for piece in pieces)
            prepared.key_rows += sum(piece.height for piece in pieces if "source_time" in piece.columns)
            prepared.put(preparation_key, frame)
        return frame

    def verify(self, receipt: FrozenInputReceipt | str | Sequence[FrozenInputReceipt | str], *,
               config: ExecutionOptions | None = None,
               check_canceled: Callable[[], None] | None = None,
               progress: Callable[[dict[str, Any]], None] | None = None) -> dict[str, Any]:
        """Raise on lost or corrupt evidence, even after object archival."""
        single = isinstance(receipt, (FrozenInputReceipt, str))
        if not single and not receipt:
            raise ValueError("Verification requires at least one frozen input receipt")
        # Lazy evidence shares one finite metadata snapshot. A newly entered
        # context has no successful byte proofs from an earlier audit.
        view = (self.read_context(receipt, config=config, check_canceled=check_canceled)
                if self._read_context() is None else nullcontext())
        with view:
            return self._verify(receipt, config=config, check_canceled=check_canceled, progress=progress)

    def _verify(self, receipt: FrozenInputReceipt | str | Sequence[FrozenInputReceipt | str], *,
                config: ExecutionOptions | None,
                check_canceled: Callable[[], None] | None,
                progress: Callable[[dict[str, Any]], None] | None) -> dict[str, Any]:
        single = isinstance(receipt, (FrozenInputReceipt, str))
        roots = [self.get(receipt)] if single else [self.get(value) for value in receipt]
        if not roots:
            raise ValueError("Verification requires at least one frozen input receipt")
        context = self._read_context()
        if check_canceled is None and context is not None:
            check_canceled = context.check_canceled
        retained_receipts = {value.receipt_id: value for value in roots}
        batch_count = 0
        verified: set[str] = set()
        metadata_verified: set[str] = set()
        verified_batches: set[tuple[int, str, str]] = set()
        batches_to_verify: list[tuple[int, str, str]] = []
        visiting: set[str] = set()
        graph: dict[str, tuple[int, tuple[FrozenInputReceipt, ...]]] = {}

        def verify_retained(value: FrozenInputReceipt) -> None:
            nonlocal batch_count
            if check_canceled is not None:
                check_canceled()
            if value.receipt_id in visiting:
                raise RuntimeError("Frozen input receipt dependency cycle")
            with self._store.connect() as db:
                registered = db.execute("SELECT payload_json FROM frozen_inputs WHERE receipt_id=?", (value.receipt_id,)).fetchone()
                if registered is not None:
                    reader = immutable_metadata.MetadataReader(database=db)
                    for reference in json.loads(registered["payload_json"])["evidence"].values():
                        if check_canceled is not None:
                            check_canceled()
                        reader.audit(reference, metadata_verified)
            if value.receipt_id in verified:
                return
            visiting.add(value.receipt_id)
            prior = None if context is None or not context.memo_available else context.verified_graph.get(value.receipt_id)
            if prior is not None:
                batch_count += prior[0]
                for parent in prior[1]:
                    verify_retained(parent)
                visiting.remove(value.receipt_id)
                verified.add(value.receipt_id)
                return
            local_count = 0
            parents: dict[str, FrozenInputReceipt] = {}
            for evidence in value.evidence.values():
                from bagelquant_data.storage.full_commit_checks import validate_seal
                retained = None
                for seal in evidence.get("full_commit_checks", ()):
                    validate_seal(seal)
                    if retained is None:
                        retained = {(batch["commit_seq"], batch["partition_path"], batch["content_hash"], batch["row_count"]) for batch in evidence["batches"]}
                    if any((batch["commit_seq"], batch["partition_path"], batch["content_hash"], batch["row_count"]) not in retained for batch in seal["binding"]["batches"]):
                        raise RuntimeError("Full-commit check seal retained batch mismatch")
                for batch in evidence["batches"]:
                    if check_canceled is not None:
                        check_canceled()
                    key = (int(batch["commit_seq"]), batch["partition_path"], batch["content_hash"])
                    if key not in verified_batches:
                        if context is None or not context.memo_available or key not in context.verified_batches:
                            batches_to_verify.append(key)
                        verified_batches.add(key)
                    batch_count += 1
                    local_count += 1
                for parent_id, parent_digest in evidence["parent_receipts"].items():
                    parent = retained_receipts.get(parent_id)
                    if parent is None:
                        parent = self.get(parent_id)
                        retained_receipts[parent_id] = parent
                    if parent.digest != parent_digest:
                        raise RuntimeError("Frozen upstream input receipt checksum mismatch")
                    parents[parent.receipt_id] = parent
                    verify_retained(parent)
            visiting.remove(value.receipt_id)
            verified.add(value.receipt_id)
            graph[value.receipt_id] = local_count, tuple(parents.values())

        for frozen in roots:
            verify_retained(frozen)
        options = config or (context.options if context is not None else ExecutionOptions())
        # Evidence-key summaries can be consumed by an equivalent later root.
        # Its explicit audit therefore includes each applicable summary's
        # original source and complete byte/index closure, rather than auditing
        # only summaries first recorded against this enclosing receipt UUID.
        from bagelquant_data import input_index
        summary_sources_checked = set()
        with self._store.connect() as db:
            has_summaries = db.execute("SELECT 1 FROM sqlite_master WHERE name='input_projection_index'").fetchone()
            while has_summaries and (pending_sources := [value for identifier, value in retained_receipts.items()
                    if identifier not in summary_sources_checked]):
                for frozen in pending_sources:
                    summary_sources_checked.add(frozen.receipt_id)
                    for alias, original_request in frozen.requests.items():
                        if (isinstance(original_request, RawInput)
                                and frozen.evidence[alias]["definition"]["update_type"] != "by_date"):
                            cursor = db.execute("SELECT rowid,length(request_key)+length(evidence_key)+512 AS size "
                                "FROM input_projection_index WHERE json_extract(request_key,'$.projection')=? "
                                "AND json_extract(request_key,'$.request.source')=? "
                                "AND json_extract(request_key,'$.request.dataset')=?",
                                (_COORDINATE_PROJECTION, original_request.source, original_request.dataset))
                            while candidate := cursor.fetchone():
                                if check_canceled is not None:
                                    check_canceled()
                                if int(candidate["size"]) * 8 > options.max_buffer_bytes // 4:
                                    raise MemoryError("Coordinate projection audit metadata exceeds max_buffer_bytes")
                                row = db.execute("SELECT evidence_key,request_key FROM input_projection_index WHERE rowid=?",
                                    (candidate["rowid"],)).fetchone()
                                key = json.loads(row["request_key"])
                                selector = None if key["selector"] is None else tuple(key["selector"])
                                locator = self._coordinate_projection(frozen, alias, key["field"], selector)
                                if locator != (row["evidence_key"], row["request_key"]):
                                    continue
                                dates, source = self._coordinate_record(frozen, alias, key["field"], selector,
                                    db=db, max_bytes=options.max_buffer_bytes // 4, legacy=False, options=options)
                                if dates is None or source is None:
                                    raise RuntimeError("Coordinate projection's original payload is missing or unsupported")
                                retained_receipts[source.receipt_id] = source
                                verify_retained(source)
                        match_fields = {"name": original_request.name, "alias": original_request.alias} if isinstance(
                            original_request, ItemInput) else {"source": original_request.source, "dataset": original_request.dataset}
                        cursor = db.execute("SELECT rowid,length(evidence_key)+length(request_key)+length(source_receipt_id)+"
                            "length(source_receipt_digest)+length(alias) AS size FROM input_projection_index "
                            "WHERE " + " AND ".join(f"json_extract(request_key,'$.request.{field}') IS ?" for field in match_fields),
                            tuple(match_fields.values()))
                        while candidate := cursor.fetchone():
                                if check_canceled is not None:
                                    check_canceled()
                                if int(candidate["size"]) * 4 > options.max_buffer_bytes // 4:
                                    raise MemoryError("Scalar selection summary metadata audit exceeds max_buffer_bytes")
                                row = db.execute("SELECT evidence_key,request_key,source_receipt_id,source_receipt_digest,alias "
                                    "FROM input_projection_index WHERE rowid=?", (candidate["rowid"],)).fetchone()
                                key = json.loads(row["request_key"])
                                if key.get("projection") == _COORDINATE_PROJECTION:
                                    continue  # Its sole original date authority is followed above.
                                request = input_from_payload(key["request"])
                                fields = _selection_fields(key.get("fields"))
                                cutoff = None if key["cutoff"] is None else _date_value(key["cutoff"])
                                try:
                                    effective, effective_cutoff = self._selection_scope(frozen, alias, start=request.start, end=request.end,
                                        as_of=cutoff, view=request.view, strict=request.strict)
                                except ValueError:
                                    continue  # This root cannot consume a wider/future summary.
                                if (effective_cutoff != cutoff or _json(input_payload(effective)) != _json(key["request"]) or row["evidence_key"]
                                        != _scalar_selection_evidence(frozen, alias, effective, cutoff, fields=fields)):
                                    continue
                                source = self.get(row["source_receipt_id"])
                                if source.digest != row["source_receipt_digest"]:
                                    raise RuntimeError("Scalar selection summary's original source receipt changed")
                                proof = input_index.projection(db,row["evidence_key"],row["request_key"],max_bytes=options.max_buffer_bytes // 4)
                                if row["alias"] not in source.requests or (proof is None or proof.get("kind") != "segment_range.v1") and row["evidence_key"] != _scalar_selection_evidence(
                                        source, row["alias"], request, cutoff, fields=fields):
                                    raise RuntimeError("Scalar selection summary's original source binding changed")
                                retained_receipts[source.receipt_id] = source
                                verify_retained(source)
                                if key.get("projection") == "general_asset_append.v1":
                                    from .general_key_appends import references
                                    for dependency in references(self, proof):
                                        retained_receipts[dependency.receipt_id] = dependency
                                        verify_retained(dependency)
        with self._store.connect() as db:
            sets = db.execute("SELECT * FROM input_selection_sets WHERE source_id IN(SELECT value FROM json_each(?))", (_json(list(retained_receipts)),)).fetchall()
            for saved in sets:
                reference = {"id": saved["id"], "digest": saved["digest"],
                    "source": {"id": saved["source_id"], "digest": saved["source_digest"]},
                    "count": saved["item_count"]}
                root = db.execute("SELECT kind,item_count FROM immutable_metadata WHERE digest=?", (saved["root_ref"],)).fetchone()
                if root is None:
                    raise RuntimeError("Registered selection set membership is missing")
                immutable_metadata.MetadataReader(database=db).audit(
                    {"ref": saved["root_ref"], "kind": root["kind"], "count": root["item_count"]}, metadata_verified)
                membership = self._selection_membership(reference, config=options,
                    check_canceled=check_canceled or (lambda: None))
                _, reader, _, blocks = membership
                cursor = db.execute("SELECT e.descriptor_key,e.reference_json FROM input_selection_chunk_entries e "
                    "JOIN json_each(?) b ON e.block_ref=json_extract(b.value,'$.ref') "
                    "ORDER BY json_extract(b.value,'$.start'),e.ordinal", (_json(blocks),))
                count = 0
                while group := cursor.fetchmany(16):
                    for row in group:
                        reader.audit(json.loads(row["reference_json"]), metadata_verified)
                    # The fixed read view already validated this set's complete
                    # chunk inventory. Recheck every selected member without
                    # rebuilding that same inventory for each small group.
                    if context is not None and context.memo_available:
                        descriptors = self._read_selection_members(*membership, keys=[row[0] for row in group],
                            config=options, check_canceled=check_canceled or (lambda: None))
                    else:
                        descriptors = self.read_selection_set(reference, keys=[row[0] for row in group],
                            config=options, check_canceled=check_canceled or (lambda: None))
                    grouped = {}
                    for descriptor in descriptors:
                        grouped.setdefault(descriptor["alias"], []).append(descriptor)
                    for values in grouped.values():
                        resolved = self.resolve_selection_descriptors(saved["source_id"], values,
                            config=options, check_canceled=check_canceled or (lambda: None))
                        if list(resolved) != values:
                            raise RuntimeError("Registered selection set proof changed")
                    count += len(descriptors)
                if count != saved["item_count"]:
                    raise RuntimeError("Registered selection set membership is incomplete")
        completed = 0
        def report() -> None:
            if progress is not None:
                progress({"stage": "verify_inputs", "completed": completed, "total": len(batches_to_verify)})
        report()
        workers = min(options.workers, options.max_in_flight or options.workers)
        # One pool for this call, joined before any caller publishes output.
        # Chunk/decode buffers divide the explicit local allocation.
        workers = min(workers, max(1, options.max_buffer_bytes // (1024 * 1024)))
        buffer_bytes = max(1, options.max_buffer_bytes // workers)
        if workers == 1:
            for seq, path, expected in batches_to_verify:
                verify_batch(self._store, path, seq, expected, buffer_bytes=buffer_bytes, check_canceled=check_canceled)
                completed += 1
                report()
        else:
            pending = iter(batches_to_verify)
            with ThreadPoolExecutor(max_workers=workers) as executor:
                while group := [key for _, key in zip(range(workers), pending)]:
                    futures = [executor.submit(verify_batch, self._store, path, seq, expected,
                                               buffer_bytes=buffer_bytes, check_canceled=check_canceled) for seq, path, expected in group]
                    for future in futures:
                        future.result()
                        completed += 1
                        report()
        if check_canceled is not None:
            check_canceled()
        from bagelquant_data.storage.recovery import verify_visibility
        visibility_checked = set()
        for frozen in retained_receipts.values():
            for evidence in frozen.evidence.values():
                for event in evidence["record_checks"]:
                    if event.get("event") and event["content_hash"] not in visibility_checked:
                        verify_visibility(self._store,dict(event),buffer_bytes=options.max_buffer_bytes,
                            check_canceled=check_canceled)
                        visibility_checked.add(event["content_hash"])
        audited_bounds = set()
        for frozen in retained_receipts.values():
            for evidence in frozen.evidence.values():
                if not evidence.get("retained_batch_bounds"):
                    continue
                for batch in evidence["batches"]:
                    key = _json({name: batch.get(name) for name in ("commit_seq", "partition_path", "content_hash",
                        "min_available", "max_available", "min_observation", "max_observation")})
                    if key not in audited_bounds:
                        input_index.audit_captured_bounds(self._store, batch, max_bytes=options.max_buffer_bytes,
                            check_canceled=check_canceled)
                        audited_bounds.add(key)
        with self._store.connect() as db:
            if db.execute("SELECT 1 FROM sqlite_master WHERE name='input_coordinate_index'").fetchone() is not None:
                for frozen in retained_receipts.values():
                    axes = db.execute("SELECT alias,selection FROM input_coordinate_index "
                        "WHERE receipt_digest=?", (frozen.digest,)).fetchall()
                    for axis in axes:
                        if check_canceled is not None:
                            check_canceled()
                        field, encoded_selector = json.loads(axis[1])
                        selector = None if encoded_selector is None else tuple(encoded_selector)
                        stored = input_index.read_coordinates(db, frozen.digest, axis[0], field, selector)
                        original = self.read(frozen, axis[0], view="snapshot", fields=()).collect()
                        if original.estimated_size() > options.max_buffer_bytes // 2:
                            raise MemoryError("Coordinate index audit exceeds max_buffer_bytes")
                        if stored != input_index.coordinate_dates(original, field, selector):
                            raise RuntimeError("Coordinate index differs from original evidence")
            if input_index.available(db):
                for frozen in retained_receipts.values():
                    rows = db.execute("SELECT alias,request_key FROM input_selection_index "
                        "WHERE receipt_digest=? AND version=?", (frozen.digest, input_index.VERSION)).fetchall()
                    for row in rows:
                        if check_canceled is not None:
                            check_canceled()
                        key = json.loads(row[1])
                        saved = input_index.selection(db, frozen.digest, row[0], row[1])
                        expected_selection = self._indexed_selection(frozen, row[0],
                            request=input_from_payload(key["request"]),
                            cutoff=None if key["cutoff"] is None else _date_value(key["cutoff"]),
                            db=db, recompute=True, options=options)
                        if check_canceled is not None:
                            check_canceled()
                        if expected_selection is None:
                            raise MemoryError("Selection summary audit exceeds max_buffer_bytes")
                        if saved is None or input_index.selection_value(saved) != input_index.selection_value(expected_selection):
                            raise RuntimeError("Selection summary differs from original evidence")
            if db.execute("SELECT 1 FROM sqlite_master WHERE name='input_projection_index'").fetchone():
                for frozen in retained_receipts.values():
                    rows = db.execute("SELECT evidence_key,request_key,alias FROM input_projection_index "
                        "WHERE source_receipt_id=? AND source_receipt_digest=?",
                        (frozen.receipt_id, frozen.digest)).fetchall()
                    for row in rows:
                        if check_canceled is not None:
                            check_canceled()
                        key = json.loads(row["request_key"])
                        if key.get("projection") == "general_asset_append.v1":
                            from .general_key_appends import audit
                            saved = input_index.projection(db, row["evidence_key"], row["request_key"],
                                max_bytes=options.max_buffer_bytes // 2)
                            audit(self, db, row["evidence_key"], row["request_key"], saved,
                                options=options, check=check_canceled or (lambda: None))
                            continue
                        if key.get("projection") == _COORDINATE_PROJECTION:
                            selector = None if key["selector"] is None else tuple(key["selector"])
                            if self._coordinate_projection(frozen, row["alias"], key["field"], selector) != (
                                    row["evidence_key"], row["request_key"]):
                                raise RuntimeError("Coordinate projection's source binding differs from original evidence")
                            dates, source = self._coordinate_record(frozen, row["alias"], key["field"], selector,
                                db=db, max_bytes=options.max_buffer_bytes // 4, legacy=False, options=options)
                            if dates is None or source is None:
                                raise RuntimeError("Coordinate projection's original payload is missing or unsupported")
                            continue  # The sole original date payload was audited above.
                        request = input_from_payload(key["request"])
                        saved = input_index.projection(db,row["evidence_key"],row["request_key"],max_bytes=options.max_buffer_bytes // 2)
                        metadata_range = saved is not None and saved.get("kind") == "segment_range.v1"
                        cutoff = None if key["cutoff"] is None else _date_value(key["cutoff"])
                        causal = key.get("projection") in {"causal_value_versions.v1", "causal_fields_versions.v1"}
                        try:
                            fields = _selection_fields(key.get("fields"))
                            self._validate_selection_projection(frozen, row["alias"], request, fields=fields,
                                values_only=True, causal_versions=causal,
                                coordinate_selector=key.get("coordinates"))
                            self._validate_selection_fields(frozen, row["alias"], fields, request=request, cutoff=cutoff)
                        except (ValueError, KeyError) as error:
                            raise RuntimeError("Consumed selection summary fields differ from original evidence") from error
                        if ((request.view == "versions") != causal
                                or row["request_key"] != _selection_key(request, cutoff, values_only=True,
                                    causal_versions=causal, coordinates=key.get("coordinates"), fields=fields)
                                or "coordinates" in key and (causal or isinstance(request, RawInput) and fields is None)
                                or not metadata_range and row["evidence_key"] != _scalar_selection_evidence(frozen, row["alias"], request, cutoff, fields=fields)):
                            raise RuntimeError("Scalar selection summary's source binding differs from original evidence")
                        effective, effective_cutoff = self._selection_scope(frozen, row["alias"], start=request.start, end=request.end,
                            as_of=cutoff, view=request.view, strict=request.strict)
                        if effective_cutoff != cutoff or _json(input_payload(effective)) != _json(key["request"]):
                            raise RuntimeError("Scalar selection summary's request differs from original evidence")
                        saved = input_index.projection(db, row["evidence_key"], row["request_key"],
                            max_bytes=options.max_buffer_bytes // 2)
                        coordinates = None if "coordinates" not in key else input_index.read_selector(
                            db, key["coordinates"], max_bytes=options.max_buffer_bytes // 4, check_canceled=check_canceled)
                        try:
                            coordinates = self._selection_coordinates(request, coordinates, options=options, fields=fields)
                        except ValueError as error:
                            raise RuntimeError("Scalar selection summary coordinates differ from the original request") from error
                        expected = (self._range_selection(frozen,row["alias"],request,cutoff,coordinates=coordinates,
                            values_only=True,causal_versions=causal,fields=fields) if saved is not None and saved.get("kind") == "segment_range.v1"
                            else self._indexed_selection(frozen, row["alias"], request=request, cutoff=cutoff,
                                db=db, recompute=True, options=options, values_only=True, causal_versions=causal,
                                coordinates=coordinates, originals=True, check_canceled=check_canceled, fields=fields))
                        if expected is None or saved is None:
                            raise MemoryError("Scalar selection summary audit exceeds max_buffer_bytes")
                        if input_index.selection_value(saved) != input_index.selection_value(expected):
                            raise RuntimeError("Scalar selection summary differs from original evidence")
        if check_canceled is not None:
            check_canceled()
        if context is not None and context.memo_available:
            context.verified_batches.update(verified_batches)
            context.verified_graph.update(graph)
        if single:
            frozen = roots[0]
            return {"receipt_id": frozen.receipt_id, "valid": True, "batch_count": batch_count,
                    "upstream_receipt_count": len(verified) - 1, "digest": frozen.digest}
        return {"receipts": [{"receipt_id": value.receipt_id, "digest": value.digest} for value in roots],
                "valid": True, "batch_count": batch_count,
                "upstream_receipt_count": len(verified) - len({value.receipt_id for value in roots})}

    def _receipt(self, key: str, payload: Mapping[str, Any], digest: str) -> FrozenInputReceipt:
        evidence = payload["evidence"]
        if payload.get("format") == 9:
            reader = immutable_metadata.MetadataReader(self._store, memo=self._metadata_memo)
            evidence = immutable_metadata.FrozenEvidenceMapping(reader, evidence)
        return FrozenInputReceipt(key, int(payload["max_commit"]), int(payload["max_check_id"]), None if payload["information_cutoff"] is None else _date_value(payload["information_cutoff"]), {name: input_from_payload(value) for name, value in payload["requests"].items()}, evidence, digest, payload["dependency_digest"])

    @staticmethod
    def _select(frame: pl.DataFrame, request: DataInput, cutoff: date | None,
                *, evidence: Mapping[str, Any] | None = None) -> pl.DataFrame:
        view = request.view
        if view not in {"history", "snapshot", "versions"}:
            raise ValueError("Frozen input view must be history, snapshot, or versions")
        if view == "snapshot" and cutoff is None:
            raise ValueError("snapshot requests require an information cutoff")
        if request.strict and "_baseline" in frame.columns:
            frame = frame.filter(~pl.col("_baseline").fill_null(False))
        if "_snapshot_id" in frame.columns:
            if view != "versions" and evidence is not None:
                from bagelquant_data.items.pit import general_input_snapshot
                frame = general_input_snapshot(
                    frame, cutoff, snapshots=evidence["general_snapshots"],
                    checks=evidence["checks"], strict=request.strict,
                    include_historical_baseline=isinstance(request, RawInput)
                    and request.include_historical_baseline,
                )
            elif cutoff is not None:
                visible = pl.col("snapshot_date") <= cutoff
                if isinstance(request, RawInput) and request.include_historical_baseline and not request.strict:
                    visible = visible | pl.col("_baseline").fill_null(False)
                frame = frame.filter(visible)
            if view != "versions" and evidence is None and frame.height:
                frame = frame.filter(pl.col("_commit_seq") == frame["_commit_seq"].max())
                latest = frame.sort([name for name in ("snapshot_date", "ingested_at", "_attestation_id") if name in frame.columns]).tail(1)
                frame = frame.filter((pl.col("_commit_seq") == latest["_commit_seq"].item()) & (pl.col("snapshot_date") == latest["snapshot_date"].item()))
                if "_attestation_id" in frame.columns:
                    frame = frame.filter(pl.col("_attestation_id").fill_null(0) == (latest["_attestation_id"].item() or 0))
        elif "_record_id" in frame.columns:
            if cutoff is not None:
                frame = frame.filter(pl.col("time") <= cutoff)
            if view == "history":
                frame = frame.filter(pl.col("time") <= pl.col("source_time"))
            if view != "versions":
                frame = frame.sort("time", "ingested_at", "_commit_seq").unique("_record_id", keep="last", maintain_order=True)
        if isinstance(request, ItemInput):
            frame = frame.rename({"time": "version_available_date", "source_time": "time"})
            axis = "time"
        else:
            axis = "source_time" if "source_time" in frame.columns else "time"
            if request.observation_start is not None and axis in frame.columns:
                frame = frame.filter(pl.col(axis) >= _date_value(request.observation_start))
            if request.observation_end is not None and axis in frame.columns:
                frame = frame.filter(pl.col(axis) <= _date_value(request.observation_end))
        if request.start is not None and axis in frame.columns:
            frame = frame.filter(pl.col(axis) >= _date_value(request.start))
        if request.end is not None and axis in frame.columns:
            frame = frame.filter(pl.col(axis) <= _date_value(request.end))
        if isinstance(request, RawInput) and request.fields:
            missing = set(request.fields) - set(frame.columns)
            if missing:
                raise ValueError(f"Frozen input is missing requested fields: {sorted(missing)}")
            frame = frame.select(request.fields)
        sort = [name for name in (axis, "asset_id", "version_available_date", "_commit_seq") if name in frame.columns]
        return frame.sort(sort) if sort else frame


__all__ = ["InputsAPI", "FrozenInputReceipt", "InputSelectionScope", "InputReadBoundary", "input_read_boundary", "current_input_boundary", "current_input_check_boundary"]
