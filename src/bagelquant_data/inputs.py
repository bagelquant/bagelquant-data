"""Frozen neutral input receipts backed by central immutable ingestion batches."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, nullcontext
from contextvars import ContextVar
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime
from pathlib import Path
from threading import get_ident, RLock
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

import polars as pl
import pyarrow as pa

from bagelquant_data.core.schema import concat_compatible_frames
from bagelquant_data.core.types import DateLike
from bagelquant_data.items.types import DataInput, ItemInput, RawInput, input_from_payload, input_payload
from bagelquant_data.query.raw import _date_value
from bagelquant_data.storage.recovery import read_batch, verify_batch
from bagelquant_data.execution import ExecutionOptions

if TYPE_CHECKING:
    from bagelquant_data.management.lake import DataLake


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
    """Bound new readers in this context without opening or copying a database.

    Readers capture the value on construction so callers can explicitly pass
    them to worker threads. Nested contexts may only tighten an existing bound.
    """
    if max_commit < 0:
        raise ValueError("max_commit must be non-negative")
    resolved_data_meta_path = Path(data_meta_path).resolve()
    explicit_check_boundary = max_check_id is not None
    if max_check_id is None:
        from bagelquant_data.storage.data_meta import DataMetaStore
        store = DataMetaStore(data_meta_path=resolved_data_meta_path, read_only=True)
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
                      default=lambda value: dict(value) if isinstance(value, Mapping) else str(value))


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


@dataclass(slots=True)
class _ReadContext:
    path: Path
    thread: int
    receipts: dict[str, FrozenInputReceipt]
    check_canceled: Callable[[], None] | None
    options: ExecutionOptions
    verified_batches: set[tuple[int, str, str]] = field(default_factory=set)
    verified_graph: dict[str, tuple[int, tuple[FrozenInputReceipt, ...]]] = field(default_factory=dict)
    currentness: dict[tuple[str, str, Path, InputReadBoundary | None], bool] = field(default_factory=dict)
    selections: dict[tuple[str, str, str], dict | None] = field(default_factory=dict)
    request_identities: dict[tuple[str, str], str] = field(default_factory=dict)
    parent: _ReadContext | None = None
    closed: bool = False
    lock: Any = field(default_factory=RLock)


_READ_CONTEXTS: ContextVar[tuple[_ReadContext, ...]] = ContextVar("frozen_input_read_contexts", default=())


class InputsAPI:
    """Freeze contracts and exact registered immutable batch identities."""

    def __init__(self, lake: DataLake) -> None:
        self._lake = lake
        self._store = lake._data_meta
        self._boundary = _boundary_record(self._store.data_meta_path)
        self._shared_context: _ReadContext | None = None
        if not self._store.read_only:
            with self._store.connect() as db:
                db.execute("create table if not exists frozen_inputs(receipt_id text primary key,payload_json text not null,digest text not null,created_at text not null)")

    def _read_context(self) -> _ReadContext | None:
        path = self._store.data_meta_path.resolve()
        return next((value for value in reversed(_READ_CONTEXTS.get())
                     if value.path == path and value.thread == get_ident()), None)

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
        state = _ReadContext(self._store.data_meta_path.resolve(), get_ident(), {}, check_canceled, config or ExecutionOptions())
        if parent is not None:
            state.receipts, state.lock, state.parent = parent.receipts, parent.lock, parent
        previous_shared, self._shared_context = self._shared_context, state
        token = _READ_CONTEXTS.set((*_READ_CONTEXTS.get(), state))
        try:
            with self._store.read_view():
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
            state.selections.clear()
            state.request_identities.clear()

    def describe(self, receipt: FrozenInputReceipt | str) -> dict[str, Any]:
        """Describe registered identity without loading the recursive evidence tree."""
        key = receipt.receipt_id if isinstance(receipt, FrozenInputReceipt) else receipt
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
        context = self._read_context()
        key = (frozen.digest, alias)
        if context is not None and key in context.request_identities:
            return context.request_identities[key]
        payload = {"version": "frozen_request.v1",
                   "request": input_payload(frozen.requests[alias]),
                   "information_cutoff": frozen.information_cutoff,
                   "evidence": frozen.evidence[alias]}
        identity = hashlib.sha256(_json(payload).encode()).hexdigest()
        if context is not None:
            context.request_identities[key] = identity
        return identity

    def selection_identity(self, receipt: FrozenInputReceipt | str, alias: str, *,
                           start: DateLike | None = None, end: DateLike | None = None,
                           as_of: DateLike | None = None, view: str | None = None,
                           strict: bool | None = None) -> str | None:
        """Return an exact selected-content token from index records, or unknown.

        Root receipt IDs and unrelated aliases do not participate. This never
        reads original batches, writes an index or relaxes the captured bounds.
        """
        frozen = self.get(receipt)
        if alias not in frozen.requests:
            raise KeyError(f"Unknown frozen input alias: {alias}")
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
        value = self._indexed_selection(frozen, alias, request=request, cutoff=cutoff)
        return None if value is None else value["identity"]

    def _indexed_selection(self, frozen, alias, *, request=None, cutoff=None, db=None, recompute=False, options=None):
        from bagelquant_data import input_index
        request = frozen.requests[alias] if request is None else request
        cutoff = frozen.information_cutoff if cutoff is None else cutoff
        key = _json({"request": input_payload(request), "cutoff": cutoff})
        context = self._read_context()
        memo_key = (frozen.digest, alias, key)
        if not recompute and context is not None and memo_key in context.selections:
            return context.selections[memo_key]
        if db is None:
            with self._store.connect() as connection:
                return self._indexed_selection(frozen, alias, request=request, cutoff=cutoff, db=connection,
                    recompute=recompute, options=options)
        saved = None if recompute else input_index.selection(db, frozen.digest, alias, key)
        if saved is not None:
            return saved
        evidence = frozen.evidence[alias]
        pieces = []
        options = options or (context.options if context is not None else ExecutionOptions())
        buffered = 0
        for batch in evidence["batches"]:
            if context is not None and context.check_canceled is not None:
                context.check_canceled()
            lower, upper = request.start, request.end
            if isinstance(request, RawInput):
                lower = max((_date_value(item) for item in (lower, request.observation_start) if item is not None), default=None)
                upper = min((_date_value(item) for item in (upper, request.observation_end) if item is not None), default=None)
            frame = input_index.read(db, batch, start=lower, end=upper, cutoff=cutoff,
                                     max_bytes=max(0, options.max_buffer_bytes // 4 - buffered))
            if frame is None:
                return None
            # Attestation joins and concatenation also need headroom.
            buffered += frame.estimated_size() * 2
            if buffered > options.max_buffer_bytes // 2:
                # An incomplete index query never becomes a positive proof.
                return None
            if frame.width:
                pieces.append(frame)
        frame = concat_compatible_frames(pieces) if pieces else pl.DataFrame()
        witnesses = len(evidence["record_checks"]) + frame.height * len(evidence.get("full_commit_checks", ()))
        if witnesses * max(128, frame.estimated_size() // max(1, frame.height)) > options.max_buffer_bytes // 2:
            return None
        from bagelquant_data.query.raw import _attested_versions
        if frame.width:
            frame = _attested_versions(frame.lazy(), evidence["checks"], evidence["record_checks"],
                as_of_date=cutoff, max_check_id=frozen.max_check_id,
                full_commit_checks=evidence.get("full_commit_checks", ())).collect()
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
        # Empty full snapshots/range proofs and schema are part of identity.
        body = {"version": input_index.VERSION, "definition": evidence["definition_hash"],
            "schema": evidence["schema_hash"], "request": input_payload(request),
            "rows": input_index.identity(selected) if selected.height else "empty", "general": selected_general,
            "empty": None if empty is None else {k: v for k, v in empty.items()
                if k not in {"input_commit", "frozen_receipt_id"}},
            "empty_checks": evidence["empty_checks"] if not selected.height else []}
        value = {"identity": hashlib.sha256(_json(body).encode()).hexdigest(),
                 "parents": sorted(parents), "has_rows": bool(selected.height)}
        if context is not None:
            if len(context.selections) >= 4096:
                context.selections.pop(next(iter(context.selections)))
            context.selections[memo_key] = value
        return value

    def index_plan(self) -> dict[str, Any]:
        """Freeze an explicit historical index build; never read original bytes."""
        from bagelquant_data import input_index
        with self._store.connect() as db:
            batches = [dict(row) for row in db.execute("SELECT b.commit_seq,b.partition_path,b.content_hash "
                "FROM version_batches b JOIN version_commits c ON c.seq=b.commit_seq "
                "WHERE c.status='committed' ORDER BY b.commit_seq,b.partition_path")]
            receipts = [dict(row) for row in db.execute("SELECT receipt_id,digest FROM frozen_inputs ORDER BY receipt_id")]
        return {"version": input_index.VERSION, "data_meta_path": str(self._store.data_meta_path.resolve()),
                "batches": batches, "receipts": receipts}

    def build_index(self, plan: Mapping[str, Any], *, config: ExecutionOptions | None = None,
                    check_canceled: Callable[[], None] = lambda: None,
                    progress: Callable[[dict[str, Any]], None] = lambda _: None) -> dict[str, Any]:
        """Explicitly rebuild derived records from frozen original evidence.

        Each batch publishes atomically against its registered hash. Cancellation
        retains complete batches only; absent selection summaries remain unknown.
        """
        from bagelquant_data import input_index
        self._store.ensure_writable()
        if plan.get("version") != input_index.VERSION or plan.get("data_meta_path") != str(self._store.data_meta_path.resolve()):
            raise ValueError("Selection index plan belongs to another authority/version")
        with self._store.connect() as db:
            input_index.initialize(db)
        for index, batch in enumerate(plan["batches"]):
            check_canceled()
            frame = input_index.project_original(self._store, batch,
                max_bytes=(config or ExecutionOptions()).max_buffer_bytes, check_canceled=check_canceled)
            payload = input_index.encode(frame, batch["content_hash"])
            summary = input_index.bounds(frame)
            del frame
            with self._store.connect() as db:
                db.execute("BEGIN IMMEDIATE")
                check_canceled()
                row = db.execute("SELECT content_hash FROM version_batches WHERE commit_seq=? AND partition_path=?",
                    (batch["commit_seq"], batch["partition_path"])).fetchone()
                if row is None or row[0] != batch["content_hash"]:
                    raise RuntimeError("Selection index plan changed")
                input_index.save(db, batch["partition_path"], batch["commit_seq"], batch["content_hash"], payload, summary)
            progress({"completed": index + 1, "total": len(plan["batches"]), "stage": "dependency_index"})
        completed, unknown = 0, 0
        for item in plan["receipts"]:
            with self.read_context(item["receipt_id"], config=config, check_canceled=check_canceled):
                frozen = self.get(item["receipt_id"])
                if frozen.digest != item["digest"]:
                    raise RuntimeError("Selection index receipt changed")
                selections = []
                for alias, request in frozen.requests.items():
                    check_canceled()
                    value = self._indexed_selection(frozen, alias, recompute=True)
                    if value is None:
                        unknown += 1
                    else:
                        key = _json({"request": input_payload(request), "cutoff": frozen.information_cutoff})
                        selections.append((alias, key, value))
            # Publish one receipt's complete summaries at a time, after releasing
            # its captured read view; never hold all historical summaries in RAM.
            with self._store.connect() as db:
                db.execute("BEGIN IMMEDIATE")
                check_canceled()
                row = db.execute("SELECT digest FROM frozen_inputs WHERE receipt_id=?",
                    (item["receipt_id"],)).fetchone()
                if row is None or row[0] != item["digest"]:
                    raise RuntimeError("Selection index receipt changed")
                for alias, key, value in selections:
                    input_index.save_selection(db, frozen.digest, alias, key, value)
                    completed += 1
        return {"status": "partial" if unknown else "complete", "batches": len(plan["batches"]),
                "selections": completed, "unknown_selections": unknown}

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
            payload = self._capture(db, requests, cutoff=cutoff, max_commit=max_commit, persist_checks=True)
            serialized = _json(payload)
            if len(serialized.encode()) > db.getlimit(sqlite3.SQLITE_LIMIT_LENGTH):
                raise MemoryError("Frozen input evidence exceeds SQLite's single-value size limit")
            digest = hashlib.sha256(serialized.encode()).hexdigest()
            receipt_id = uuid4().hex
            db.execute("insert into frozen_inputs values(?,?,?,?)", (receipt_id, serialized, digest, datetime.now(UTC).isoformat()))
            from bagelquant_data import input_index
            if input_index.available(db):
                registered = self._receipt(receipt_id, payload, digest)
                for alias, request in registered.requests.items():
                    indexed = self._indexed_selection(registered, alias, db=db)
                    if indexed is not None:
                        key = _json({"request": input_payload(request), "cutoff": cutoff})
                        input_index.save_selection(db, digest, alias, key, indexed)
        return self._receipt(receipt_id, payload, digest)

    def _capture(self, db: sqlite3.Connection, requests: Mapping[str, DataInput], *,
                 cutoff: date | None, max_commit: int | None = None, persist_checks: bool = False,
                 compact_aliases: set[str] | None = None,
                 scoped_aliases: set[str] | None = None) -> dict[str, Any]:
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
            schema = db.execute("select schema_ipc,schema_hash from dataset_schemas where source=? and dataset=?", (source, dataset)).fetchone()
            batches = db.execute(
                "select b.commit_seq,b.partition_path,b.content_hash,b.row_count,c.mode,c.spec_hash,c.pit_date,c.input_receipt_id,c.baseline,"
                "b.min_available,b.max_available,b.min_observation,b.max_observation "
                "from version_batches b join version_commits c on c.seq=b.commit_seq "
                "where c.source=? and c.dataset=? and c.status='committed' and c.seq<=? order by b.commit_seq,b.partition_path",
                (source, dataset, boundary),
            ).fetchall()
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
                commits_sql = ",".join(str(value) for value in retained_commits) or "-1"
                def relevant_check(check: sqlite3.Row) -> bool:
                    if check["row_count"] == 0:
                        scopes = [query["item_range"] for query in json.loads(check["request_json"]) if "item_range" in query]
                        return not scopes or any(
                            (lower is None or _date_value(scope["end"]) >= lower)
                            and (upper is None or _date_value(scope["start"]) <= upper) for scope in scopes)
                    if check["visible_commit"] is None or check["visible_commit"] in retained_commits:
                        return True
                    return db.execute(f"select 1 from version_check_records where check_id=? and version_commit in ({commits_sql}) limit 1", (check["id"],)).fetchone() is not None
                audit_checks = [check for check in audit_checks if relevant_check(check)]
            from bagelquant_data.storage.full_commit_checks import full_commit_checks
            context = self._read_context()
            seals = full_commit_checks(self._store, db,
                [{key: value for key, value in dict(row).items() if key != "content_baseline"} for row in audit_checks],
                persist=persist_checks,
                max_buffer_bytes=None if context is None else context.options.max_buffer_bytes
                ) if compact_aliases is None or alias in compact_aliases else []
            sealed_ids = {seal["binding"]["check"]["id"] for seal in seals}
            excluded = ",".join(str(int(value)) for value in sealed_ids) or "-1"
            if scoped:
                proof_refs = {(row["commit_seq"], row["partition_path"], row["content_hash"])
                              for seal in seals for row in seal["binding"]["batches"]}
                selected_refs = {(row["commit_seq"], row["partition_path"], row["content_hash"]) for row in selected_batches} | proof_refs
                batches = [row for row in batches if (row["commit_seq"], row["partition_path"], row["content_hash"]) in selected_refs]
                commits_sql = ",".join(str(int(row["commit_seq"])) for row in batches) or "-1"
                checks_sql = ",".join(str(int(row["id"])) for row in audit_checks) or "-1"
                record_scope = f" and r.version_commit in ({commits_sql}) and v.id in ({checks_sql})"
            else:
                record_scope = ""
            if not scoped:
                bounds_fields = {"min_available", "max_available", "min_observation", "max_observation"}
                batches = [{key: value for key, value in dict(row).items() if key not in bounds_fields} for row in batches]
            inline_count = db.execute(
                "select count(*) from version_checks v cross join version_check_records r on v.id=r.check_id "
                "join version_commits c on c.seq=r.version_commit where v.source=? and v.dataset=? "
                "and v.id<=? and r.version_commit<=? and v.baseline=0 and c.baseline=1 and c.status='committed' "
                f"and v.id not in ({excluded}){record_scope}", (source, dataset, max_check_id, boundary)).fetchone()[0]
            inline_bytes += inline_count * 300 + len(_json(seals).encode())
            if inline_bytes > db.getlimit(sqlite3.SQLITE_LIMIT_LENGTH) // 2:
                raise MemoryError("Partial/mixed input witnesses exceed the frozen receipt size limit; use bounded input evidence")
            record_checks = db.execute(
                # Filter small headers before touching any dataset's record index.
                "select r.* from version_checks v cross join version_check_records r on v.id=r.check_id "
                "join version_commits c on c.seq=r.version_commit "
                "where v.source=? and v.dataset=? and v.id<=? and r.version_commit<=? "
                f"and v.baseline=0 and c.baseline=1 and c.status='committed' and v.id not in ({excluded}){record_scope} order by v.id,r.record_id",
                (source, dataset, max_check_id, boundary),
            ).fetchall()
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
                    "(select hex(schema_ipc) from version_batches where commit_seq=c.seq "
                    "order by partition_path limit 1) as schema_ipc "
                    "from version_commits c left join version_batches b on b.commit_seq=c.seq "
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
                parent = db.execute("select payload_json,digest from frozen_inputs where receipt_id=?", (parent_id,)).fetchone()
                if parent is None:
                    raise RuntimeError("Committed input receipt evidence is missing")
                if hashlib.sha256(str(parent["payload_json"]).encode()).hexdigest() != parent["digest"]:
                    raise RuntimeError("Committed input receipt checksum mismatch")
                parent_receipts[parent_id] = parent["digest"]
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
        semantic_evidence = {}
        for alias, evidence in payload["evidence"].items():
            semantic = {key: value for key, value in evidence.items()
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
                   config: ExecutionOptions | None = None) -> bool:
        """Compare current semantic evidence without creating a new receipt.

        The original request windows and information cutoff are retained.
        Revisions conservatively invalidate; pure verified rechecks do not.
        Item inputs follow the newest retained dependency proof at the cutoff,
        recursively checking upstream declarations and evidence in one read view.
        Archived inputs and changed declarations return false. Missing or corrupt
        frozen receipts fail explicitly; verify() separately checks retained bytes.
        A nonempty finite sequence checks every original root in the same read
        view. An entered read context additionally reuses completed currentness
        booleans for the same original digest and complete reader boundary only.
        """
        single = isinstance(receipt, (FrozenInputReceipt, str))
        roots = [self.get(receipt)] if single else [self.get(value) for value in receipt]
        if not roots:
            raise ValueError("Currentness requires at least one frozen input receipt")
        checked: dict[str, bool] = {}
        context = self._read_context()
        options = config or (context.options if context is not None else ExecutionOptions())
        memo_keys: dict[str, tuple[str, str, Path, InputReadBoundary | None]] = {}
        memo_path = self._store.data_meta_path.resolve()
        visiting: set[str] = set()
        with self._store.connect() as db:
            if not db.in_transaction:
                db.execute("begin")

            def current(value: FrozenInputReceipt) -> bool:
                if value.receipt_id in visiting:
                    raise RuntimeError("Frozen input receipt dependency cycle")
                if value.receipt_id in checked:
                    return checked[value.receipt_id]
                memo_key = (value.receipt_id, value.digest, memo_path, self._boundary)
                memo_keys[value.receipt_id] = memo_key
                if context is not None and memo_key in context.currentness:
                    checked[value.receipt_id] = context.currentness[memo_key]
                    return checked[value.receipt_id]
                visiting.add(value.receipt_id)
                # A newly added large verified baseline check changes semantic
                # evidence before any representation choice. Older receipts
                # cannot have captured this oversized inline witness set.
                from bagelquant_data.storage.full_commit_checks import COMPACT_MIN_ROWS
                for evidence in value.evidence.values():
                    if "full_commit_checks" in evidence or evidence.get("scoped_batches"):
                        continue
                    changed = db.execute(
                        "select 1 from version_checks v join version_commits c on c.seq=v.visible_commit "
                        "where v.source=? and v.dataset=? and v.id>? and v.baseline=0 and c.baseline=1 "
                        "and c.status='committed' and v.row_count>=? "
                        "and (? is null or v.id<=?) and (? is null or v.visible_commit<=?) limit 1",
                        (evidence["source"], evidence["dataset"], value.max_check_id, COMPACT_MIN_ROWS,
                         None if self._boundary is None else self._boundary.max_check_id,
                         None if self._boundary is None else self._boundary.max_check_id,
                         None if self._boundary is None else self._boundary.max_commit,
                         None if self._boundary is None else self._boundary.max_commit)).fetchone()
                    if changed is not None:
                        checked[value.receipt_id] = False
                        visiting.remove(value.receipt_id)
                        return False
                try:
                    payload = self._capture(db, value.requests, cutoff=value.information_cutoff,
                        max_commit=None if self._boundary is None else self._boundary.max_commit,
                        compact_aliases={alias for alias, evidence in value.evidence.items() if "full_commit_checks" in evidence},
                        scoped_aliases={alias for alias, evidence in value.evidence.items() if evidence.get("scoped_batches")})
                except KeyError:
                    checked[value.receipt_id] = False
                    visiting.remove(value.receipt_id)
                    return False
                result = payload["dependency_digest"] == value.dependency_digest
                if not result:
                    captured = self._receipt("current", payload, hashlib.sha256(_json(payload).encode()).hexdigest())
                    result = all(
                        (original := self._indexed_selection(value, alias)) is not None
                        and (latest := self._indexed_selection(captured, alias)) is not None
                        and original["identity"] == latest["identity"]
                        for alias in value.requests)
                if result:
                    for alias, request in value.requests.items():
                        if not isinstance(request, ItemInput):
                            continue
                        evidence = value.evidence[alias]
                        proof = evidence["empty_item_build"]
                        parent_ids = evidence["parent_receipts"]
                        # Capture includes every batch/check parent and the
                        # covering build proof. If all are current, every
                        # possible selected row or empty build is current.
                        # Any stale parent still needs exact cutoff selection;
                        # an unselected historical parent may be stale safely.
                        if proof is not None and proof["frozen_receipt_id"] in parent_ids:
                            if all(current(self.get(parent_id)) for parent_id in parent_ids):
                                continue
                        selected = self._selected_item_parents(value, alias, options)
                        if selected is None:
                            result = False
                            break
                        selected_rows, parents = selected
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
                return result

            # Visit every root even if another is stale; missing/corrupt/cyclic
            # evidence must not be hidden by a short-circuiting aggregate.
            results = [current(value) for value in roots]
            if context is not None:
                # Publish only after every original root completed successfully.
                # Bound operation-local bookkeeping; no frames/proofs persist.
                for receipt_id, result in checked.items():
                    key = memo_keys[receipt_id]
                    if key not in context.currentness and len(context.currentness) >= 4096:
                        context.currentness.pop(next(iter(context.currentness)))
                    context.currentness[key] = result
            return all(results)

    def _selected_item_parents(self, frozen: FrozenInputReceipt, alias: str,
                               options: ExecutionOptions) -> tuple[bool, set[str]] | None:
        """Resolve selected lineage from owner index records, never original IPC."""
        request = replace(frozen.requests[alias], view="snapshot")
        value = self._indexed_selection(frozen, alias, request=request, options=options)
        if value is None:
            return None
        return value["has_rows"], set(value["parents"])

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
            cached = context.receipts.get(key)
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
        result = self._receipt(key, json.loads(serialized), digest)
        if context is not None:
            result = replace(result, requests=_readonly(result.requests), evidence=_readonly(result.evidence))
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
        """Prove a visible baseline row; uncertainty requires ordinary selection.

        Uniform committed flags and entire coordinate containment establish
        a surviving row only when no visible witness can replace its timing.
        This positive-only proof never substitutes for original byte integrity.
        """
        request, evidence = frozen.requests[alias], frozen.evidence[alias]
        if (not evidence.get("scoped_batches") or evidence["general_snapshots"]
                or request.strict or request.view not in {"snapshot", "versions"}):
            return None
        cutoff = min(cutoff, frozen.information_cutoff) if frozen.information_cutoff is not None else cutoff
        commit_bound = min(frozen.max_commit, max_commit) if max_commit is not None else frozen.max_commit
        check_bound = min(frozen.max_check_id, max_check_id) if max_check_id is not None else frozen.max_check_id
        batches = [batch for batch in evidence["batches"] if batch["commit_seq"] <= commit_bound]
        if not batches or any(not batch["baseline"] for batch in batches):
            return None
        from bagelquant_data.storage.full_commit_checks import validate_seal
        checks = list(evidence["checks"])
        for seal in evidence.get("full_commit_checks", ()):
            validate_seal(seal)
            checks.append(seal["binding"]["check"])
        if any(not check["baseline"] and check["id"] <= check_bound
               and _date_value(check["pit_date"]) <= cutoff for check in checks):
            return None
        lower, upper = [request.start], [request.end]
        if isinstance(request, RawInput):
            lower.append(request.observation_start)
            upper.append(request.observation_end)
        for batch in batches:
            if batch.get("min_observation") is None or batch.get("max_observation") is None:
                return None
            first, last = _date_value(batch["min_observation"]), _date_value(batch["max_observation"])
            if (first > last or any(value is not None and first < _date_value(value) for value in lower)
                    or any(value is not None and last > _date_value(value) for value in upper)):
                return None
        if any(batch["row_count"] > 0 and batch.get("min_available") is not None
               and _date_value(batch["min_available"]) <= cutoff for batch in batches):
            return True
        return None

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
            with self._store.connect() as db:
                for batch in evidence["batches"]:
                    if context is not None and context.check_canceled is not None:
                        context.check_canceled()
                    projected = input_index.read(db, batch, cutoff=cutoff,
                        max_bytes=max(0, budget // 2 - buffered))
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
                    full_commit_checks=evidence.get("full_commit_checks", ())).collect()
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
    ) -> pl.LazyFrame:
        """Replay frozen evidence with an optional tighter information cutoff.

        View and projection overrides retain the original observation window,
        definition, commit boundary and check boundary after object archival.
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
        if isinstance(request, RawInput):
            request = replace(request, fields=())
        frame = self._select(self._read_frame(frozen, alias, request=request), request, cutoff,
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

        A metadata-only planning query; never consult live batch summaries or
        infer bounds for legacy/general inputs. Unknown batch bounds return false.
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
        return all(
            bool(evidence.get("scoped_batches"))
            and batch.get("min_observation") is not None and batch.get("max_observation") is not None
            or (batch["commit_seq"], batch["partition_path"], batch["content_hash"]) in sealed
            for batch in evidence["batches"])

    def _read_frame(self, frozen: FrozenInputReceipt, alias: str, *,
                    timing_cutoff: date | None = None, request: DataInput | None = None) -> pl.DataFrame:
        """Load exact retained versions before view or field selection."""
        if alias not in frozen.requests:
            raise KeyError(f"Unknown frozen input alias: {alias}")
        request = frozen.requests[alias] if request is None else request
        evidence = frozen.evidence[alias]
        # Timing-only selection cannot observe future physical availability.
        # General snapshots and unknown/legacy bounds keep their broad reads.
        effective_cutoff = frozen.information_cutoff
        if timing_cutoff is not None:
            effective_cutoff = timing_cutoff if effective_cutoff is None else min(timing_cutoff, effective_cutoff)
        from bagelquant_data.storage.full_commit_checks import validate_seal
        bounds = {}
        for seal in evidence.get("full_commit_checks", ()):
            validate_seal(seal)
            for batch in seal["batch_bounds"]:
                bounds[(batch["commit_seq"], batch["partition_path"], batch["content_hash"])] = batch
        def needed(batch: Mapping[str, Any]) -> bool:
            registered = batch if evidence.get("scoped_batches") else None
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
        context = self._read_context()
        pieces = [read_batch(self._store, value["partition_path"], int(value["commit_seq"]), value["content_hash"],
                            check_canceled=None if context is None else context.check_canceled)
                  for value in evidence["batches"] if needed(value)]
        if pieces:
            frame = concat_compatible_frames(pieces)
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
                                   full_commit_checks=evidence.get("full_commit_checks", ())).collect()
        return frame

    def verify(self, receipt: FrozenInputReceipt | str | Sequence[FrozenInputReceipt | str], *,
               config: ExecutionOptions | None = None,
               check_canceled: Callable[[], None] | None = None,
               progress: Callable[[dict[str, Any]], None] | None = None) -> dict[str, Any]:
        """Raise on lost or corrupt evidence, even after object archival."""
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
            if value.receipt_id in verified:
                return
            visiting.add(value.receipt_id)
            prior = None if context is None else context.verified_graph.get(value.receipt_id)
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
                for seal in evidence.get("full_commit_checks", ()):
                    validate_seal(seal)
                    retained = {(batch["commit_seq"], batch["partition_path"], batch["content_hash"], batch["row_count"]) for batch in evidence["batches"]}
                    if any((batch["commit_seq"], batch["partition_path"], batch["content_hash"], batch["row_count"]) not in retained for batch in seal["binding"]["batches"]):
                        raise RuntimeError("Full-commit check seal retained batch mismatch")
                for batch in evidence["batches"]:
                    if check_canceled is not None:
                        check_canceled()
                    key = (int(batch["commit_seq"]), batch["partition_path"], batch["content_hash"])
                    if key not in verified_batches:
                        if context is None or key not in context.verified_batches:
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
        completed = 0
        def report() -> None:
            if progress is not None:
                progress({"stage": "verify_inputs", "completed": completed, "total": len(batches_to_verify)})
        report()
        options = config or (context.options if context is not None else ExecutionOptions())
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
        # Derived records are audited against their original owner evidence.
        from bagelquant_data import input_index
        for seq, path, expected in verified_batches:
            input_index.audit(self._store, {"commit_seq": seq, "partition_path": path,
                "content_hash": expected}, max_bytes=options.max_buffer_bytes,
                check_canceled=check_canceled)
        with self._store.connect() as db:
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
                        if saved != expected_selection:
                            raise RuntimeError("Selection summary differs from original evidence")
        if check_canceled is not None:
            check_canceled()
        if context is not None:
            context.verified_batches.update(verified_batches)
            context.verified_graph.update(graph)
        if single:
            frozen = roots[0]
            return {"receipt_id": frozen.receipt_id, "valid": True, "batch_count": batch_count,
                    "upstream_receipt_count": len(verified) - 1, "digest": frozen.digest}
        return {"receipts": [{"receipt_id": value.receipt_id, "digest": value.digest} for value in roots],
                "valid": True, "batch_count": batch_count,
                "upstream_receipt_count": len(verified) - len({value.receipt_id for value in roots})}

    @staticmethod
    def _receipt(key: str, payload: Mapping[str, Any], digest: str) -> FrozenInputReceipt:
        return FrozenInputReceipt(key, int(payload["max_commit"]), int(payload["max_check_id"]), None if payload["information_cutoff"] is None else _date_value(payload["information_cutoff"]), {name: input_from_payload(value) for name, value in payload["requests"].items()}, payload["evidence"], digest, payload["dependency_digest"])

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


__all__ = ["InputsAPI", "FrozenInputReceipt", "InputReadBoundary", "input_read_boundary", "current_input_boundary", "current_input_check_boundary"]
