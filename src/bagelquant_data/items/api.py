"""DataItem management on the shared Raw version and persistence engine."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from contextlib import closing, contextmanager
from concurrent.futures import CancelledError, ThreadPoolExecutor
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any, cast, overload
from threading import Lock, get_ident
from uuid import uuid4

import polars as pl

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.types import DateLike
from bagelquant_data.execution import ExecutionOptions
from bagelquant_data.pipeline.versions import VERSION_FIELDS, commit_versions
from bagelquant_data.query.raw import RawQueryService, _date_value
from bagelquant_data.transforms import apply_transforms, scalar_dtype

from .types import BuildContext, DataItemSpec, ItemBuildReport, ItemInput, ItemPublication, Producer, RawInput, spec_from_payload, spec_payload
from .pit import _same_ordered_rows, general_input_snapshot, iter_computed_versions

if TYPE_CHECKING:
    from bagelquant_data.management.lake import DataLake
    from bagelquant_data.inputs import FrozenInputReceipt


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _baseline_publication_groups(frame: pl.DataFrame, rows: int, byte_limit: int):
    """Keep independent baseline coordinates in causal, bounded batches."""
    for group in frame.sort("version_available_date", "time", "asset_id").iter_slices(rows):
        pending = [group]
        while pending:
            part = pending.pop()
            if part.height > 1 and part.estimated_size() > byte_limit:
                middle = part.height // 2
                pending.extend((part.slice(middle), part.head(middle)))
                continue
            yield _date_value(cast(DateLike, part["version_available_date"].max())), part


@dataclass(slots=True)
class _PublicationVerification:
    owner: ItemAPI
    receipt_id: str
    digest: str
    active: bool = True
    thread_id: int = field(default_factory=get_ident)
    timing_cache_entries: int = 0
    timing_buffer_bytes: int | None = None
    baseline_dates: dict[date, bool] = field(default_factory=dict)


class ItemPublisher:
    """Serial output groups in one verified immutable-input operation."""

    def __init__(self, owner: ItemAPI, receipt: FrozenInputReceipt | None,
                 verification: _PublicationVerification,
                 cancelled: Callable[[], bool] | None) -> None:
        self._owner, self._receipt = owner, receipt
        self._verification, self._cancelled = verification, cancelled
        self._lock = Lock()

    @property
    def lake(self) -> DataLake:
        return self._owner._lake

    @property
    def input_receipt(self) -> FrozenInputReceipt | None:
        return self._receipt

    @property
    def active(self) -> bool:
        return self._verification.active

    def publish(self, publications: Sequence[ItemPublication]) -> dict[str, ItemBuildReport | None]:
        if not self._verification.active:
            raise RuntimeError("DataItem publication operation has closed")
        if get_ident() != self._verification.thread_id:
            raise RuntimeError("DataItem publication requires its owning thread")
        if not self._lock.acquire(blocking=False):
            raise RuntimeError("DataItem publication groups must be serial")
        try:
            return self._owner._publish(publications, input_receipt=self._receipt,
                cancelled=self._cancelled, _verification=self._verification)
        except BaseException:
            self._verification.active = False
            self._verification.baseline_dates.clear()
            raise
        finally:
            self._lock.release()


class ItemAPI:
    """Typed item definitions, dependency builds and causal long-table reads."""

    def __init__(self, lake: DataLake) -> None:
        self._lake = lake
        self._store = lake._data_meta
        self._parquet = lake._parquet
        self._query = RawQueryService(self._parquet, self._store)
        from bagelquant_data.inputs import current_input_boundary, current_input_check_boundary
        self._max_commit, self._as_of = current_input_boundary(self._store.data_meta_path)
        self._max_check_id = current_input_check_boundary(self._store.data_meta_path)
        self._producers: dict[tuple[str, str], Producer] = {}
        if not self._store.read_only:
            with self._store.connect() as db:
                db.executescript("""
                    create table if not exists item_definitions (
                        name text primary key, spec_json text not null,
                        spec_hash text not null, active integer not null default 1
                    );
                    create table if not exists item_dependencies (
                        item_name text not null, kind text not null,
                        source text not null, dataset text not null,
                        primary key(item_name,kind,source,dataset)
                    );
                    create table if not exists item_builds (
                        id integer primary key, name text not null,
                        definition_hash text not null, dependency_digest text not null,
                        start_date text not null, end_date text not null,
                        input_commit integer not null, result_commit integer,
                        status text not null, created_at text not null,
                        frozen_receipt_id text not null
                    );
                    create table if not exists item_initializations (
                        name text primary key, definition_hash text not null,
                        start_date text not null, end_date text not null,
                        status text not null
                    );
                """)

    def register(self, spec: DataItemSpec, *, category_id: str | None = None) -> None:
        with self._store.dataset_writer("items", spec.name, uuid4().hex):
            self._register(spec, category_id=category_id)

    def _register(self, spec: DataItemSpec, *, category_id: str | None = None) -> None:
        self._store.ensure_writable()
        if not isinstance(spec, DataItemSpec):
            raise TypeError("spec must be a DataItemSpec")
        previous = self._store._rows("select spec_json from item_definitions where name=?", (spec.name,))
        if previous and self._store.manifest("items", spec.name) and spec_from_payload(json.loads(previous[0]["spec_json"])).value_dtype != spec.value_dtype:
            raise ValueError("A committed DataItem's scalar type is immutable; register a new item identity")
        payload = _json(spec_payload(spec))
        digest = hashlib.sha256(payload.encode()).hexdigest()
        physical = self._physical_spec(spec)
        self._store.upsert_source("items", "data_items", configured=True)
        with self._store.connect() as db:
            if not db.in_transaction:
                db.execute("begin immediate")
            # Verify dependencies and cycles against the prospective graph before
            # changing either the item definition or the physical dataset contract.
            for dependency in spec.inputs:
                if isinstance(dependency, ItemInput):
                    if dependency.name == spec.name or self._reaches(dependency.name, spec.name):
                        raise ValueError("DataItem dependency cycle")
                    self.get(dependency.name)
                elif self._store.get_dataset(dependency.source, dependency.dataset) is None:
                    raise ValueError(f"Unknown Raw dependency: {dependency.source}/{dependency.dataset}")
            db.execute(
                "insert into item_definitions values(?,?,?,1) "
                "on conflict(name) do update set spec_json=excluded.spec_json,spec_hash=excluded.spec_hash,active=1",
                (spec.name, payload, digest),
            )
            db.execute("delete from item_dependencies where item_name=?", (spec.name,))
            db.executemany(
                "insert into item_dependencies values(?,?,?,?)",
                [(spec.name, "raw", value.source, value.dataset) if isinstance(value, RawInput)
                 else (spec.name, "item", "items", value.name) for value in spec.inputs],
            )
            self._store._write_dataset(db, physical)
        if category_id is not None:
            self._lake.catalog.item_categories.assign(spec.name, category_id)

    def register_producer(self, key: str, revision: str, producer: Producer) -> None:
        """Register runtime code matching an explicit immutable declaration."""
        self._store.ensure_writable()
        if not key or not revision or not callable(producer):
            raise ValueError("producer requires a non-empty key/revision and a callable")
        self._producers[(key, revision)] = producer

    def list(self, *, include_inactive: bool = False) -> list[DataItemSpec]:
        rows = self._store._rows("select spec_json from item_definitions " + ("" if include_inactive else "where active=1 ") + "order by name")
        return [spec_from_payload(json.loads(row["spec_json"])) for row in rows]

    def get(self, name: str, *, include_inactive: bool = False) -> DataItemSpec:
        rows = self._store._rows("select spec_json,active from item_definitions where name=?", (name,))
        if not rows or (not include_inactive and not rows[0]["active"]):
            raise KeyError(f"Unknown active DataItem: {name}")
        return spec_from_payload(json.loads(rows[0]["spec_json"]))

    def remove(self, name: str) -> None:
        """Archive an item while retaining committed and frozen evidence."""
        self._store.ensure_writable()
        self.get(name)
        with self._store.dataset_writer("items", name, uuid4().hex), self._store.connect() as db:
            db.execute("begin immediate")
            dependants = self._store._rows(
                "select d.item_name from item_dependencies d join item_definitions i on i.name=d.item_name "
                "where d.kind='item' and d.dataset=? and i.active=1 order by d.item_name", (name,),
            )
            if dependants:
                raise ValueError(f"DataItem has active dependencies: {[value['item_name'] for value in dependants]}")
            db.execute("update item_definitions set active=0 where name=?", (name,))
            db.execute("delete from catalog_assignments where kind='item' and object_key=?", (name,))
            db.execute("update datasets set active=0,enabled=0 where source='items' and name=?", (name,))

    def plan_update(self, name: str, *, end: DateLike, start: DateLike = "2000-01-01") -> list[dict[str, str]]:
        """Plan initialization/resume/incremental work from committed evidence, including empties."""
        from bagelquant_data.pipeline.planning import initialization_actions
        definition_hash = hashlib.sha256(_json(spec_payload(self.get(name))).encode()).hexdigest()
        rows = self._store._rows("select * from item_initializations where name=?", (name,))
        row = rows[0] if rows else None
        initialization = None if row is None else {"status": row["status"], "start": row["start_date"],
            "end": row["end_date"], "definition_hash": row["definition_hash"]}
        history = bool(self._store._rows("select 1 from version_commits where source='items' and dataset=? and status='committed' limit 1", (name,)))
        history = history or bool(self._store._rows("select 1 from item_builds where name=? and status='success' limit 1", (name,)))
        return initialization_actions(start=_date_value(start), end=_date_value(end),
            definition_hash=definition_hash, initialization=initialization, has_history=history)

    def initialize(self, name: str, *, end: DateLike, start: DateLike = "2000-01-01", config: ExecutionOptions | None = None,
                   progress: Callable[[ItemBuildReport], None] | None = None, cancelled: Callable[[], bool] | None = None) -> ItemBuildReport:
        self._store.ensure_writable()
        first, last = _date_value(start), _date_value(end)
        if last < first:
            raise ValueError("end precedes start")
        definition_hash = hashlib.sha256(_json(spec_payload(self.get(name))).encode()).hexdigest()
        with self._store.connect() as db:
            row = db.execute("select * from item_initializations where name=?", (name,)).fetchone()
            if row and (row["definition_hash"] != definition_hash or row["start_date"] != first.isoformat() or row["end_date"] != last.isoformat()):
                raise ValueError("Initialization range and definition are fixed; use update after completion")
            if row is None:
                if db.execute("select 1 from version_commits where source='items' and dataset=? and status='committed' limit 1", (name,)).fetchone():
                    raise ValueError("Historical initialization requires a new DataItem")
                db.execute("insert into item_initializations values(?,?,?,?,'running')", (name, definition_hash, first.isoformat(), last.isoformat()))
            elif row["status"] == "complete":
                previous = db.execute("select * from item_builds where name=? and definition_hash=? and start_date=? and end_date=? and status='success' order by id desc limit 1", (name, definition_hash, first.isoformat(), last.isoformat())).fetchone()
                if previous is None:
                    raise RuntimeError("Completed initialization has no committed build evidence")
                return ItemBuildReport(name, "unchanged", 0, previous["result_commit"], previous["input_commit"], first, last, previous["dependency_digest"], previous["frozen_receipt_id"])
        report = self._build(name, start=first, end=last, initialize=True, config=config, stack=(), progress=progress, cancelled=cancelled)
        if report.status in {"success", "unchanged"}:
            with self._store.connect() as db:
                db.execute("update item_initializations set status='complete' where name=?", (name,))
        return report

    def update(self, name: str, *, end: DateLike, start: DateLike | None = None, config: ExecutionOptions | None = None,
               progress: Callable[[ItemBuildReport], None] | None = None, cancelled: Callable[[], bool] | None = None,
               force: bool = False,
               input_windows: Mapping[str, tuple[DateLike, DateLike]] | None = None) -> ItemBuildReport:
        """Conservatively recompute the persisted materialized range.

        No unverified lookback or producer formula is used to infer how far a
        historical revision propagates. Dependency identity controls reuse.
        Explicit input_windows are caller-declared dependency observation
        windows, pinned in the actual frozen requests without changing the
        stored DataItem definition. Omitted dependencies retain their scope.
        """
        rows = self._store._rows("select min(start_date) as start_date from item_builds where name=? and status='success'", (name,))
        first = start or (rows[0]["start_date"] if rows and rows[0]["start_date"] else "2000-01-01")
        return self._build(name, start=_date_value(first), end=_date_value(end), initialize=False, config=config, stack=(), progress=progress, cancelled=cancelled, force=force, input_windows=input_windows)

    def ingest(
        self, name: str, frame: pl.DataFrame, *, available_date: DateLike | None = None,
        historical_baseline: bool = False, ingested_at: datetime | None = None,
        input_receipt: FrozenInputReceipt | str | None = None,
        expected_definition_hash: str | None = None,
    ) -> ItemBuildReport:
        """Publish neutral producer output through the shared immutable engine."""
        run_id = uuid4().hex
        with self._store.dataset_writer("items", name, run_id):
            return self._ingest(name, frame, available_date=available_date,
                historical_baseline=historical_baseline, ingested_at=ingested_at,
                input_receipt=input_receipt, expected_definition_hash=expected_definition_hash,
                run_id=run_id)

    def _ingest(self, name: str, frame: pl.DataFrame, *, available_date: DateLike | None,
                historical_baseline: bool, ingested_at: datetime | None,
                input_receipt: FrozenInputReceipt | str | None,
                expected_definition_hash: str | None, run_id: str,
                _baseline_proven: bool = False,
                _declared_range: tuple[date, date] | None = None,
                writer_executor: ThreadPoolExecutor | None = None,
                partition_workers: int = 1,
                writer_buffer_bytes: int = 0,
                commit_batch_rows: int | None = None,
                commit_batch_bytes: int | None = None,
                cancelled: Callable[[], bool] | None = None,
                on_commit: Callable[[int, int | None], None] | None = None,
                _verification: _PublicationVerification | None = None) -> ItemBuildReport:
        self._store.ensure_writable()
        spec = self.get(name)
        explicit_availability = "available_date" in frame.columns or "version_available_date" in frame.columns
        frame = self._normalize(spec, frame)
        definition_hash = hashlib.sha256(_json(spec_payload(spec)).encode()).hexdigest()
        if expected_definition_hash is not None and definition_hash != expected_definition_hash:
            raise RuntimeError("DataItem definition changed while computing producer output")
        receipt = None if input_receipt is None else self._lake.inputs.get(input_receipt)
        if spec.inputs and receipt is None:
            raise ValueError("DataItem dependency publication requires an input_receipt")
        if receipt is not None:
            def identity(value):
                return ("raw", value.source, value.dataset) if isinstance(value, RawInput) else ("item", value.name)
            if {identity(value) for value in spec.inputs} != {identity(value) for value in receipt.requests.values()}:
                raise ValueError("Frozen input receipt does not match declared DataItem dependencies")
            if _verification is not None and not (_verification.active and _verification.owner is self
                      and _verification.receipt_id == receipt.receipt_id
                      and _verification.digest == receipt.digest):
                raise RuntimeError("Publication input verification no longer matches this operation")
        now = ingested_at or datetime.now(UTC)
        if now.tzinfo is None:
            raise ValueError("ingested_at must be timezone-aware")
        frame = frame.with_columns(
            pl.lit(definition_hash).alias("_item_definition_hash"),
            pl.lit(spec.producer_key, dtype=pl.String).alias("_producer_key"),
            pl.lit(spec.producer_revision, dtype=pl.String).alias("_producer_revision"),
            pl.lit(None if receipt is None else receipt.dependency_digest, dtype=pl.String).alias("_dependency_digest"),
        )
        if available_date is not None:
            frame = frame.with_columns(pl.max_horizontal("time", pl.lit(_date_value(available_date))).alias("version_available_date"))
        elif "version_available_date" not in frame.columns:
            frame = frame.with_columns(pl.max_horizontal("time", "available_date", *([] if historical_baseline or explicit_availability else [pl.lit(now.astimezone(UTC).date())])).alias("version_available_date"))
        if frame.select((pl.col("version_available_date") < pl.max_horizontal("time", "available_date")).any()).item():
            raise ValueError("version availability precedes the observation or declared availability")
        publication_dates = sorted(frame["version_available_date"].unique().to_list())
        if not publication_dates:
            publication_dates = [_date_value(available_date) if available_date is not None else now.astimezone(UTC).date()]
        baseline_by_date = {cutoff: historical_baseline for cutoff in publication_dates}
        if receipt is not None and not historical_baseline and not _baseline_proven:
            cutoffs = {day: min(day, receipt.information_cutoff) if receipt.information_cutoff is not None else day
                       for day in publication_dates}
            cache = {} if _verification is None else _verification.baseline_dates
            unresolved = [day for day in publication_dates if cutoffs[day] not in cache]
            for day in publication_dates:
                if cutoffs[day] in cache:
                    baseline_by_date[day] = cache[cutoffs[day]]
            # A caller cannot promote unknown input timing by omitting the
            # output baseline flag. Retained original baselines cease to
            # taint a later version once exact input attestations are visible.
            for alias in receipt.requests:
                pending = [day for day in unresolved if not baseline_by_date[day]]
                if not pending:
                    break
                # Every immutable commit records the timing flag written on
                # all its rows; empty general identities carry the same proof.
                # Selection and attestations can only remove a baseline, never
                # create one. Verified inputs need no repeated prefix replay.
                if not self._lake.inputs._could_have_baseline(receipt, alias):
                    continue
                for day in pending:
                    if self._lake.inputs._proven_baseline_at(receipt, alias, cutoffs[day]) is True:
                        baseline_by_date[day] = True
                pending = [day for day in pending if not baseline_by_date[day]]
                if not pending:
                    continue
                for publication_date in pending:
                    cutoff = cutoffs[publication_date]
                    if self._lake.inputs._baseline_at(receipt, alias, cutoff,
                            max_buffer_bytes=None if _verification is None else _verification.timing_buffer_bytes):
                        baseline_by_date[publication_date] = True
            # Only complete OR results enter this operation's fixed-receipt memo.
            # Dates are independent: later attestations can change True to False.
            if _verification is not None and _verification.timing_cache_entries:
                for day in unresolved:
                    key = cutoffs[day]
                    if key not in cache and len(cache) >= _verification.timing_cache_entries:
                        cache.pop(next(iter(cache)))
                    cache[key] = baseline_by_date[day]
        if frame.select("time", "asset_id", "version_available_date").is_duplicated().any():
            raise ValueError("DataItem contains duplicate version keys")
        first = _date_value(cast(DateLike, frame["time"].min())) if frame.height else date(2000, 1, 1)
        last = _date_value(cast(DateLike, frame["time"].max())) if frame.height else first
        if _declared_range is not None:
            first, last = _declared_range
        physical = self._physical_spec(spec)
        # Generic Raw storage uses source_time for the stable observation key and
        # time for version availability. Public DataItem reads restore the axis.
        # A first panel with distinct coordinates has no revisions or attestations
        # to sequence. One atomic commit preserves its row availability dates and
        # writes each month once. Existing content, repeated coordinates and mixed
        # timing proof retain chronological publications below.
        has_committed = bool(self._store._rows(
            "select 1 from version_commits where source='items' and dataset=? "
            "and status='committed' limit 1", (name,),
        ))
        initial_bulk = (
            frame.height > 0 and len(publication_dates) > 1
            and len(set(baseline_by_date.values())) == 1
            and not frame.select("time", "asset_id").is_duplicated().any()
            and not has_committed
        )
        if initial_bulk:
            publication_dates = [publication_dates[-1]]
        # Resuming an internally evaluated unverified baseline need not rewrite
        # a month for every day. Each coordinate has one causal version, and
        # baseline checks cannot promote strict visibility. Verified no-op
        # witnesses retain date-by-date checks (their check date is causal).
        resume_bulk = (frame.height > 0 and has_committed and _baseline_proven and historical_baseline
                       and commit_batch_rows is not None and commit_batch_bytes is not None
                       and len(set(baseline_by_date.values())) == 1
                       and not frame.select("time", "asset_id").is_duplicated().any())
        # Only first-publication deltas have bounded old-partition memory (zero).
        # Use the still-unused previous-revision reserve for preparation, with
        # a conservative allowance for sort/hash/Arrow/compressed copies.
        admitted_writers = 1
        if initial_bulk and partition_workers > 1 and writer_buffer_bytes:
            parts = frame.with_columns(pl.col("version_available_date").dt.strftime("%Y-%m").alias("_writer_month")).partition_by("_writer_month")
            largest = max(int(part.estimated_size()) for part in parts)
            admitted_writers = max(1, min(partition_workers, len(parts), writer_buffer_bytes // max(1, 6 * largest)))
            del parts
        rows_committed = 0
        groups = _baseline_publication_groups(frame, cast(int, commit_batch_rows), cast(int, commit_batch_bytes)) if resume_bulk else (
                      (day, frame if initial_bulk else frame.filter(pl.col("version_available_date") == day))
                      for day in publication_dates)
        for publication_date, group in groups:
            if cancelled and cancelled():
                raise CancelledError("DataItem publication cancelled")
            delta = group.rename({"time": "source_time", "version_available_date": "time"})
            result = commit_versions(
                physical, delta, self._parquet, run_id=run_id,
                mode="incremental", ingested_at=ingested_at,
                available_date=publication_date, preserve_available=True,
                historical_baseline=baseline_by_date[publication_date],
                input_receipt_id=None if receipt is None else receipt.receipt_id,
                requests=None if _declared_range is None else [{"item_range": {"start": first.isoformat(), "end": last.isoformat()}}],
                writer_executor=writer_executor if admitted_writers > 1 else None,
                partition_workers=admitted_writers,
            )
            rows_committed += result.rows_committed
            if on_commit:
                latest = self._store._rows("select max(seq) as seq from version_commits where source='items' and dataset=? and status='committed'", (name,))[0]["seq"]
                on_commit(rows_committed, latest)
        if cancelled and cancelled():
            raise CancelledError("DataItem publication cancelled")
        commits = self._store._rows("select max(seq) as seq from version_commits where source='items' and dataset=? and status='committed'", (name,))
        seq = commits[0]["seq"] if commits else None
        if receipt is not None:
            with self._store.connect() as db:
                db.execute("insert into item_builds(name,definition_hash,dependency_digest,start_date,end_date,input_commit,result_commit,status,created_at,frozen_receipt_id) values(?,?,?,?,?,?,?,'success',?,?)", (name, definition_hash, receipt.dependency_digest, first.isoformat(), last.isoformat(), receipt.max_commit, seq, now.isoformat(), receipt.receipt_id))
        return ItemBuildReport(name, "success", rows_committed, seq, 0 if receipt is None else receipt.max_commit, first, last, "" if receipt is None else receipt.dependency_digest, "" if receipt is None else receipt.receipt_id)

    def _publish_proven(self, name: str, frame: pl.DataFrame, *, historical_baseline: bool,
                        input_receipt: FrozenInputReceipt,
                        expected_definition_hash: str,
                        writer_executor: ThreadPoolExecutor | None = None,
                        partition_workers: int = 1,
                        writer_buffer_bytes: int = 0,
                        commit_batch_rows: int | None = None,
                        commit_batch_bytes: int | None = None,
                        cancelled: Callable[[], bool] | None = None,
                        on_commit: Callable[[int, int | None], None] | None = None) -> ItemBuildReport:
        """Publish only internally evaluated frames with selected timing proof."""
        run_id = uuid4().hex
        with self._store.dataset_writer("items", name, run_id):
            return self._ingest(name, frame, available_date=None,
                historical_baseline=historical_baseline, ingested_at=None,
                input_receipt=input_receipt, expected_definition_hash=expected_definition_hash,
                run_id=run_id, _baseline_proven=True,
                writer_executor=writer_executor, partition_workers=partition_workers,
                writer_buffer_bytes=writer_buffer_bytes, commit_batch_rows=commit_batch_rows,
                commit_batch_bytes=commit_batch_bytes, cancelled=cancelled, on_commit=on_commit)

    def replace_range(self, name: str, frame: pl.DataFrame, *, start: DateLike, end: DateLike,
                      available_date: DateLike, input_receipt: FrozenInputReceipt | str | None = None,
                      expected_definition_hash: str | None = None,
                      historical_baseline: bool = False) -> ItemBuildReport:
        """Publish a complete coordinate range, with null events for withdrawn keys."""
        run_id = uuid4().hex
        with self._store.dataset_writer("items", name, run_id):
            return self._replace_range(name, frame, start=start, end=end, available_date=available_date,
                input_receipt=input_receipt, expected_definition_hash=expected_definition_hash,
                historical_baseline=historical_baseline, run_id=run_id)

    def _replace_range(self, name: str, frame: pl.DataFrame, *, start: DateLike, end: DateLike,
                       available_date: DateLike, input_receipt: FrozenInputReceipt | str | None,
                       expected_definition_hash: str | None, historical_baseline: bool,
                       run_id: str, _verification: _PublicationVerification | None = None,
                       cancelled: Callable[[], bool] | None = None) -> ItemBuildReport:
        spec = self.get(name)
        first, last, cutoff = _date_value(start), _date_value(end), _date_value(available_date)
        if last < first:
            raise ValueError("end precedes start")
        normalized = self._normalize(spec, frame)
        if normalized.filter(~pl.col("time").is_between(first, last)).height:
            raise ValueError("Replacement output lies outside its declared range")
        existing = self.read(name, start=first, end=last, view="snapshot", as_of=cutoff).collect()
        removed = existing.join(normalized.select("time", "asset_id"), on=["time", "asset_id"], how="anti")
        tombstones = removed.select("time", "asset_id").with_columns(pl.lit(None, dtype=scalar_dtype(spec.value_dtype)).alias("value"))
        from bagelquant_data.core.schema import concat_compatible_frames
        combined = concat_compatible_frames([normalized, tombstones]) if tombstones.height else normalized
        return self._ingest(name, combined, available_date=cutoff,
            historical_baseline=historical_baseline, ingested_at=None,
            input_receipt=input_receipt, expected_definition_hash=expected_definition_hash,
            run_id=run_id, _declared_range=(first, last), _verification=_verification, cancelled=cancelled)

    @contextmanager
    def publication(self, *, input_receipt: FrozenInputReceipt | str | None = None,
                    config: ExecutionOptions | None = None,
                    cancelled: Callable[[], bool] | None = None) -> Iterator[ItemPublisher]:
        """Bind published input metadata for one serial publication operation."""
        if cancelled and cancelled():
            raise CancelledError("DataItem publication cancelled")
        receipt = None if input_receipt is None else self._lake.inputs.get(input_receipt)
        verification = _PublicationVerification(self, "" if receipt is None else receipt.receipt_id,
            "" if receipt is None else receipt.digest,
            # A conservative 1KiB entry reserve; never retain producer frames.
            timing_cache_entries=min(1024, (config or ExecutionOptions()).max_buffer_bytes // 4 // 1024),
            timing_buffer_bytes=(config or ExecutionOptions()).max_buffer_bytes)
        try:
            if cancelled and cancelled():
                raise CancelledError("DataItem publication cancelled")
            yield ItemPublisher(self, receipt, verification, cancelled)
            if get_ident() != verification.thread_id:
                raise RuntimeError("DataItem publication requires its owning thread")
            if not verification.active:
                raise RuntimeError("DataItem publication operation failed")
            if cancelled and cancelled():
                raise CancelledError("DataItem publication cancelled")
        finally:
            verification.active = False
            verification.baseline_dates.clear()

    def _publish(self, publications: Sequence[ItemPublication], *,
                input_receipt: FrozenInputReceipt | str | None = None,
                _verification: _PublicationVerification,
                cancelled: Callable[[], bool] | None = None) -> dict[str, ItemBuildReport | None]:
        """Publish a preflighted group through the ordinary writer mechanics.

        Keep historical versions, complete-range certificates and per-item
        writer transactions. Earlier committed outputs survive later failures;
        this operation does not claim atomic publication across items.
        """
        values = tuple(publications)
        if any(not isinstance(value, ItemPublication) for value in values):
            raise TypeError("publish requires ItemPublication values")
        if len({value.name for value in values}) != len(values):
            raise ValueError("Publication item names must be unique")
        receipt = None if input_receipt is None else self._lake.inputs.get(input_receipt)
        def identity(value):
            return ("raw", value.source, value.dataset) if isinstance(value, RawInput) else ("item", value.name)
        for value in values:
            spec = self.get(value.name)
            definition_hash = hashlib.sha256(_json(spec_payload(spec)).encode()).hexdigest()
            if value.expected_definition_hash is not None and value.expected_definition_hash != definition_hash:
                raise RuntimeError("DataItem definition changed while computing producer output")
            if spec.inputs and receipt is None:
                raise ValueError("DataItem dependency publication requires an input_receipt")
            if receipt is not None and {identity(v) for v in spec.inputs} != {identity(v) for v in receipt.requests.values()}:
                raise ValueError("Frozen input receipt does not match declared DataItem dependencies")
            if value.start is not None and value.end is not None and _date_value(value.end) < _date_value(value.start):
                raise ValueError("end precedes start")
            if not value.frame.is_empty() or value.start is not None:
                normalized = self._normalize(spec, value.frame)
                if value.start is not None and value.end is not None and normalized.filter(
                        ~pl.col("time").is_between(_date_value(value.start), _date_value(value.end))).height:
                    raise ValueError("Replacement output lies outside its declared range")
        if cancelled and cancelled():
            raise CancelledError("DataItem publication cancelled")
        verification = _verification
        results: dict[str, ItemBuildReport | None] = {}
        for value in values:
            if cancelled and cancelled():
                raise CancelledError("DataItem publication cancelled")
            run_id = uuid4().hex
            result = None
            with self._store.dataset_writer("items", value.name, run_id):
                if not value.frame.is_empty():
                    result = self._ingest(value.name, value.frame, available_date=None,
                        historical_baseline=False, ingested_at=None, input_receipt=receipt,
                        expected_definition_hash=value.expected_definition_hash, run_id=run_id,
                        cancelled=cancelled, _verification=verification)
                if value.start is not None and value.end is not None and value.complete_at is not None:
                    if cancelled and cancelled():
                        raise CancelledError("DataItem publication cancelled")
                    latest = value.frame
                    spec = self.get(value.name)
                    if "version_available_date" in latest.columns:
                        latest = latest.sort("version_available_date").unique([spec.time_column, spec.asset_column], keep="last")
                    result = self._replace_range(value.name, latest, start=value.start, end=value.end,
                        available_date=value.complete_at, input_receipt=receipt,
                        expected_definition_hash=value.expected_definition_hash,
                        historical_baseline=False, run_id=run_id, _verification=verification,
                        cancelled=cancelled)
            if cancelled and cancelled():
                raise CancelledError("DataItem publication cancelled")
            results[value.name] = result
        if cancelled and cancelled():
            raise CancelledError("DataItem publication cancelled")
        return results

    def read(
        self, name: str, *, start: DateLike | None = None, end: DateLike | None = None,
        assets: tuple[str, ...] | list[str] | None = None,
        as_of: DateLike | None = None, view: str = "history", strict: bool = False,
        max_commit: int | None = None, max_check_id: int | None = None,
    ) -> pl.LazyFrame:
        """Read causal history, an explicit as-of snapshot, or diagnostic versions."""
        spec = self.get(name)
        if view not in {"history", "snapshot", "versions"}:
            raise ValueError("view must be history, snapshot, or versions")
        if view == "snapshot" and as_of is None:
            raise ValueError("snapshot reads require as_of")
        if self._max_commit is not None:
            if max_commit is not None and max_commit > self._max_commit:
                raise ValueError("max_commit exceeds the captured input boundary")
            max_commit = self._max_commit if max_commit is None else max_commit
        if self._as_of is not None:
            if as_of is not None and _date_value(as_of) > self._as_of:
                raise ValueError("as_of exceeds the captured information cutoff")
            as_of = self._as_of if as_of is None else as_of
        if self._max_check_id is not None:
            if max_check_id is not None and max_check_id > self._max_check_id:
                raise ValueError("max_check_id exceeds the captured input boundary")
            max_check_id = self._max_check_id if max_check_id is None else max_check_id
        if not self._store.manifest("items", name):
            return pl.DataFrame(schema={"time": pl.Date, "asset_id": pl.String, "value": scalar_dtype(spec.value_dtype), "observation_date": pl.Date, "available_date": pl.Date, "version_available_date": pl.Date, "_baseline": pl.Boolean}).lazy()
        frame = self._query.query(
            name, source="items", view="versions", max_commit=max_commit,
            observation_start=start, observation_end=end, assets=assets,
            max_check_id=max_check_id,
        )
        if strict:
            frame = frame.filter(~pl.col("_baseline").fill_null(False))
        if as_of is not None:
            frame = frame.filter(pl.col("time") <= _date_value(as_of))
        if view == "history":
            frame = frame.filter(pl.col("time") <= pl.col("source_time"))
        if view != "versions":
            frame = frame.sort("time", "ingested_at", "_commit_seq").unique(["source_time", "asset_id"], keep="last", maintain_order=True)
        return frame.rename({"time": "version_available_date", "source_time": "time"}).sort("time", "asset_id", "version_available_date")

    @overload
    def status(self, name: str, *, include_inactive: bool = False) -> dict[str, Any]: ...

    @overload
    def status(self, name: None = None, *, include_inactive: bool = False) -> list[dict[str, Any]]: ...

    def status(self, name: str | None = None, *, include_inactive: bool = False) -> list[dict[str, Any]] | dict[str, Any]:
        rows = self._store.dataset_statuses(source="items", datasets=None if name is None else [name], include_inactive=include_inactive)
        if name is None:
            return rows
        self.get(name, include_inactive=include_inactive)
        row = dict(rows[0]) if rows else {"name": name, "row_count": 0}
        item = self._store._rows("select spec_hash from item_definitions where name=?", (name,))[0]
        builds = self._store._rows("select dependency_digest,input_commit,result_commit,frozen_receipt_id from item_builds where name=? and status='success' order by id desc limit 1", (name,))
        row["definition_hash"] = item["spec_hash"]
        physical_hash = self._store.dataset_spec_hash("items", name)
        manifests = self._store.manifest("items", name)
        committed = self._store._rows(
            "with latest as (select b.partition_path,max(c.seq) as seq from version_batches b "
            "join version_commits c on c.seq=b.commit_seq where c.source='items' and c.dataset=? "
            "and c.status='committed' group by b.partition_path) "
            "select latest.partition_path,c.spec_hash from latest join version_commits c on c.seq=latest.seq",
            (name,),
        )
        hashes = {value["partition_path"]: value["spec_hash"] for value in committed}
        row["committed_definition_current"] = bool(manifests) and all(
            hashes.get(value["partition_path"]) == physical_hash for value in manifests
        )
        row.update(builds[0] if builds else {})
        # The physical manifest's time is availability; report the daily axis
        # separately using registered batch observation bounds.
        bounds = self._store._rows("select min(b.min_observation) as minimum_time,max(b.max_observation) as maximum_time from version_batches b join version_commits c on c.seq=b.commit_seq where c.source='items' and c.dataset=? and c.status='committed'", (name,))[0]
        row.update(bounds)
        return row

    def builds(self, name: str) -> list[dict[str, Any]]:
        """Successful immutable build references, retained after item archival."""
        self.get(name, include_inactive=True)
        return self._store._rows(
            "select id,name,definition_hash,dependency_digest,start_date,end_date,"
            "input_commit,result_commit,status,created_at,frozen_receipt_id "
            "from item_builds where name=? and status='success' order by id",
            (name,),
        )

    def manifest(self, name: str, *, include_inactive: bool = False) -> list[dict[str, Any]]:
        """Committed public partition receipts for application references."""
        self.get(name, include_inactive=include_inactive)
        definition = self._store._rows("select spec_hash from item_definitions where name=?", (name,))[0]["spec_hash"]
        result = []
        for manifest in self._store.manifest("items", name):
            row = dict(manifest)
            row["definition_hash"] = definition
            commit = self._store._rows("select max(b.commit_seq) as commit_seq,min(b.min_observation) as minimum_time,max(b.max_observation) as maximum_time from version_batches b join version_commits c on c.seq=b.commit_seq where c.source='items' and c.dataset=? and c.status='committed' and b.partition_path=?", (name, row["partition_path"]))[0]
            row.update(commit)
            result.append(row)
        return result

    def _reaches(self, first: str, target: str, seen: set[str] | None = None) -> bool:
        if first == target:
            return True
        seen = set() if seen is None else seen
        if first in seen:
            return False
        seen.add(first)
        return any(self._reaches(str(row["dataset"]), target, seen) for row in self._store._rows("select dataset from item_dependencies where item_name=? and kind='item'", (first,)))

    @staticmethod
    def _physical_spec(spec: DataItemSpec) -> DatasetSpec:
        return DatasetSpec(spec.name, "by_date", source="items", description=spec.description,
                           date_kind="calendar", field_mappings={"time": "time", "asset_id": "asset_id"},
                           request_options={"item_definition_hash": hashlib.sha256(_json(spec_payload(spec)).encode()).hexdigest()})

    def _normalize(self, spec: DataItemSpec, frame: pl.DataFrame) -> pl.DataFrame:
        frame = frame.drop([name for name in VERSION_FIELDS if name in frame.columns])
        mapping = {spec.time_column if spec.time_column in frame.columns else "time": "time", spec.asset_column if spec.asset_column in frame.columns else "asset_id": "asset_id", spec.value_column if spec.value_column in frame.columns else "value": "value"}
        missing = set(mapping) - set(frame.columns)
        if missing:
            raise ValueError(f"DataItem producer is missing columns: {sorted(missing)}")
        if spec.time_column != "time" and spec.time_column in frame.columns and "time" in frame.columns and "available_date" not in frame.columns:
            frame = frame.with_columns(pl.col("time").alias("available_date"))
        collision = [value for key, value in mapping.items() if key != value and value in frame.columns]
        frame = frame.drop(collision).rename({key: value for key, value in mapping.items() if key != value})
        frame = frame.with_columns(pl.col("time").cast(pl.Date, strict=True), pl.col("asset_id").cast(pl.String, strict=True), pl.col("value").cast(scalar_dtype(spec.value_dtype), strict=True))
        if frame.select(pl.any_horizontal(pl.col("time").is_null(), pl.col("asset_id").is_null(), pl.col("asset_id").str.strip_chars().str.len_chars() == 0).any()).item():
            raise ValueError("DataItem keys must not be null")
        frame = frame.with_columns(
            (pl.col("observation_date").cast(pl.Date) if "observation_date" in frame.columns else pl.col("time")).alias("observation_date"),
            (pl.col("available_date").cast(pl.Date) if "available_date" in frame.columns else pl.col("time")).alias("available_date"),
        )
        frame = frame.with_columns(
            pl.when(pl.col("value").is_null() & pl.col(name).is_null()).then(pl.col("time")).otherwise(pl.col(name)).alias(name)
            for name in ("observation_date", "available_date")
        )
        if frame["available_date"].null_count() or frame["observation_date"].null_count():
            raise ValueError("DataItem evidence dates must not be null")
        if "version_available_date" in frame.columns:
            frame = frame.with_columns(pl.col("version_available_date").cast(pl.Date, strict=True))
        return frame.sort("time", "asset_id", *( ["version_available_date"] if "version_available_date" in frame.columns else []))

    def _build(self, name: str, *, start: date, end: date, initialize: bool, config: ExecutionOptions | None, stack: tuple[str, ...],
               progress: Callable[[ItemBuildReport], None] | None, cancelled: Callable[[], bool] | None, force: bool = False,
               input_windows: Mapping[str, tuple[DateLike, DateLike]] | None = None) -> ItemBuildReport:
        self._store.ensure_writable()
        if end < start:
            raise ValueError("end precedes start")
        if name in stack:
            raise ValueError("DataItem dependency cycle")
        if cancelled and cancelled():
            return ItemBuildReport(name, "cancelled", 0, None, 0, start, end, "")
        spec = self.get(name)
        from dataclasses import replace
        requests = {value.key: replace(value, view="versions", fields=(), include_historical_baseline=True) if isinstance(value, RawInput)
                    else replace(value, view="versions") for value in spec.inputs}
        for key, window in (input_windows or {}).items():
            if key not in requests:
                raise ValueError(f"Unknown input window alias: {key}")
            first, last = (_date_value(value) for value in window)
            if last < first:
                raise ValueError("input window end precedes start")
            original = requests[key]
            if (original.start is not None and first < _date_value(original.start)
                    or original.end is not None and last > _date_value(original.end)):
                raise ValueError("input window cannot widen the declared dependency window")
            if isinstance(original, RawInput):
                if (original.observation_start is not None and first < _date_value(original.observation_start)
                        or original.observation_end is not None and last > _date_value(original.observation_end)):
                    raise ValueError("input window cannot widen the declared observation window")
                definition = self._store.get_dataset(original.source, original.dataset)
                if definition is None or json.loads(definition["spec_json"])["update_type"] != "by_date":
                    raise ValueError("input windows require by-date dependencies")
            requests[key] = replace(original, start=first, end=last)
        for dependency in sorted((value for value in spec.inputs if isinstance(value, ItemInput)), key=lambda value: value.name):
            if self.get(dependency.name).inputs:
                self._build(dependency.name, start=start, end=end, initialize=initialize, config=config, stack=(*stack, name), progress=progress, cancelled=cancelled)
        row = self._store._rows("select coalesce(max(seq),0) as seq from version_commits where status='committed'")[0]
        max_commit = int(row["seq"])
        # Frozen receipts capture dependency contracts and central immutable bytes
        # in one SQLite read transaction, independently from later publications.
        receipt = self._lake.inputs.freeze(requests, information_cutoff=end, max_commit=max_commit)
        max_commit = receipt.max_commit
        definition_hash = hashlib.sha256(_json(spec_payload(spec)).encode()).hexdigest()
        dependency_digest = receipt.dependency_digest
        prior = self._store._rows(
            "select result_commit from item_builds where name=? and definition_hash=? and dependency_digest=? and start_date=? and end_date=? and status='success' order by id desc limit 1",
            (name, definition_hash, dependency_digest, start.isoformat(), end.isoformat()),
        )
        if prior and not force:
            return ItemBuildReport(name, "unchanged", 0, prior[0]["result_commit"], max_commit, start, end, dependency_digest, receipt.receipt_id)
        options = config or ExecutionOptions()
        frames: dict[str, pl.DataFrame] = {}
        buffered = 0
        for key in requests:
            frame = self._lake.inputs.read(receipt, key).collect()
            buffered += frame.estimated_size()
            if buffered > options.max_buffer_bytes // 2:
                raise MemoryError("Frozen DataItem inputs exceed max_buffer_bytes; use smaller declared input windows")
            frames[key] = frame
        if not spec.inputs:
            raise ValueError("An initialized DataItem must declare inputs")
        producer = None
        if spec.producer_key:
            producer = self._producers.get((spec.producer_key, str(spec.producer_revision)))
            if producer is None:
                raise ValueError(f"Producer is not registered: {spec.producer_key}@{spec.producer_revision}")
        raw = {value.key: frames[value.key] for value in spec.inputs if isinstance(value, RawInput)}
        items = {value.key: frames[value.key] for value in spec.inputs if isinstance(value, ItemInput)}
        from .pit import _general_snapshot_identity
        general_evidence = {
            key: receipt.evidence[key] for key, frame in raw.items()
            if "_snapshot_id" in frame.columns
        }
        resolvers = {
            key: (lambda cutoff, frame=raw[key], evidence=evidence,
                  request=requests[key]: general_input_snapshot(
                      frame, cutoff, snapshots=evidence["general_snapshots"],
                      checks=evidence["checks"], strict=request.strict,
                      include_historical_baseline=isinstance(request, RawInput)
                      and request.include_historical_baseline,
                  ).lazy())
            for key, evidence in general_evidence.items()
        }
        general_boundaries = {
            _date_value(value["pit_date"])
            for evidence in general_evidence.values()
            for value in evidence["general_snapshots"]
            if value["mode"] != "initialize"
        } | {
            _date_value(value["pit_date"])
            for evidence in general_evidence.values() for value in evidence["checks"]
        }
        for key in items:
            general_boundaries.update(self._lake.inputs._empty_baseline_boundaries(receipt, key))
        final_output: dict[str, pl.DataFrame] = {}
        final_baseline = False

        def evaluate(raw_snapshots: dict, item_snapshots: dict, cutoff: date) -> pl.DataFrame:
            nonlocal final_baseline
            snapshots = {key: value.collect() for key, value in {**raw_snapshots, **item_snapshots}.items()}
            baseline = any("_baseline" in frame.columns and frame["_baseline"].fill_null(False).any() for frame in snapshots.values())
            # An empty initialization still carries unverified timing evidence;
            # no row exists on which to project its baseline marker.
            baseline = baseline or any(
                identity is not None and identity["baseline"]
                for key, evidence in general_evidence.items()
                for identity in [_general_snapshot_identity(
                    evidence["general_snapshots"], cutoff, checks=evidence["checks"],
                    strict=requests[key].strict,
                    include_historical_baseline=isinstance(requests[key], RawInput)
                    and cast(RawInput, requests[key]).include_historical_baseline,
                )]
            )
            baseline = baseline or any(self._lake.inputs._baseline_at(receipt, key, cutoff,
                                       max_buffer_bytes=int(options.max_buffer_bytes - buffered))
                                       for key in snapshots if snapshots[key].is_empty()
                                       and self._lake.inputs._could_have_baseline(receipt, key))
            context = BuildContext(snapshots, start, end, cutoff, max_commit, baseline)
            if producer:
                result = producer(context)
                frame = result.collect() if isinstance(result, pl.LazyFrame) else result
            else:
                frame = apply_transforms(snapshots[spec.inputs[0].key], spec.transforms, snapshots)
            frame = self._normalize(spec, frame).filter(pl.col("time").is_between(start, end))
            if cutoff == end:
                final_output["frame"] = frame
                final_baseline = baseline
            return frame.with_columns(pl.lit(baseline).alias("_build_baseline"))

        committed_rows = 0
        result_commit: int | None = None

        def publish_group(frame, baseline, *, executor, writer_buffer=0,
                          publication_bytes=None):
            before = committed_rows
            def published(rows, commit):
                nonlocal committed_rows, result_commit
                committed_rows = before + rows
                result_commit = commit
                if progress:
                    progress(ItemBuildReport(name, "running", committed_rows, result_commit, max_commit,
                        start, end, dependency_digest, receipt.receipt_id))
            return self._publish_proven(name, frame, historical_baseline=baseline,
                input_receipt=receipt, expected_definition_hash=definition_hash,
                writer_executor=executor, partition_workers=partition_workers,
                writer_buffer_bytes=writer_buffer, commit_batch_rows=options.commit_batch_rows,
                commit_batch_bytes=publication_bytes, cancelled=cancelled, on_commit=published)

        partition_workers = min(options.workers, options.max_in_flight or options.workers,
                                options.batch_size or options.workers)
        with ThreadPoolExecutor(max_workers=options.workers, thread_name_prefix="data-item") as executor:
            try:
                output_options = replace(options, max_buffer_bytes=options.max_buffer_bytes - buffered)
                writer_buffer = output_options.max_buffer_bytes // (options.workers + 1)
                publication_bytes = max(1, writer_buffer // 6)
                with closing(iter_computed_versions(raw=raw, items=items, start=start, end=end, evaluate=lambda raw, items: evaluate(raw, items, end), evaluate_at=evaluate, raw_resolvers=resolvers, extra_boundaries=sorted(general_boundaries), config=output_options, cancelled=cancelled, executor=executor)) as outputs:
                    for output in outputs:
                        if cancelled and cancelled():
                            raise CancelledError("DataItem build cancelled")
                        for baseline in (False, True):
                            if output.is_empty():
                                continue
                            group = output.filter(pl.col("_build_baseline") == baseline).drop("_build_baseline")
                            if group.is_empty():
                                continue
                            publish_group(group, baseline, executor=executor,
                                          writer_buffer=writer_buffer, publication_bytes=publication_bytes)
                        if progress:
                            progress(ItemBuildReport(name, "running", committed_rows, result_commit, max_commit, start, end, dependency_digest, receipt.receipt_id))
                        writer_buffer = 0
            except CancelledError:
                return ItemBuildReport(name, "cancelled", committed_rows, result_commit, max_commit, start, end, dependency_digest, receipt.receipt_id)
            if "frame" in final_output and self._store.manifest("items", name):
                existing = self.read(name, start=start, end=end, view="snapshot", as_of=end)
                keys = ["time", "asset_id"]
                existing_keys = existing.select(keys).collect()
                current_keys = final_output["frame"].select(keys)
                removed_keys = (existing_keys.head(0) if _same_ordered_rows(existing_keys, current_keys, keys)
                                else existing_keys.join(current_keys, on=keys, how="anti"))
                if removed_keys.height:
                    # Only actual withdrawals need the old values/extra columns.
                    removed = existing.join(removed_keys.lazy(), on=keys, how="inner",
                                            maintain_order="left").collect()
                    removed = removed.with_columns(pl.lit(None, dtype=scalar_dtype(spec.value_dtype)).alias("value"), pl.max_horizontal("time", pl.lit(end)).alias("version_available_date"))
                    try:
                        publish_group(removed, final_baseline, executor=executor)
                    except CancelledError:
                        return ItemBuildReport(name, "cancelled", committed_rows, result_commit,
                            max_commit, start, end, dependency_digest, receipt.receipt_id)
        with self._store.connect() as db:
            db.execute("insert into item_builds(name,definition_hash,dependency_digest,start_date,end_date,input_commit,result_commit,status,created_at,frozen_receipt_id) values(?,?,?,?,?,?,?,'success',?,?)", (name, definition_hash, dependency_digest, start.isoformat(), end.isoformat(), max_commit, result_commit, datetime.now(UTC).isoformat(), receipt.receipt_id))
        return ItemBuildReport(name, "success", committed_rows, result_commit, max_commit, start, end, dependency_digest, receipt.receipt_id)

__all__ = ["ItemAPI"]
