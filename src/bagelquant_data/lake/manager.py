"""Data lake management and update orchestration."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, date, datetime, timedelta
from threading import Lock
from typing import Any, Literal

import pandas as pd

from bagelquant_data.datasource.base import DataRequest, DataSource
from bagelquant_data.datasource.registry import DataSourceRegistry
from bagelquant_data.lake.local import LocalDataLake, WriteMode
from bagelquant_data.lake.snapshot import SnapshotRef
from bagelquant_data.lake.tushare_update import (
    TushareTableKind,
    TushareTableUpdateSpec,
    TushareTradingCalendarRef,
    TushareUniverseRef,
    TushareUpdateJob,
    TushareUpdatePlan,
    TushareUpdateReport,
)
from bagelquant_data.utils.exceptions import DatasetNotFoundError

ParallelMode = Literal["thread"]
ProgressCallback = Callable[[Mapping[str, Any]], None]


class DataLakeManager:
    """Manage local lake datasets and provider refreshes."""

    def __init__(
        self,
        lake: LocalDataLake,
        *,
        registry: DataSourceRegistry | None = None,
    ) -> None:
        self.lake = lake
        self.registry = registry or DataSourceRegistry()

    def add(
        self,
        source: str,
        dataset: str,
        data: pd.DataFrame,
        *,
        metadata: Mapping[str, Any] | None = None,
    ) -> SnapshotRef:
        """Add a local dataset under a source namespace."""

        return self.lake.add(source, dataset, data, metadata=metadata)

    def edit(
        self,
        source: str,
        dataset: str,
        data: pd.DataFrame,
        *,
        metadata: Mapping[str, Any] | None = None,
    ) -> SnapshotRef:
        """Replace a local dataset with a new snapshot."""

        return self.lake.edit(source, dataset, data, metadata=metadata)

    def delete(
        self,
        source: str,
        dataset: str | None = None,
        *,
        snapshot: str | None = None,
    ) -> None:
        """Delete a source, dataset, or snapshot."""

        self.lake.delete(source, dataset, snapshot=snapshot)

    def list_sources(self) -> tuple[str, ...]:
        """List source namespaces."""

        return self.lake.list_sources()

    def list_datasets(self, source: str | None = None) -> tuple[tuple[str, str], ...]:
        """List managed datasets."""

        return self.lake.list_datasets(source)

    def list_tables(self, source: str | None = None) -> tuple[tuple[str, str], ...]:
        """List managed tables."""

        return self.lake.list_tables(source)

    def snapshots(self, source: str, dataset: str) -> tuple[SnapshotRef, ...]:
        """List snapshots for a dataset."""

        return self.lake.snapshots(source, dataset)

    def update(
        self,
        source: str | DataSource,
        request: DataRequest,
        *,
        mode: WriteMode = "overwrite",
    ) -> SnapshotRef:
        """Fetch from a provider and write a new local lake snapshot."""

        provider = self.registry.resolve(source) if isinstance(source, str) else source
        return self.lake.ingest(provider, request, mode=mode)

    def update_tushare_all(
        self,
        table: str,
        *,
        kind: TushareTableKind | None = None,
        start_date: str | date | datetime = "2000-01-01",
        end_date: str | date | datetime | None = None,
        workers: int = 4,
        parallel: ParallelMode = "thread",
        progress: ProgressCallback | None = None,
        universe: TushareUniverseRef | None = None,
        trading_calendar: TushareTradingCalendarRef | None = None,
    ) -> tuple[SnapshotRef, ...]:
        """Update Tushare All universe into the local lake."""

        resolved_end = _normalize_date(end_date or date.today())
        resolved_start = _normalize_date(start_date)
        if resolved_start > resolved_end:
            raise ValueError("start_date must not be after end_date")
        kinds: dict[str, TushareTableKind | None] = {}
        if kind is not None:
            kinds[table] = kind
        report = self.scan_tushare_updates(
            specs=(
                TushareTableUpdateSpec(
                    table=table,
                    kind=kinds.get(table),
                    universe=universe,
                    trading_calendar=trading_calendar,
                ),
            ),
            start_date=resolved_start,
            end_date=resolved_end,
        )
        return self.execute_tushare_update_report(
            report,
            workers=workers,
            parallel=parallel,
            progress=progress,
        )

    def scan_tushare_updates(
        self,
        tables: list[str] | tuple[str, ...] | None = None,
        *,
        specs: list[TushareTableUpdateSpec] | tuple[TushareTableUpdateSpec, ...]
        | None = None,
        kinds: Mapping[str, TushareTableKind | None] | None = None,
        start_date: str | date | datetime = "2000-01-01",
        end_date: str | date | datetime | None = None,
        universes: Mapping[str, TushareUniverseRef | None] | None = None,
        trading_calendars: Mapping[str, TushareTradingCalendarRef | None] | None = None,
    ) -> TushareUpdateReport:
        """Scan the local lake and build a dry-run Tushare update report."""

        update_specs = _update_specs_from_inputs(
            tables=tables,
            specs=specs,
            kinds=kinds,
            universes=universes,
            trading_calendars=trading_calendars,
        )
        resolved_end = _normalize_date(end_date or date.today())
        resolved_start = _normalize_date(start_date)
        if resolved_start > resolved_end:
            raise ValueError("start_date must not be after end_date")
        plans: list[TushareUpdatePlan] = []
        jobs: list[TushareUpdateJob] = []
        for spec in update_specs:
            table_kind = _resolve_tushare_table_kind(spec.table, spec.kind)
            plan, table_jobs = self._scan_tushare_table(
                spec.table,
                table_kind,
                start_date=resolved_start,
                end_date=resolved_end,
                universe=spec.universe,
                trading_calendar=spec.trading_calendar,
            )
            plans.append(plan)
            jobs.extend(table_jobs)
        return TushareUpdateReport(
            generated_at=datetime.now(UTC),
            source="tushare",
            requested_start=resolved_start,
            requested_end=resolved_end,
            plans=tuple(plans),
            jobs=tuple(jobs),
        )

    def execute_tushare_update_report(
        self,
        report: TushareUpdateReport,
        *,
        workers: int = 4,
        parallel: ParallelMode = "thread",
        progress: ProgressCallback | None = None,
    ) -> tuple[SnapshotRef, ...]:
        """Execute provider jobs from a confirmed Tushare update report."""

        _validate_parallel(workers=workers, parallel=parallel)
        source = self.registry.resolve("tushare")
        refs: list[SnapshotRef] = []
        catalog_assets: dict[str, set[str]] = {}
        catalog_fields: dict[str, set[str]] = {}
        completed = 0
        total = len(report.jobs)
        write_lock = Lock()
        existing_fundamentals = {
            table: _existing_table(self.lake, "tushare", table)
            for table in {
                job.table for job in report.jobs if job.kind == "fundamental"
            }
        }

        def read_job(job: TushareUpdateJob) -> tuple[TushareUpdateJob, pd.DataFrame]:
            if job.table == "stock_basic":
                return job, self._read_tushare_stock_basic(source)
            return job, source.read(
                DataRequest(
                    dataset=job.table,
                    filters=job.filters,
                    start_date=job.start_date,
                    end_date=job.end_date,
                )
            )

        for job, data in _parallel_iter(
            read_job,
            report.jobs,
            workers=workers,
            parallel=parallel,
        ):
            with write_lock:
                ref = None
                if job.kind == "fundamental":
                    data = _filter_incremental_rows(
                        existing_fundamentals.get(job.table),
                        data,
                    )
                if not data.empty:
                    ref = self.lake.write(
                        "tushare",
                        job.table,
                        data,
                        mode=job.mode,
                        partition_column=job.partition_column,
                        partition_granularity=job.partition_granularity,
                        metadata=job.metadata,
                        update_catalogs=False,
                    )
                    refs.append(ref)
                    assets, fields = _catalog_entries(data)
                    catalog_assets.setdefault(job.table, set()).update(assets)
                    catalog_fields.setdefault(job.table, set()).update(fields)
                completed += 1
                _emit_progress(
                    progress,
                    table=job.table,
                    kind=job.kind,
                    item=job.item,
                    completed=completed,
                    total=total,
                    rows_written=0 if data.empty else len(data),
                    snapshot=ref,
                )
        for table in sorted(set(catalog_assets).union(catalog_fields)):
            self.lake.update_catalog_entries(
                "tushare",
                table,
                asset_ids=catalog_assets.get(table),
                fields=catalog_fields.get(table),
            )
        return tuple(refs)

    def _scan_tushare_table(
        self,
        table: str,
        kind: TushareTableKind,
        *,
        start_date: date,
        end_date: date,
        universe: TushareUniverseRef | None = None,
        trading_calendar: TushareTradingCalendarRef | None = None,
    ) -> tuple[TushareUpdatePlan, tuple[TushareUpdateJob, ...]]:
        if table == "stock_basic":
            job = TushareUpdateJob(
                table="stock_basic",
                kind="general",
                metadata={"update_strategy": "full_refresh"},
                item="full_refresh",
                mode="overwrite",
            )
            return (
                TushareUpdatePlan(
                    table=table,
                    kind="general",
                    requested_start=start_date,
                    requested_end=end_date,
                    effective_start=start_date,
                    pending_items=("full_refresh",),
                    reason="stock_basic is refreshed as a full table",
                    estimated_job_count=1,
                    status="pending",
                ),
                (job,),
            )
        if kind == "general":
            job = TushareUpdateJob(
                table=table,
                kind="general",
                metadata={"update_strategy": "full_refresh"},
                item="full_refresh",
                mode="overwrite",
            )
            return (
                TushareUpdatePlan(
                    table=table,
                    kind="general",
                    requested_start=start_date,
                    requested_end=end_date,
                    effective_start=start_date,
                    pending_items=("full_refresh",),
                    reason="general table is refreshed as a full table",
                    estimated_job_count=1,
                    status="pending",
                ),
                (job,),
            )
        if kind == "price":
            return self._scan_tushare_price_table(
                table,
                start_date,
                end_date,
                trading_calendar=trading_calendar or _default_tushare_calendar_ref(),
            )
        if kind == "fundamental_vip":
            return self._scan_tushare_fundamental_vip_table(
                table,
                start_date,
                end_date,
            )
        return self._scan_tushare_fundamental_table(
            table,
            start_date,
            end_date,
            universe=universe or _default_tushare_universe_ref(),
        )

    def update_tushare_stock_basic(self) -> SnapshotRef:
        """Refresh the full Tushare stock universe table."""

        source = self.registry.resolve("tushare")
        stock_basic = self._read_tushare_stock_basic(source)
        return self.lake.write(
            "tushare",
            "stock_basic",
            stock_basic.reset_index(drop=True),
            mode="overwrite",
        )

    def update_tushare_universe(
        self,
        table: str,
        *,
        mode: WriteMode = "overwrite",
    ) -> SnapshotRef:
        """Refresh a Tushare universe reference table."""

        source = self.registry.resolve("tushare")
        if table == "stock_basic":
            data = self._read_tushare_stock_basic(source)
        else:
            data = source.read(DataRequest(dataset=table))
        return self.lake.write("tushare", table, data.reset_index(drop=True), mode=mode)

    def update_tushare_trading_calendar(
        self,
        table: str = "trade_cal",
        *,
        start_date: str | date | datetime = "2000-01-01",
        end_date: str | date | datetime | None = None,
        filters: Mapping[str, Any] | None = None,
        mode: WriteMode = "overwrite",
    ) -> SnapshotRef:
        """Refresh a Tushare trading calendar reference table."""

        source = self.registry.resolve("tushare")
        resolved_end = _normalize_date(end_date or date.today())
        resolved_start = _normalize_date(start_date)
        data = source.read(
            DataRequest(
                dataset=table,
                filters=dict(filters or {}),
                start_date=resolved_start,
                end_date=resolved_end,
            )
        )
        return self.lake.write("tushare", table, data.reset_index(drop=True), mode=mode)

    def _read_tushare_stock_basic(self, source: DataSource) -> pd.DataFrame:
        frames = [
            source.read(
                DataRequest(dataset="stock_basic", filters={"list_status": status})
            )
            for status in ("L", "D", "P")
        ]
        non_empty = [frame for frame in frames if not frame.empty]
        stock_basic = (
            pd.concat(non_empty, axis=0, ignore_index=True)
            if non_empty
            else pd.DataFrame()
        )
        if "ts_code" in stock_basic.columns:
            stock_basic = stock_basic.drop_duplicates("ts_code").sort_values("ts_code")
        return stock_basic.reset_index(drop=True)

    def _scan_tushare_price_table(
        self,
        table: str,
        start_date: date,
        end_date: date,
        *,
        trading_calendar: TushareTradingCalendarRef,
    ) -> tuple[TushareUpdatePlan, tuple[TushareUpdateJob, ...]]:
        existing_dates = _existing_dates(self.lake, "tushare", table)
        dates = [
            day
            for day in _local_trading_dates(
                self.lake,
                "tushare",
                trading_calendar,
                start_date,
                end_date,
            )
            if day not in existing_dates
        ]
        jobs = tuple(
            TushareUpdateJob(
                table=table,
                kind="price",
                filters={"trade_date": day.strftime("%Y%m%d")},
                partition_column="trade_date",
                partition_granularity="day",
                metadata={
                    "update_strategy": "day_by_day_incremental",
                    "trade_date": day.isoformat(),
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "trading_calendar": trading_calendar.name,
                },
                item=day.isoformat(),
                trading_calendar=trading_calendar.name,
            )
            for day in dates
        )
        pending_items = tuple(day.isoformat() for day in dates)
        return (
            TushareUpdatePlan(
                table=table,
                kind="price",
                requested_start=start_date,
                requested_end=end_date,
                effective_start=dates[0] if dates else None,
                pending_items=pending_items,
                reason=(
                    "missing local trade_date partitions"
                    if dates
                    else "all requested trade_date values already exist locally"
                ),
                estimated_job_count=len(jobs),
                status="pending" if jobs else "up_to_date",
                trading_calendar=trading_calendar.name,
            ),
            jobs,
        )

    def _scan_tushare_fundamental_table(
        self,
        table: str,
        start_date: date,
        end_date: date,
        *,
        universe: TushareUniverseRef,
    ) -> tuple[TushareUpdatePlan, tuple[TushareUpdateJob, ...]]:
        existing = _existing_table(self.lake, "tushare", table)
        ts_codes = _local_tushare_codes(self.lake, universe=universe)
        if not ts_codes:
            return (
                TushareUpdatePlan(
                    table=table,
                    kind="fundamental",
                    requested_start=start_date,
                    requested_end=end_date,
                    effective_start=None,
                    pending_items=(),
                    reason=(
                        f"no local {universe.table}.{universe.code_column} "
                        "values available"
                    ),
                    estimated_job_count=0,
                    status="up_to_date",
                    universe=universe.name,
                ),
                (),
            )
        jobs = []
        for ts_code in ts_codes:
            update_start = _incremental_start(existing, ts_code, start_date)
            if update_start > end_date:
                continue
            jobs.append(
                TushareUpdateJob(
                    table=table,
                    kind="fundamental",
                    filters={"ts_code": ts_code},
                    start_date=update_start,
                    end_date=end_date,
                    partition_column="f_ann_date",
                    metadata={
                        "update_strategy": "asset_incremental",
                        "asset_id": ts_code,
                        "start_date": update_start.isoformat(),
                        "end_date": end_date.isoformat(),
                        "universe": universe.name,
                    },
                    item=ts_code,
                    universe=universe.name,
                )
            )
        pending_items = tuple(job.item for job in jobs)
        effective_starts = [
            job.start_date for job in jobs if job.start_date is not None
        ]
        return (
            TushareUpdatePlan(
                table=table,
                kind="fundamental",
                requested_start=start_date,
                requested_end=end_date,
                effective_start=min(effective_starts) if effective_starts else None,
                pending_items=pending_items,
                reason=(
                    "asset-level incremental requests from latest local f_ann_date"
                    if jobs
                    else (
                        "all asset-level f_ann_date values are after requested "
                        "end date"
                    )
                ),
                estimated_job_count=len(jobs),
                status="pending" if jobs else "up_to_date",
                universe=universe.name,
            ),
            tuple(jobs),
        )

    def _scan_tushare_fundamental_vip_table(
        self,
        table: str,
        start_date: date,
        end_date: date,
    ) -> tuple[TushareUpdatePlan, tuple[TushareUpdateJob, ...]]:
        existing = _existing_table(self.lake, "tushare", table)
        periods = _incremental_periods(existing, start_date, end_date)
        jobs = tuple(
            TushareUpdateJob(
                table=table,
                kind="fundamental_vip",
                filters={"period": period.strftime("%Y%m%d")},
                partition_column="f_ann_date",
                partition_granularity="quarter",
                metadata={
                    "update_strategy": "season_by_season_incremental",
                    "period": period.isoformat(),
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                },
                item=period.isoformat(),
            )
            for period in periods
        )
        pending_items = tuple(period.isoformat() for period in periods)
        return (
            TushareUpdatePlan(
                table=table,
                kind="fundamental_vip",
                requested_start=start_date,
                requested_end=end_date,
                effective_start=periods[0] if periods else None,
                pending_items=pending_items,
                reason=(
                    "missing local reporting quarters"
                    if periods
                    else "all requested reporting quarters already exist locally"
                ),
                estimated_job_count=len(jobs),
                status="pending" if jobs else "up_to_date",
            ),
            jobs,
        )

    def _update_tushare_price_table(
        self,
        *,
        source: DataSource,
        table: str,
        start_date: date,
        end_date: date,
        workers: int,
        parallel: ParallelMode,
        progress: ProgressCallback | None,
    ) -> tuple[SnapshotRef, ...]:
        existing_dates = _existing_dates(self.lake, "tushare", table)
        dates = [
            day
            for day in _date_range(start_date, end_date)
            if day not in existing_dates
        ]
        refs: list[SnapshotRef] = []
        catalog_assets: set[str] = set()
        catalog_fields: set[str] = set()
        completed = 0
        total = len(dates)
        write_lock = Lock()

        def read_day(day: date) -> tuple[date, pd.DataFrame]:
            return day, source.read(
                DataRequest(
                    dataset=table,
                    filters={"trade_date": day.strftime("%Y%m%d")},
                )
            )

        for day, data in _parallel_iter(
            read_day,
            dates,
            workers=workers,
            parallel=parallel,
        ):
            with write_lock:
                ref = None
                if not data.empty:
                    ref = self.lake.write(
                        "tushare",
                        table,
                        data,
                        mode="append",
                        partition_column="trade_date",
                        partition_granularity="day",
                        metadata={
                            "update_strategy": "day_by_day_incremental",
                            "trade_date": day.isoformat(),
                            "start_date": start_date.isoformat(),
                            "end_date": end_date.isoformat(),
                        },
                        update_catalogs=False,
                    )
                    refs.append(ref)
                    assets, fields = _catalog_entries(data)
                    catalog_assets.update(assets)
                    catalog_fields.update(fields)
                completed += 1
                _emit_progress(
                    progress,
                    table=table,
                    kind="price",
                    item=day.isoformat(),
                    completed=completed,
                    total=total,
                    rows_written=0 if data.empty else len(data),
                    snapshot=ref,
                )
        self.lake.update_catalog_entries(
            "tushare",
            table,
            asset_ids=catalog_assets,
            fields=catalog_fields,
        )
        return tuple(refs)

    def _update_tushare_fundamental_table(
        self,
        *,
        source: DataSource,
        table: str,
        start_date: date,
        end_date: date,
        workers: int,
        parallel: ParallelMode,
        progress: ProgressCallback | None,
    ) -> tuple[SnapshotRef, ...]:
        if parallel != "thread":
            raise ValueError(
                "Only thread parallelism is supported for provider updates"
            )
        existing = _existing_table(self.lake, "tushare", table)
        update_start = _latest_f_ann_date(existing, start_date)
        if update_start > end_date:
            return ()
        refs: list[SnapshotRef] = []
        catalog_assets: set[str] = set()
        catalog_fields: set[str] = set()

        data = source.read(
            DataRequest(
                dataset=table,
                start_date=update_start,
                end_date=end_date,
            )
        )
        data = _filter_incremental_rows(existing, data)
        ref = None
        if not data.empty:
            ref = self.lake.write(
                "tushare",
                table,
                data,
                mode="append",
                partition_column="f_ann_date",
                metadata={
                    "update_strategy": "table_incremental",
                    "start_date": update_start.isoformat(),
                    "end_date": end_date.isoformat(),
                },
                update_catalogs=False,
            )
            refs.append(ref)
            assets, fields = _catalog_entries(data)
            catalog_assets.update(assets)
            catalog_fields.update(fields)

        _emit_progress(
            progress,
            table=table,
            kind="fundamental",
            item=update_start.isoformat(),
            completed=1,
            total=1,
            rows_written=0 if data.empty else len(data),
            snapshot=ref,
        )
        self.lake.update_catalog_entries(
            "tushare",
            table,
            asset_ids=catalog_assets,
            fields=catalog_fields,
        )
        return tuple(refs)

    def _update_tushare_fundamental_vip_table(
        self,
        *,
        source: DataSource,
        table: str,
        start_date: date,
        end_date: date,
        workers: int,
        parallel: ParallelMode,
        progress: ProgressCallback | None,
    ) -> tuple[SnapshotRef, ...]:
        existing = _existing_table(self.lake, "tushare", table)
        periods = _incremental_periods(existing, start_date, end_date)
        refs: list[SnapshotRef] = []
        catalog_assets: set[str] = set()
        catalog_fields: set[str] = set()
        completed = 0
        total = len(periods)
        write_lock = Lock()

        def read_period(period: date) -> tuple[date, pd.DataFrame]:
            return period, source.read(
                DataRequest(
                    dataset=table,
                    filters={"period": period.strftime("%Y%m%d")},
                )
            )

        for period, data in _parallel_iter(
            read_period,
            periods,
            workers=workers,
            parallel=parallel,
        ):
            with write_lock:
                ref = None
                if not data.empty:
                    ref = self.lake.write(
                        "tushare",
                        table,
                        data,
                        mode="append",
                        partition_column="f_ann_date",
                        partition_granularity="quarter",
                        metadata={
                            "update_strategy": "season_by_season_incremental",
                            "period": period.isoformat(),
                            "start_date": start_date.isoformat(),
                            "end_date": end_date.isoformat(),
                        },
                        update_catalogs=False,
                    )
                    refs.append(ref)
                    assets, fields = _catalog_entries(data)
                    catalog_assets.update(assets)
                    catalog_fields.update(fields)
                completed += 1
                _emit_progress(
                    progress,
                    table=table,
                    kind="fundamental_vip",
                    item=period.isoformat(),
                    completed=completed,
                    total=total,
                    rows_written=0 if data.empty else len(data),
                    snapshot=ref,
                )
        self.lake.update_catalog_entries(
            "tushare",
            table,
            asset_ids=catalog_assets,
            fields=catalog_fields,
        )
        return tuple(refs)


def _parallel_iter(
    fn,
    values,
    *,
    workers: int,
    parallel: ParallelMode,
):
    _validate_parallel(workers=workers, parallel=parallel)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(fn, value) for value in values]
        for future in as_completed(futures):
            yield future.result()


def _validate_parallel(*, workers: int, parallel: ParallelMode) -> None:
    if workers < 1:
        raise ValueError("workers must be greater than or equal to 1")
    if parallel != "thread":
        raise ValueError("Only thread parallelism is supported for provider updates")


def _update_specs_from_inputs(
    *,
    tables: list[str] | tuple[str, ...] | None,
    specs: list[TushareTableUpdateSpec] | tuple[TushareTableUpdateSpec, ...] | None,
    kinds: Mapping[str, TushareTableKind | None] | None,
    universes: Mapping[str, TushareUniverseRef | None] | None,
    trading_calendars: Mapping[str, TushareTradingCalendarRef | None] | None,
) -> tuple[TushareTableUpdateSpec, ...]:
    if specs is not None:
        return tuple(specs)
    if tables is None:
        raise ValueError("scan_tushare_updates requires specs or tables")
    return tuple(
        TushareTableUpdateSpec(
            table=table,
            kind=(kinds or {}).get(table),
            universe=(universes or {}).get(table),
            trading_calendar=(trading_calendars or {}).get(table),
        )
        for table in tables
    )


def _emit_progress(
    progress: ProgressCallback | None,
    *,
    table: str,
    kind: TushareTableKind,
    item: str,
    completed: int,
    total: int,
    rows_written: int,
    snapshot: SnapshotRef | None,
) -> None:
    if progress is None:
        return
    progress(
        {
            "table": table,
            "kind": kind,
            "item": item,
            "completed": completed,
            "total": total,
            "rows_written": rows_written,
            "snapshot": snapshot,
        }
    )


def _catalog_entries(data: pd.DataFrame) -> tuple[set[str], set[str]]:
    asset_column = next(
        (
            column
            for column in ("ts_code", "symbol", "asset_id", "code")
            if column in data.columns
        ),
        None,
    )
    assets = (
        set(data[asset_column].dropna().astype(str).tolist())
        if asset_column is not None
        else set()
    )
    ignored = {"index", "create_time", "delete_flag"}
    fields = {
        str(column)
        for column in data.reset_index().columns
        if column not in ignored
    }
    return assets, fields


def _resolve_tushare_table_kind(
    table: str,
    kind: TushareTableKind | None,
) -> TushareTableKind:
    if kind is not None:
        return kind
    if table == "stock_basic":
        return "general"
    if table in {"daily", "index_daily"}:
        return "price"
    if table.endswith("_vip"):
        return "fundamental_vip"
    return "fundamental"


def _date_range(start: date, end: date) -> list[date]:
    days = []
    current = start
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return days


def _quarter_periods(start: date, end: date) -> list[date]:
    periods = []
    for year in range(start.year, end.year + 1):
        for month, day in ((3, 31), (6, 30), (9, 30), (12, 31)):
            period = date(year, month, day)
            if start <= period <= end:
                periods.append(period)
    return periods


def _normalize_date(value: str | date | datetime) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(value).date()


def _existing_table(
    lake: LocalDataLake,
    source: str,
    table: str,
) -> pd.DataFrame | None:
    try:
        return lake.read(source, table)
    except DatasetNotFoundError:
        return None


def _existing_dates(
    lake: LocalDataLake,
    source: str,
    table: str,
) -> set[date]:
    existing = _existing_table(lake, source, table)
    if existing is None or existing.empty:
        return set()
    if isinstance(existing.index, pd.DatetimeIndex):
        return {value.date() for value in existing.index.dropna().unique()}
    if "trade_date" not in existing.columns:
        return set()
    raw_dates = [
        str(value) for value in existing["trade_date"].tolist() if pd.notna(value)
    ]
    return {_parse_yyyymmdd(value) for value in raw_dates}


def _default_tushare_universe_ref() -> TushareUniverseRef:
    return TushareUniverseRef(
        name="stock_basic",
        table="stock_basic",
        code_column="ts_code",
    )


def _default_tushare_calendar_ref() -> TushareTradingCalendarRef:
    return TushareTradingCalendarRef(
        name="trade_cal",
        table="trade_cal",
        date_column="cal_date",
        open_column="is_open",
    )


def _local_tushare_codes(
    lake: LocalDataLake,
    *,
    universe: TushareUniverseRef,
) -> tuple[str, ...]:
    universe_table = _existing_table(lake, "tushare", universe.table)
    if universe_table is not None and universe.code_column in universe_table.columns:
        codes = [
            str(code)
            for code in universe_table[universe.code_column].dropna().tolist()
        ]
        return tuple(dict.fromkeys(codes))
    if universe.table != "stock_basic":
        return ()
    codes = [
        _strip_tushare_asset_prefix(asset_id)
        for asset_id in lake.asset_ids("tushare")
        if asset_id
    ]
    return tuple(dict.fromkeys(codes))


def _local_trading_dates(
    lake: LocalDataLake,
    source: str,
    calendar: TushareTradingCalendarRef,
    start_date: date,
    end_date: date,
) -> tuple[date, ...]:
    calendar_table = _existing_table(lake, source, calendar.table)
    if calendar_table is None or calendar_table.empty:
        return tuple(_date_range(start_date, end_date))
    if calendar.date_column not in calendar_table.columns:
        return tuple(_date_range(start_date, end_date))
    frame = calendar_table
    if calendar.open_column in frame.columns:
        frame = frame.loc[frame[calendar.open_column].map(_is_open_calendar_value)]
    raw_dates = [
        str(value)
        for value in frame[calendar.date_column].tolist()
        if pd.notna(value)
    ]
    dates = sorted(
        {
            _parse_yyyymmdd(value)
            for value in raw_dates
            if start_date <= _parse_yyyymmdd(value) <= end_date
        }
    )
    return tuple(dates)


def _is_open_calendar_value(value: object) -> bool:
    if pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text in {"1", "true", "t", "yes", "y", "open"}


def _strip_tushare_asset_prefix(asset_id: str) -> str:
    return asset_id.removeprefix("tushare_")


def _incremental_start(
    existing: pd.DataFrame | None,
    ts_code: str,
    default_start: date,
) -> date:
    if (
        existing is None
        or "ts_code" not in existing.columns
        or "f_ann_date" not in existing.columns
    ):
        return default_start
    rows = existing[existing["ts_code"].astype(str) == ts_code]
    if rows.empty:
        return default_start
    raw_dates = [str(value) for value in rows["f_ann_date"].tolist() if pd.notna(value)]
    dates = [_parse_yyyymmdd(value) for value in raw_dates]
    if not dates:
        return default_start
    return max(max(dates), default_start)


def _latest_f_ann_date(
    existing: pd.DataFrame | None,
    default_start: date,
) -> date:
    if (
        existing is None
        or "f_ann_date" not in existing.columns
    ):
        return default_start
    raw_dates = [
        str(value) for value in existing["f_ann_date"].tolist() if pd.notna(value)
    ]
    dates = [_parse_yyyymmdd(value) for value in raw_dates]
    if not dates:
        return default_start
    return max(max(dates), default_start)


def _filter_incremental_rows(
    existing: pd.DataFrame | None,
    data: pd.DataFrame,
) -> pd.DataFrame:
    if existing is None or existing.empty or data.empty:
        return data
    keys = _stable_row_keys(existing, data)
    if not keys:
        return data
    existing_keys = {
        tuple(
            _stable_key_value(column, value)
            for column, value in zip(keys, row, strict=True)
        )
        for row in existing[keys].fillna("").itertuples(index=False, name=None)
    }
    rows = data[keys].fillna("").itertuples(index=False, name=None)
    mask = [
        tuple(
            _stable_key_value(column, value)
            for column, value in zip(keys, row, strict=True)
        )
        not in existing_keys
        for row in rows
    ]
    return data.loc[mask].copy(deep=True)


def _stable_row_keys(existing: pd.DataFrame, data: pd.DataFrame) -> list[str]:
    preferred = (
        "ts_code",
        "symbol",
        "asset_id",
        "code",
        "end_date",
        "f_ann_date",
        "ann_date",
        "period",
    )
    keys = [
        column
        for column in preferred
        if column in existing.columns and column in data.columns
    ]
    if "f_ann_date" in existing.columns and "f_ann_date" in data.columns:
        return keys or ["f_ann_date"]
    return keys


def _stable_key_value(column: str, value: object) -> str:
    if column in {"trade_date", "f_ann_date", "ann_date", "end_date", "period"}:
        try:
            timestamp = pd.Timestamp(value)
        except Exception:
            return str(value)
        if not pd.isna(timestamp):
            return timestamp.strftime("%Y%m%d")
    return str(value)


def _incremental_periods(
    existing: pd.DataFrame | None,
    start_date: date,
    end_date: date,
) -> list[date]:
    if existing is None:
        return _quarter_periods(start_date, end_date)
    column = "end_date" if "end_date" in existing.columns else "f_ann_date"
    if column not in existing.columns:
        return _quarter_periods(start_date, end_date)
    raw_dates = [str(value) for value in existing[column].tolist() if pd.notna(value)]
    dates = [_parse_yyyymmdd(value) for value in raw_dates]
    if not dates:
        return _quarter_periods(start_date, end_date)
    return _quarter_periods(max(max(dates) + timedelta(days=1), start_date), end_date)


def _parse_yyyymmdd(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except ValueError:
        timestamp = pd.Timestamp(value)
        if pd.isna(timestamp):
            raise
        return timestamp.date()
