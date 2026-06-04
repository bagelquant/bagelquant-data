"""Data lake management and update orchestration."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from threading import Lock
from typing import Any, Literal

import pandas as pd

from bagelquant_data.datasource.base import DataRequest, DataSource
from bagelquant_data.datasource.registry import DataSourceRegistry
from bagelquant_data.lake.local import LocalDataLake, WriteMode
from bagelquant_data.lake.snapshot import SnapshotRef

ParallelMode = Literal["thread"]
TushareTableKind = Literal["general", "price", "fundamental", "fundamental_vip"]
TushareUpdateStatus = Literal["pending", "up_to_date"]
ProgressCallback = Callable[[Mapping[str, Any]], None]


@dataclass(frozen=True, slots=True)
class TushareUpdateJob:
    """Confirmed provider request needed to update a Tushare lake table."""

    table: str
    kind: TushareTableKind
    filters: Mapping[str, Any] = field(default_factory=dict)
    start_date: date | None = None
    end_date: date | None = None
    partition_column: str | None = None
    partition_granularity: Literal["month", "day", "quarter"] = "month"
    metadata: Mapping[str, Any] = field(default_factory=dict)
    item: str = ""
    mode: WriteMode = "append"


@dataclass(frozen=True, slots=True)
class TushareUpdatePlan:
    """Dry-run summary for a configured Tushare table."""

    table: str
    kind: TushareTableKind
    requested_start: date
    requested_end: date
    effective_start: date | None
    pending_items: tuple[str, ...]
    reason: str
    estimated_job_count: int
    status: TushareUpdateStatus


@dataclass(frozen=True, slots=True)
class TushareUpdateReport:
    """Dry-run report plus executable jobs for confirmed Tushare updates."""

    generated_at: datetime
    source: str
    requested_start: date
    requested_end: date
    plans: tuple[TushareUpdatePlan, ...]
    jobs: tuple[TushareUpdateJob, ...]


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
    ) -> tuple[SnapshotRef, ...]:
        """Update Tushare All universe into the local lake."""

        resolved_end = _normalize_date(end_date or date.today())
        resolved_start = _normalize_date(start_date)
        if resolved_start > resolved_end:
            raise ValueError("start_date must not be after end_date")
        tables = [table] if table == "stock_basic" else ["stock_basic", table]
        kinds: dict[str, TushareTableKind | None] = {"stock_basic": "general"}
        if kind is not None:
            kinds[table] = kind
        report = self.scan_tushare_updates(
            tables,
            kinds=kinds,
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
        tables: list[str] | tuple[str, ...],
        *,
        kinds: Mapping[str, TushareTableKind | None] | None = None,
        start_date: str | date | datetime = "2000-01-01",
        end_date: str | date | datetime | None = None,
    ) -> TushareUpdateReport:
        """Scan the local lake and build a dry-run Tushare update report."""

        resolved_end = _normalize_date(end_date or date.today())
        resolved_start = _normalize_date(start_date)
        if resolved_start > resolved_end:
            raise ValueError("start_date must not be after end_date")
        plans: list[TushareUpdatePlan] = []
        jobs: list[TushareUpdateJob] = []
        for table in tables:
            table_kind = _resolve_tushare_table_kind(table, (kinds or {}).get(table))
            plan, table_jobs = self._scan_tushare_table(
                table,
                table_kind,
                start_date=resolved_start,
                end_date=resolved_end,
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

        source = self.registry.resolve("tushare")
        if parallel != "thread":
            raise ValueError(
                "Only thread parallelism is supported for provider updates"
            )
        refs: list[SnapshotRef] = []
        catalog_assets: dict[str, set[str]] = {}
        catalog_fields: dict[str, set[str]] = {}
        completed = 0
        total = len(report.jobs)
        write_lock = Lock()

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
                        _existing_table(self.lake, "tushare", job.table),
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
            return self._scan_tushare_price_table(table, start_date, end_date)
        if kind == "fundamental_vip":
            return self._scan_tushare_fundamental_vip_table(
                table,
                start_date,
                end_date,
            )
        return self._scan_tushare_fundamental_table(table, start_date, end_date)

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
    ) -> tuple[TushareUpdatePlan, tuple[TushareUpdateJob, ...]]:
        existing_dates = _existing_dates(self.lake, "tushare", table)
        dates = [
            day
            for day in _date_range(start_date, end_date)
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
                },
                item=day.isoformat(),
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
            ),
            jobs,
        )

    def _scan_tushare_fundamental_table(
        self,
        table: str,
        start_date: date,
        end_date: date,
    ) -> tuple[TushareUpdatePlan, tuple[TushareUpdateJob, ...]]:
        existing = _existing_table(self.lake, "tushare", table)
        ts_codes = _local_tushare_codes(self.lake)
        if not ts_codes:
            return (
                TushareUpdatePlan(
                    table=table,
                    kind="fundamental",
                    requested_start=start_date,
                    requested_end=end_date,
                    effective_start=None,
                    pending_items=(),
                    reason="no local stock_basic ts_code values available",
                    estimated_job_count=0,
                    status="up_to_date",
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
                    },
                    item=ts_code,
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
    if parallel != "thread":
        raise ValueError("Only thread parallelism is supported for provider updates")
    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = [executor.submit(fn, value) for value in values]
        for future in as_completed(futures):
            yield future.result()


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
    except Exception:
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


def _local_tushare_codes(lake: LocalDataLake) -> tuple[str, ...]:
    stock_basic = _existing_table(lake, "tushare", "stock_basic")
    if stock_basic is not None and "ts_code" in stock_basic.columns:
        codes = [str(code) for code in stock_basic["ts_code"].dropna().tolist()]
        return tuple(dict.fromkeys(codes))
    codes = [
        _strip_tushare_asset_prefix(asset_id)
        for asset_id in lake.asset_ids("tushare")
        if asset_id
    ]
    return tuple(dict.fromkeys(codes))


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
