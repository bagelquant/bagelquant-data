"""Data lake management and update orchestration."""

from __future__ import annotations

from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any, Literal

import pandas as pd

from bagelquant_data.datasource.base import DataRequest, DataSource
from bagelquant_data.datasource.registry import DataSourceRegistry
from bagelquant_data.lake.local import LocalDataLake, WriteMode
from bagelquant_data.lake.snapshot import SnapshotRef

ScheduleUnit = Literal["minutes", "hours", "days"]
ParallelMode = Literal["thread"]
TushareTableKind = Literal["price", "fundamental", "fundamental_vip"]


@dataclass(frozen=True, slots=True)
class UpdateSchedule:
    """Simple periodic update schedule."""

    every: int
    unit: ScheduleUnit = "days"

    def interval(self) -> timedelta:
        """Return schedule interval."""

        if self.unit == "minutes":
            return timedelta(minutes=self.every)
        if self.unit == "hours":
            return timedelta(hours=self.every)
        return timedelta(days=self.every)


@dataclass(slots=True)
class UpdateJob:
    """A repeatable provider-to-lake update job."""

    source_name: str
    request: DataRequest
    schedule: UpdateSchedule
    mode: WriteMode = "overwrite"
    last_run_at: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def due(self, now: datetime | None = None) -> bool:
        """Return whether the job should run."""

        current = now or datetime.now(UTC)
        if self.last_run_at is None:
            return True
        return current - self.last_run_at >= self.schedule.interval()


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
        self._jobs: dict[str, UpdateJob] = {}

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

    def define_universe(
        self,
        source: str,
        name: str,
        asset_ids: list[str],
    ) -> SnapshotRef:
        """Define a source universe as a subset of All."""

        return self.lake.define_universe(source, name, asset_ids)

    def universe(self, source: str, name: str = "All") -> tuple[str, ...]:
        """Return a source universe."""

        return self.lake.universe(source, name)

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
    ) -> tuple[SnapshotRef, ...]:
        """Update Tushare All universe into the local lake."""

        source = self.registry.resolve("tushare")
        resolved_end = _normalize_date(end_date or date.today())
        resolved_start = _normalize_date(start_date)
        if resolved_start > resolved_end:
            raise ValueError("start_date must not be after end_date")

        stock_basic = source.read(DataRequest(dataset="stock_basic"))
        self.lake.write("tushare", "stock_basic", stock_basic, mode="overwrite")
        all_codes = [str(code) for code in stock_basic["ts_code"].dropna().tolist()]

        table_kind = (
            kind
            or (
                "price"
                if table in {"daily", "index_daily"}
                else "fundamental_vip"
                if table.endswith("_vip")
                else "fundamental"
            )
        )
        if table_kind == "price":
            return self._update_tushare_price_table(
                source=source,
                table=table,
                start_date=resolved_start,
                end_date=resolved_end,
                workers=workers,
                parallel=parallel,
            )
        if table_kind == "fundamental_vip":
            return self._update_tushare_fundamental_vip_table(
                source=source,
                table=table,
                start_date=resolved_start,
                end_date=resolved_end,
                workers=workers,
                parallel=parallel,
            )
        return self._update_tushare_fundamental_table(
            source=source,
            table=table,
            all_codes=all_codes,
            start_date=resolved_start,
            end_date=resolved_end,
            workers=workers,
            parallel=parallel,
        )

    def register_job(self, name: str, job: UpdateJob, *, replace: bool = False) -> None:
        """Register a periodic update job."""

        if name in self._jobs and not replace:
            raise ValueError(f"Update job already registered: {name}")
        self._jobs[name] = job

    def periodic_update(
        self,
        name: str,
        *,
        source_name: str,
        request: DataRequest,
        schedule: UpdateSchedule,
        mode: WriteMode = "overwrite",
        replace: bool = False,
    ) -> UpdateJob:
        """Configure a periodic provider-to-lake update job."""

        job = UpdateJob(
            source_name=source_name,
            request=request,
            schedule=schedule,
            mode=mode,
        )
        self.register_job(name, job, replace=replace)
        return job

    def jobs(self) -> Mapping[str, UpdateJob]:
        """Return registered jobs."""

        return dict(self._jobs)

    def run_due(self, now: datetime | None = None) -> tuple[SnapshotRef, ...]:
        """Run due jobs once and return created snapshots."""

        current = now or datetime.now(UTC)
        snapshots = []
        for job in self._jobs.values():
            if job.due(current):
                snapshots.append(
                    self.update(job.source_name, job.request, mode=job.mode)
                )
                job.last_run_at = current
        return tuple(snapshots)

    def _update_tushare_price_table(
        self,
        *,
        source: DataSource,
        table: str,
        start_date: date,
        end_date: date,
        workers: int,
        parallel: ParallelMode,
    ) -> tuple[SnapshotRef, ...]:
        dates = _date_range(start_date, end_date)

        def read_day(day: date) -> pd.DataFrame:
            return source.read(
                DataRequest(
                    dataset=table,
                    filters={"trade_date": day.strftime("%Y%m%d")},
                )
            )

        frames = _parallel_map(read_day, dates, workers=workers, parallel=parallel)
        data = pd.concat(
            [frame for frame in frames if not frame.empty],
            ignore_index=True,
        )
        if data.empty:
            return ()
        return (
            self.lake.write(
                "tushare",
                table,
                data,
                mode="overwrite",
                partition_column="trade_date",
                metadata={
                    "update_strategy": "day_by_day_all",
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                },
            ),
        )

    def _update_tushare_fundamental_table(
        self,
        *,
        source: DataSource,
        table: str,
        all_codes: list[str],
        start_date: date,
        end_date: date,
        workers: int,
        parallel: ParallelMode,
    ) -> tuple[SnapshotRef, ...]:
        existing = _existing_table(self.lake, "tushare", table)

        def read_code(ts_code: str) -> pd.DataFrame:
            code_start = _incremental_start(existing, ts_code, start_date)
            if code_start > end_date:
                return pd.DataFrame()
            return source.read(
                DataRequest(
                    dataset=table,
                    filters={"ts_code": ts_code},
                    start_date=code_start,
                    end_date=end_date,
                )
            )

        frames = _parallel_map(read_code, all_codes, workers=workers, parallel=parallel)
        changes = pd.concat(
            [frame for frame in frames if not frame.empty],
            ignore_index=True,
        )
        if changes.empty:
            return ()
        mode: WriteMode = "append" if existing is not None else "overwrite"
        return (
            self.lake.write(
                "tushare",
                table,
                changes,
                mode=mode,
                partition_column="f_ann_date",
                metadata={
                    "update_strategy": "id_by_id_incremental",
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                },
            ),
        )

    def _update_tushare_fundamental_vip_table(
        self,
        *,
        source: DataSource,
        table: str,
        start_date: date,
        end_date: date,
        workers: int,
        parallel: ParallelMode,
    ) -> tuple[SnapshotRef, ...]:
        existing = _existing_table(self.lake, "tushare", table)
        periods = _incremental_periods(existing, start_date, end_date)

        def read_period(period: date) -> pd.DataFrame:
            return source.read(
                DataRequest(
                    dataset=table,
                    filters={"period": period.strftime("%Y%m%d")},
                )
            )

        frames = _parallel_map(read_period, periods, workers=workers, parallel=parallel)
        changes = pd.concat(
            [frame for frame in frames if not frame.empty],
            ignore_index=True,
        )
        if changes.empty:
            return ()
        mode: WriteMode = "append" if existing is not None else "overwrite"
        return (
            self.lake.write(
                "tushare",
                table,
                changes,
                mode=mode,
                partition_column="f_ann_date",
                metadata={
                    "update_strategy": "season_by_season_incremental",
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                },
            ),
        )


def _parallel_map(
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
        return [future.result() for future in as_completed(futures)]


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
    return max(dates) + timedelta(days=1)


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
    return datetime.strptime(value, "%Y%m%d").date()
