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
TUSHARE_PRICE_UPDATE_RECORDS = "__tushare_price_update_records"
TUSHARE_FUNDAMENTAL_UPDATE_RECORDS = "__tushare_fundamental_update_records"


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
        write_batch_size: int = 20,
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
            write_batch_size=write_batch_size,
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
        write_batch_size: int = 20,
    ) -> tuple[SnapshotRef, ...]:
        """Execute provider jobs from a confirmed Tushare update report."""

        _validate_parallel(workers=workers, parallel=parallel)
        if write_batch_size < 1:
            raise ValueError("write_batch_size must be greater than or equal to 1")
        source = self.registry.resolve("tushare")
        refs: list[SnapshotRef] = []
        catalog_assets: dict[str, set[str]] = {}
        catalog_fields: dict[str, set[str]] = {}
        completed = 0
        total = len(report.jobs)
        write_lock = Lock()
        pending_writes: dict[
            tuple[
                str,
                WriteMode,
                str | None,
                str,
                tuple[str, ...],
            ],
            list[tuple[TushareUpdateJob, pd.DataFrame, int]],
        ] = {}
        price_records = (
            _read_update_records(
                self.lake,
                TUSHARE_PRICE_UPDATE_RECORDS,
                _empty_price_update_records(),
            )
            if any(job.kind == "price" for job in report.jobs)
            else _empty_price_update_records()
        )
        fundamental_records = (
            _read_update_records(
                self.lake,
                TUSHARE_FUNDAMENTAL_UPDATE_RECORDS,
                _empty_fundamental_update_records(),
            )
            if any(job.kind == "fundamental" for job in report.jobs)
            else _empty_fundamental_update_records()
        )
        price_records_changed = False
        fundamental_records_changed = False
        existing_fundamentals = {
            table: _existing_table(self.lake, "tushare", table)
            for table in {
                job.table for job in report.jobs if job.kind == "fundamental"
            }
        }

        def read_job(
            job: TushareUpdateJob,
        ) -> tuple[TushareUpdateJob, pd.DataFrame | None, Exception | None]:
            _emit_progress(
                progress,
                table=job.table,
                kind=job.kind,
                item=job.item,
                completed=completed,
                total=total,
                rows_written=0,
                snapshot=None,
                status="started",
                filters=job.filters,
            )
            try:
                if job.table == "stock_basic":
                    return job, self._read_tushare_stock_basic(source), None
                return job, source.read(
                    DataRequest(
                        dataset=job.table,
                        filters=job.filters,
                        start_date=job.start_date,
                        end_date=job.end_date,
                    )
                ), None
            except Exception as exc:
                return job, None, exc

        def flush_batch(
            key: tuple[str, WriteMode, str | None, str, tuple[str, ...]],
        ) -> None:
            nonlocal price_records
            nonlocal price_records_changed
            nonlocal fundamental_records
            nonlocal fundamental_records_changed
            batch = pending_writes.pop(key, [])
            if not batch:
                return
            sample = batch[0][0]
            batch_data = pd.concat(
                [data for _, data, _ in batch],
                axis=0,
                ignore_index=False,
            )
            ref = self.lake.write(
                "tushare",
                sample.table,
                batch_data,
                mode=sample.mode,
                partition_column=sample.partition_column,
                partition_granularity=sample.partition_granularity,
                metadata=_batch_metadata(batch),
                update_catalogs=False,
            )
            refs.append(ref)
            assets, fields = _catalog_entries(batch_data)
            catalog_assets.setdefault(sample.table, set()).update(assets)
            catalog_fields.setdefault(sample.table, set()).update(fields)
            for job, data, completed_count in batch:
                if job.kind == "price":
                    price_records = _upsert_price_update_record(
                        price_records,
                        job,
                        ref,
                    )
                    price_records_changed = True
                elif job.kind == "fundamental":
                    fundamental_records = _upsert_fundamental_update_record(
                        fundamental_records,
                        job,
                        data,
                        ref,
                    )
                    fundamental_records_changed = True
                _emit_progress(
                    progress,
                    table=job.table,
                    kind=job.kind,
                    item=job.item,
                    completed=completed_count,
                    total=total,
                    rows_written=len(data),
                    snapshot=ref,
                    status="succeeded",
                    filters=job.filters,
                )

        for job, data, error in _parallel_iter(
            read_job,
            report.jobs,
            workers=workers,
            parallel=parallel,
        ):
            with write_lock:
                if error is not None:
                    completed += 1
                    _emit_progress(
                        progress,
                        table=job.table,
                        kind=job.kind,
                        item=job.item,
                        completed=completed,
                        total=total,
                        rows_written=0,
                        snapshot=None,
                        status="failed",
                        error=str(error),
                        filters=job.filters,
                    )
                    continue
                if data is None:
                    data = pd.DataFrame()
                if job.kind == "fundamental":
                    data = _filter_incremental_rows(
                        existing_fundamentals.get(job.table),
                        data,
                    )
                completed += 1
                if data.empty:
                    if job.kind == "price":
                        price_records = _upsert_price_update_record(
                            price_records,
                            job,
                            None,
                        )
                        price_records_changed = True
                    elif job.kind == "fundamental":
                        fundamental_records = _upsert_fundamental_update_record(
                            fundamental_records,
                            job,
                            data,
                            None,
                        )
                        fundamental_records_changed = True
                    _emit_progress(
                        progress,
                        table=job.table,
                        kind=job.kind,
                        item=job.item,
                        completed=completed,
                        total=total,
                        rows_written=0,
                        snapshot=None,
                        status="succeeded",
                        filters=job.filters,
                    )
                    continue
                key = _write_batch_key(job)
                pending_writes.setdefault(key, []).append((job, data, completed))
                if len(pending_writes[key]) >= write_batch_size:
                    flush_batch(key)
        for key in list(pending_writes):
            flush_batch(key)
        if price_records_changed:
            _write_update_records(
                self.lake,
                TUSHARE_PRICE_UPDATE_RECORDS,
                _normalize_price_update_records(price_records),
            )
        if fundamental_records_changed:
            _write_update_records(
                self.lake,
                TUSHARE_FUNDAMENTAL_UPDATE_RECORDS,
                _normalize_fundamental_update_records(fundamental_records),
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

    def rebuild_tushare_update_records(
        self,
        *,
        specs: list[TushareTableUpdateSpec] | tuple[TushareTableUpdateSpec, ...],
        start_date: str | date | datetime = "2000-01-01",
        end_date: str | date | datetime | None = None,
    ) -> None:
        """Rebuild compact update-record system tables for configured Tushare specs."""

        resolved_start = _normalize_date(start_date)
        resolved_end = _normalize_date(end_date or date.today())
        for spec in specs:
            kind = _resolve_tushare_table_kind(spec.table, spec.kind)
            if kind == "price":
                self._ensure_tushare_price_update_records(
                    spec.table,
                    start_date=resolved_start,
                    end_date=resolved_end,
                    trading_calendar=spec.trading_calendar
                    or _default_tushare_calendar_ref(),
                    rebuild=True,
                )
            elif kind == "fundamental":
                self._ensure_tushare_fundamental_update_records(
                    spec.table,
                    start_date=resolved_start,
                    universe=spec.universe or _default_tushare_universe_ref(),
                    rebuild=True,
                )

    def scan_tushare_data_lake(
        self,
        *,
        specs: list[TushareTableUpdateSpec] | tuple[TushareTableUpdateSpec, ...],
        start_date: str | date | datetime = "2000-01-01",
        end_date: str | date | datetime | None = None,
    ) -> None:
        """Rebuild Tushare update records from local lake state and config."""

        self.rebuild_tushare_update_records(
            specs=specs,
            start_date=start_date,
            end_date=end_date,
        )

    def scan_data_lake(
        self,
        *,
        specs: list[TushareTableUpdateSpec] | tuple[TushareTableUpdateSpec, ...],
        start_date: str | date | datetime = "2000-01-01",
        end_date: str | date | datetime | None = None,
    ) -> None:
        """Rebuild local update records for configured provider tables."""

        self.scan_tushare_data_lake(
            specs=specs,
            start_date=start_date,
            end_date=end_date,
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
        *,
        trading_calendar: TushareTradingCalendarRef,
    ) -> tuple[TushareUpdatePlan, tuple[TushareUpdateJob, ...]]:
        records = self._ensure_tushare_price_update_records(
            table,
            start_date=start_date,
            end_date=end_date,
            trading_calendar=trading_calendar,
        )
        candidate_dates = []
        for record in records.itertuples(index=False):
            if str(record.table) != table:
                continue
            if str(record.calendar) != trading_calendar.name:
                continue
            day = _parse_yyyymmdd(str(record.trade_date))
            if start_date <= day <= end_date and not _record_exists(record.exists):
                candidate_dates.append(day)
        dates = [
            day
            for day in sorted(candidate_dates)
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
                    "missing update-record trade_date values"
                    if dates
                    else "all requested trade_date values are marked complete"
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
        records = self._ensure_tushare_fundamental_update_records(
            table,
            start_date=start_date,
            universe=universe,
        )
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
        table_records = records.loc[
            (records["table"].astype(str) == table)
            & (records["universe"].astype(str) == universe.name)
        ]
        by_asset = {
            str(row.asset_id): row
            for row in table_records.itertuples(index=False)
        }
        for ts_code in ts_codes:
            record = by_asset.get(ts_code)
            update_start = _record_update_start(record, start_date)
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
                    "asset-level incremental requests from update records"
                    if jobs
                    else (
                        "all asset-level update-record dates are after requested "
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

    def _ensure_tushare_price_update_records(
        self,
        table: str,
        *,
        start_date: date,
        end_date: date,
        trading_calendar: TushareTradingCalendarRef,
        rebuild: bool = False,
    ) -> pd.DataFrame:
        records = (
            _empty_price_update_records()
            if rebuild
            else _read_update_records(
                self.lake,
                TUSHARE_PRICE_UPDATE_RECORDS,
                _empty_price_update_records(),
            )
        )
        trading_dates = _local_trading_dates(
            self.lake,
            "tushare",
            trading_calendar,
            start_date,
            end_date,
        )
        existing_keys = {
            (
                str(row.table),
                str(row.calendar),
                str(row.trade_date),
            )
            for row in records.itertuples(index=False)
        }
        missing_days = [
            day
            for day in trading_dates
            if (table, trading_calendar.name, day.strftime("%Y%m%d"))
            not in existing_keys
        ]
        existing_dates = (
            _existing_dates(self.lake, "tushare", table) if missing_days else set()
        )
        rows = []
        for day in missing_days:
            trade_date = day.strftime("%Y%m%d")
            rows.append(
                {
                    "source": "tushare",
                    "table": table,
                    "calendar": trading_calendar.name,
                    "trade_date": trade_date,
                    "exists": day in existing_dates,
                    "last_snapshot_id": "",
                    "last_updated_at": "",
                }
            )
        if rows:
            records = pd.concat(
                [records, pd.DataFrame(rows)],
                axis=0,
                ignore_index=True,
            )
        records = _normalize_price_update_records(records)
        _write_update_records(
            self.lake,
            TUSHARE_PRICE_UPDATE_RECORDS,
            records,
        )
        return records

    def _ensure_tushare_fundamental_update_records(
        self,
        table: str,
        *,
        start_date: date,
        universe: TushareUniverseRef,
        rebuild: bool = False,
    ) -> pd.DataFrame:
        records = (
            _empty_fundamental_update_records()
            if rebuild
            else _read_update_records(
                self.lake,
                TUSHARE_FUNDAMENTAL_UPDATE_RECORDS,
                _empty_fundamental_update_records(),
            )
        )
        ts_codes = _local_tushare_codes(self.lake, universe=universe)
        existing_keys = {
            (
                str(row.table),
                str(row.universe),
                str(row.asset_id),
            )
            for row in records.itertuples(index=False)
        }
        missing_codes = [
            ts_code
            for ts_code in ts_codes
            if (table, universe.name, ts_code) not in existing_keys
        ]
        existing_latest = (
            _fundamental_latest_dates(self.lake, table) if missing_codes else {}
        )
        rows = []
        for ts_code in missing_codes:
            latest = existing_latest.get(ts_code)
            rows.append(
                {
                    "source": "tushare",
                    "table": table,
                    "universe": universe.name,
                    "asset_id": ts_code,
                    "latest_date": latest.strftime("%Y%m%d") if latest else "",
                    "exists": latest is not None and latest >= start_date,
                    "last_snapshot_id": "",
                    "last_updated_at": "",
                }
            )
        if rows:
            records = pd.concat(
                [records, pd.DataFrame(rows)],
                axis=0,
                ignore_index=True,
            )
        records = _normalize_fundamental_update_records(records)
        _write_update_records(
            self.lake,
            TUSHARE_FUNDAMENTAL_UPDATE_RECORDS,
            records,
        )
        return records

    def _mark_tushare_update_record(
        self,
        job: TushareUpdateJob,
        data: pd.DataFrame,
        snapshot: SnapshotRef | None,
    ) -> None:
        if job.kind == "price":
            self._mark_tushare_price_update_record(job, snapshot)
        elif job.kind == "fundamental":
            self._mark_tushare_fundamental_update_record(job, data, snapshot)

    def _mark_tushare_price_update_record(
        self,
        job: TushareUpdateJob,
        snapshot: SnapshotRef | None,
    ) -> None:
        trade_date = str(job.filters.get("trade_date") or "")
        if not trade_date:
            return
        records = _read_update_records(
            self.lake,
            TUSHARE_PRICE_UPDATE_RECORDS,
            _empty_price_update_records(),
        )
        calendar = job.trading_calendar or ""
        if records.empty:
            records = _empty_price_update_records()
        mask = (
            (records["table"].astype(str) == job.table)
            & (records["calendar"].astype(str) == calendar)
            & (records["trade_date"].astype(str) == trade_date)
        )
        if not mask.any():
            records = pd.concat(
                [
                    records,
                    pd.DataFrame(
                        [
                            {
                                "source": "tushare",
                                "table": job.table,
                                "calendar": calendar,
                                "trade_date": trade_date,
                                "exists": True,
                                "last_snapshot_id": "",
                                "last_updated_at": "",
                            }
                        ]
                    ),
                ],
                axis=0,
                ignore_index=True,
            )
            mask = records.index == records.index[-1]
        records.loc[mask, "exists"] = True
        records.loc[mask, "last_snapshot_id"] = (
            snapshot.snapshot_id if snapshot is not None else ""
        )
        records.loc[mask, "last_updated_at"] = datetime.now(UTC).isoformat()
        _write_update_records(
            self.lake,
            TUSHARE_PRICE_UPDATE_RECORDS,
            _normalize_price_update_records(records),
        )

    def _mark_tushare_fundamental_update_record(
        self,
        job: TushareUpdateJob,
        data: pd.DataFrame,
        snapshot: SnapshotRef | None,
    ) -> None:
        asset_id = str(job.filters.get("ts_code") or job.item or "")
        if not asset_id:
            return
        records = _read_update_records(
            self.lake,
            TUSHARE_FUNDAMENTAL_UPDATE_RECORDS,
            _empty_fundamental_update_records(),
        )
        universe = job.universe or ""
        if records.empty:
            records = _empty_fundamental_update_records()
        mask = (
            (records["table"].astype(str) == job.table)
            & (records["universe"].astype(str) == universe)
            & (records["asset_id"].astype(str) == asset_id)
        )
        if not mask.any():
            records = pd.concat(
                [
                    records,
                    pd.DataFrame(
                        [
                            {
                                "source": "tushare",
                                "table": job.table,
                                "universe": universe,
                                "asset_id": asset_id,
                                "latest_date": "",
                                "exists": False,
                                "last_snapshot_id": "",
                                "last_updated_at": "",
                            }
                        ]
                    ),
                ],
                axis=0,
                ignore_index=True,
            )
            mask = records.index == records.index[-1]
        latest_date = _latest_fundamental_job_date(data, job.end_date)
        records.loc[mask, "latest_date"] = (
            latest_date.strftime("%Y%m%d") if latest_date is not None else ""
        )
        records.loc[mask, "exists"] = True
        records.loc[mask, "last_snapshot_id"] = (
            snapshot.snapshot_id if snapshot is not None else ""
        )
        records.loc[mask, "last_updated_at"] = datetime.now(UTC).isoformat()
        _write_update_records(
            self.lake,
            TUSHARE_FUNDAMENTAL_UPDATE_RECORDS,
            _normalize_fundamental_update_records(records),
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


def _empty_price_update_records() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "source",
            "table",
            "calendar",
            "trade_date",
            "exists",
            "last_snapshot_id",
            "last_updated_at",
        ]
    )


def _empty_fundamental_update_records() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "source",
            "table",
            "universe",
            "asset_id",
            "latest_date",
            "exists",
            "last_snapshot_id",
            "last_updated_at",
        ]
    )


def _read_update_records(
    lake: LocalDataLake,
    table: str,
    empty: pd.DataFrame,
) -> pd.DataFrame:
    try:
        data = lake.read("tushare", table)
    except DatasetNotFoundError:
        return empty.copy()
    for column in empty.columns:
        if column not in data.columns:
            data[column] = empty[column]
    return data.loc[:, list(empty.columns)].copy(deep=True)


def _write_update_records(
    lake: LocalDataLake,
    table: str,
    data: pd.DataFrame,
) -> None:
    if data.empty:
        return
    lake.write(
        "tushare",
        table,
        data.reset_index(drop=True),
        mode="overwrite",
        metadata={
            "system_table": True,
            "update_records": True,
            "created_at": datetime.now(UTC).isoformat(),
        },
        update_catalogs=False,
    )


def _write_batch_key(
    job: TushareUpdateJob,
) -> tuple[str, WriteMode, str | None, str, tuple[str, ...]]:
    return (
        job.table,
        job.mode,
        job.partition_column,
        job.partition_granularity,
        tuple(sorted(str(key) for key in job.metadata)),
    )


def _batch_metadata(
    batch: list[tuple[TushareUpdateJob, pd.DataFrame, int]],
) -> Mapping[str, Any]:
    if len(batch) == 1:
        return batch[0][0].metadata
    first = batch[0][0]
    shared: dict[str, Any] = {
        "update_strategy": first.metadata.get("update_strategy", "batch"),
        "batch_size": len(batch),
        "batched_update": True,
        "jobs": [dict(job.metadata) for job, _, _ in batch],
    }
    for key in ("start_date", "end_date", "trading_calendar", "universe"):
        values = {
            job.metadata.get(key)
            for job, _, _ in batch
            if job.metadata.get(key) is not None
        }
        if len(values) == 1:
            shared[key] = values.pop()
    return shared


def _upsert_price_update_record(
    records: pd.DataFrame,
    job: TushareUpdateJob,
    snapshot: SnapshotRef | None,
) -> pd.DataFrame:
    trade_date = str(job.filters.get("trade_date") or "")
    if not trade_date:
        return records
    updated = records.copy(deep=True)
    for column in _empty_price_update_records().columns:
        if column not in updated.columns:
            updated[column] = ""
    calendar = job.trading_calendar or ""
    mask = (
        (updated["table"].astype(str) == job.table)
        & (updated["calendar"].astype(str) == calendar)
        & (updated["trade_date"].astype(str) == trade_date)
    )
    if not mask.any():
        updated = pd.concat(
            [
                updated,
                pd.DataFrame(
                    [
                        {
                            "source": "tushare",
                            "table": job.table,
                            "calendar": calendar,
                            "trade_date": trade_date,
                            "exists": True,
                            "last_snapshot_id": "",
                            "last_updated_at": "",
                        }
                    ]
                ),
            ],
            axis=0,
            ignore_index=True,
        )
        mask = updated.index == updated.index[-1]
    updated.loc[mask, "exists"] = True
    updated.loc[mask, "last_snapshot_id"] = (
        snapshot.snapshot_id if snapshot is not None else ""
    )
    updated.loc[mask, "last_updated_at"] = datetime.now(UTC).isoformat()
    return updated


def _upsert_fundamental_update_record(
    records: pd.DataFrame,
    job: TushareUpdateJob,
    data: pd.DataFrame,
    snapshot: SnapshotRef | None,
) -> pd.DataFrame:
    asset_id = str(job.filters.get("ts_code") or job.item or "")
    if not asset_id:
        return records
    updated = records.copy(deep=True)
    for column in _empty_fundamental_update_records().columns:
        if column not in updated.columns:
            updated[column] = ""
    universe = job.universe or ""
    mask = (
        (updated["table"].astype(str) == job.table)
        & (updated["universe"].astype(str) == universe)
        & (updated["asset_id"].astype(str) == asset_id)
    )
    if not mask.any():
        updated = pd.concat(
            [
                updated,
                pd.DataFrame(
                    [
                        {
                            "source": "tushare",
                            "table": job.table,
                            "universe": universe,
                            "asset_id": asset_id,
                            "latest_date": "",
                            "exists": False,
                            "last_snapshot_id": "",
                            "last_updated_at": "",
                        }
                    ]
                ),
            ],
            axis=0,
            ignore_index=True,
        )
        mask = updated.index == updated.index[-1]
    latest_date = _latest_fundamental_job_date(data, job.end_date)
    updated.loc[mask, "latest_date"] = (
        latest_date.strftime("%Y%m%d") if latest_date is not None else ""
    )
    updated.loc[mask, "exists"] = True
    updated.loc[mask, "last_snapshot_id"] = (
        snapshot.snapshot_id if snapshot is not None else ""
    )
    updated.loc[mask, "last_updated_at"] = datetime.now(UTC).isoformat()
    return updated


def _normalize_price_update_records(data: pd.DataFrame) -> pd.DataFrame:
    records = data.copy(deep=True)
    for column in _empty_price_update_records().columns:
        if column not in records.columns:
            records[column] = ""
    records["source"] = records["source"].astype(str)
    records["table"] = records["table"].astype(str)
    records["calendar"] = records["calendar"].astype(str)
    records["trade_date"] = records["trade_date"].astype(str)
    records["exists"] = records["exists"].map(_record_exists)
    records["last_snapshot_id"] = records["last_snapshot_id"].fillna("").astype(str)
    records["last_updated_at"] = records["last_updated_at"].fillna("").astype(str)
    return records.drop_duplicates(
        ["source", "table", "calendar", "trade_date"],
        keep="last",
    ).sort_values(
        ["source", "table", "calendar", "trade_date"],
        ignore_index=True,
    )


def _normalize_fundamental_update_records(data: pd.DataFrame) -> pd.DataFrame:
    records = data.copy(deep=True)
    for column in _empty_fundamental_update_records().columns:
        if column not in records.columns:
            records[column] = ""
    records["source"] = records["source"].astype(str)
    records["table"] = records["table"].astype(str)
    records["universe"] = records["universe"].astype(str)
    records["asset_id"] = records["asset_id"].astype(str)
    records["latest_date"] = records["latest_date"].fillna("").astype(str)
    records["exists"] = records["exists"].map(_record_exists)
    records["last_snapshot_id"] = records["last_snapshot_id"].fillna("").astype(str)
    records["last_updated_at"] = records["last_updated_at"].fillna("").astype(str)
    return records.drop_duplicates(
        ["source", "table", "universe", "asset_id"],
        keep="last",
    ).sort_values(
        ["source", "table", "universe", "asset_id"],
        ignore_index=True,
    )


def _record_exists(value: object) -> bool:
    if pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text in {"1", "true", "t", "yes", "y"}


def _record_update_start(record: object | None, default_start: date) -> date:
    if record is None:
        return default_start
    latest = str(getattr(record, "latest_date", "") or "")
    if not latest:
        return default_start
    return max(_parse_yyyymmdd(latest), default_start)


def _fundamental_latest_dates(
    lake: LocalDataLake,
    table: str,
) -> dict[str, date]:
    try:
        existing = lake.read("tushare", table, columns=("ts_code", "f_ann_date"))
    except DatasetNotFoundError:
        return {}
    except Exception:
        existing = _existing_table(lake, "tushare", table)
    if (
        existing is None
        or existing.empty
        or "ts_code" not in existing.columns
        or "f_ann_date" not in existing.columns
    ):
        return {}
    latest: dict[str, date] = {}
    rows = existing[["ts_code", "f_ann_date"]].dropna().itertuples(index=False)
    for ts_code, raw_date in rows:
        parsed = _parse_yyyymmdd(str(raw_date))
        asset_id = str(ts_code)
        if asset_id not in latest or parsed > latest[asset_id]:
            latest[asset_id] = parsed
    return latest


def _latest_fundamental_job_date(
    data: pd.DataFrame,
    fallback: date | None,
) -> date | None:
    if not data.empty and "f_ann_date" in data.columns:
        raw_dates = [
            str(value) for value in data["f_ann_date"].tolist() if pd.notna(value)
        ]
        dates = [_parse_yyyymmdd(value) for value in raw_dates]
        if dates:
            return max(dates)
    return fallback


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
    status: str | None = None,
    error: str | None = None,
    filters: Mapping[str, Any] | None = None,
) -> None:
    if progress is None:
        return
    event: dict[str, Any] = {
        "table": table,
        "kind": kind,
        "item": item,
        "completed": completed,
        "total": total,
        "rows_written": rows_written,
        "snapshot": snapshot,
    }
    if status is not None:
        event["status"] = status
    if error is not None:
        event["error"] = error
    if filters is not None:
        event["filters"] = dict(filters)
    progress(event)


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
