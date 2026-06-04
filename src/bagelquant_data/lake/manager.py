"""Data lake management and update orchestration."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from threading import Lock
from typing import Any, Literal

import pandas as pd

from bagelquant_data.datasource.base import DataRequest, DataSource
from bagelquant_data.datasource.registry import DataSourceRegistry
from bagelquant_data.lake.local import LocalDataLake, WriteMode
from bagelquant_data.lake.snapshot import SnapshotRef

ParallelMode = Literal["thread"]
TushareTableKind = Literal["general", "price", "fundamental", "fundamental_vip"]
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
    ) -> tuple[SnapshotRef, ...]:
        """Update Tushare All universe into the local lake."""

        source = self.registry.resolve("tushare")
        resolved_end = _normalize_date(end_date or date.today())
        resolved_start = _normalize_date(start_date)
        if resolved_start > resolved_end:
            raise ValueError("start_date must not be after end_date")

        stock_basic = self.update_tushare_stock_basic()
        stock_basic_data = self.lake.read("tushare", "stock_basic")
        all_codes = [
            str(code) for code in stock_basic_data["ts_code"].dropna().tolist()
        ]

        if kind == "general" or table == "stock_basic":
            return (stock_basic,)

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
                progress=progress,
            )
        if table_kind == "fundamental_vip":
            return self._update_tushare_fundamental_vip_table(
                source=source,
                table=table,
                start_date=resolved_start,
                end_date=resolved_end,
                workers=workers,
                parallel=parallel,
                progress=progress,
            )
        return self._update_tushare_fundamental_table(
            source=source,
            table=table,
            all_codes=all_codes,
            start_date=resolved_start,
            end_date=resolved_end,
            workers=workers,
            parallel=parallel,
            progress=progress,
        )

    def update_tushare_stock_basic(self) -> SnapshotRef:
        """Refresh the full Tushare stock universe table."""

        source = self.registry.resolve("tushare")
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
        return self.lake.write(
            "tushare",
            "stock_basic",
            stock_basic.reset_index(drop=True),
            mode="overwrite",
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
                        metadata={
                            "update_strategy": "day_by_day_incremental",
                            "trade_date": day.isoformat(),
                            "start_date": start_date.isoformat(),
                            "end_date": end_date.isoformat(),
                        },
                    )
                    refs.append(ref)
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
        return tuple(refs)

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
        progress: ProgressCallback | None,
    ) -> tuple[SnapshotRef, ...]:
        existing = _existing_table(self.lake, "tushare", table)
        targets: list[tuple[str, date]] = []
        for ts_code in all_codes:
            code_start = _incremental_start(existing, ts_code, start_date)
            if code_start <= end_date:
                targets.append((ts_code, code_start))
        refs: list[SnapshotRef] = []
        completed = 0
        total = len(targets)
        write_lock = Lock()

        def read_code(target: tuple[str, date]) -> tuple[str, pd.DataFrame]:
            ts_code, code_start = target
            return ts_code, source.read(
                DataRequest(
                    dataset=table,
                    filters={"ts_code": ts_code},
                    start_date=code_start,
                    end_date=end_date,
                )
            )

        for ts_code, data in _parallel_iter(
            read_code,
            targets,
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
                        metadata={
                            "update_strategy": "id_by_id_incremental",
                            "asset_id": ts_code,
                            "start_date": start_date.isoformat(),
                            "end_date": end_date.isoformat(),
                        },
                    )
                    refs.append(ref)
                completed += 1
                _emit_progress(
                    progress,
                    table=table,
                    kind="fundamental",
                    item=ts_code,
                    completed=completed,
                    total=total,
                    rows_written=0 if data.empty else len(data),
                    snapshot=ref,
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
                        metadata={
                            "update_strategy": "season_by_season_incremental",
                            "period": period.isoformat(),
                            "start_date": start_date.isoformat(),
                            "end_date": end_date.isoformat(),
                        },
                    )
                    refs.append(ref)
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
