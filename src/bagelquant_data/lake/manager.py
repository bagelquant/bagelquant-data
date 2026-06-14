"""Polars-native data lake manager facade."""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from typing import Any, cast

import polars as pl

from bagelquant_data.datasource.base import DataRequest, DataSource
from bagelquant_data.datasource.registry import DataSourceRegistry, default_registry
from bagelquant_data.lake.local import LocalDataLake, WriteMode
from bagelquant_data.lake.snapshot import SnapshotRef
from bagelquant_data.lake.tushare_update import (
    TushareCallStatus,
    TushareTableKind,
    TushareTableUpdateSpec,
    TushareTradingCalendarRef,
    TushareUniverseRef,
    TushareUpdateJob,
    TushareUpdatePlan,
    TushareUpdateReport,
)
from bagelquant_data.utils.exceptions import DatasetNotFoundError
from bagelquant_data.utils.normalize import (
    as_date,
    normalize_table_columns,
    parse_date,
    parse_tushare_date,
    tushare_date,
)

TUSHARE_CALL_LOG_TABLE = "__api_call_log"
TUSHARE_UPDATE_TABLES = "__update_tables"
PRICE_TABLES = {"daily", "adj_factor", "index_daily"}
FUNDAMENTAL_TABLES = {"balancesheet", "income", "cashflow"}
CALL_LOG_SCHEMA = {
    "called_at": pl.String,
    "api_name": pl.String,
    "table": pl.String,
    "kind": pl.String,
    "item_key": pl.String,
    "item_value": pl.String,
    "request_start_date": pl.Date,
    "request_end_date": pl.Date,
    "data_min_time": pl.Date,
    "data_max_time": pl.Date,
    "rows": pl.Int64,
    "status": pl.String,
    "error": pl.String,
    "duration_ms": pl.Int64,
    "request_hash": pl.String,
    "snapshot_id": pl.String,
    "params_json": pl.String,
    "fields_json": pl.String,
}
UPDATE_TABLE_SCHEMA = {
    "table": pl.String,
    "kind": pl.String,
    "enabled": pl.Boolean,
    "created_at": pl.String,
    "updated_at": pl.String,
}


class DataLakeManager:
    """Small orchestration layer around source reads and Polars lake writes."""

    def __init__(
        self,
        lake: LocalDataLake,
        *,
        registry: DataSourceRegistry | None = None,
    ) -> None:
        self.lake = lake
        self.registry = registry or default_registry

    def add(
        self,
        source: str,
        dataset: str,
        data: pl.DataFrame,
        *,
        metadata: Mapping[str, Any] | None = None,
    ) -> SnapshotRef:
        return self.lake.add(source, dataset, data, metadata=metadata)

    def edit(
        self,
        source: str,
        dataset: str,
        data: pl.DataFrame,
        *,
        metadata: Mapping[str, Any] | None = None,
    ) -> SnapshotRef:
        return self.lake.edit(source, dataset, data, metadata=metadata)

    def delete(
        self,
        source: str,
        dataset: str | None = None,
        *,
        snapshot: str | None = None,
    ) -> None:
        self.lake.delete(source, dataset, snapshot=snapshot)

    def list_sources(self) -> tuple[str, ...]:
        return self.lake.list_sources()

    def list_datasets(self, source: str | None = None) -> tuple[tuple[str, str], ...]:
        return self.lake.list_datasets(source)

    def list_tables(self, source: str | None = None) -> tuple[tuple[str, str], ...]:
        return self.lake.list_tables(source)

    def snapshots(self, source: str, dataset: str) -> tuple[SnapshotRef, ...]:
        return self.lake.snapshots(source, dataset)

    def latest(self, source: str, dataset: str) -> SnapshotRef | None:
        return self.lake.latest(source, dataset)

    def update(
        self,
        source: str | DataSource,
        request: DataRequest,
        *,
        mode: WriteMode = "overwrite",
    ) -> SnapshotRef:
        resolved = self._source(source)
        return self.lake.write(
            resolved.name,
            request.dataset,
            resolved.read(request),
            mode=mode,
            metadata={"request": _request_payload(request)},
        )

    def ingest(
        self,
        source: str | DataSource,
        request: DataRequest,
        *,
        mode: WriteMode = "overwrite",
    ) -> SnapshotRef:
        return self.update(source, request, mode=mode)

    def update_tushare_stock_basic(self, **options: Any) -> SnapshotRef:
        source = self._source("tushare")
        frames: list[pl.DataFrame] = []
        log_rows: list[dict[str, Any]] = []
        for status in ("L", "D", "P"):
            request = DataRequest(
                dataset="stock_basic",
                filters={"list_status": status},
                start_date=options.get("start_date"),
                end_date=options.get("end_date"),
            )
            job = TushareUpdateJob(
                table="stock_basic",
                kind="general",
                filters=request.filters,
                start_date=request.start_date,
                end_date=request.end_date,
                item=f"list_status={status}",
                item_key="list_status",
                item_value=status,
                mode="overwrite",
            )
            started = time.perf_counter()
            called_at = datetime.now(UTC)
            try:
                data = source.read(request)
            except Exception as exc:
                self._append_tushare_call_log(
                    _call_log_row(
                        job,
                        request=request,
                        called_at=called_at,
                        data=None,
                        status="failed",
                        duration_ms=int((time.perf_counter() - started) * 1000),
                        error=str(exc),
                    )
                )
                raise
            frames.append(data)
            log_rows.append(
                _call_log_row(
                    job,
                    request=request,
                    called_at=called_at,
                    data=data,
                    status="empty" if data.is_empty() else "success",
                    duration_ms=int((time.perf_counter() - started) * 1000),
                )
            )
        combined = (
            pl.concat(frames, how="diagonal_relaxed")
            if frames
            else pl.DataFrame()
        )
        if "asset_id" in combined.columns:
            combined = combined.unique(subset=["asset_id"], keep="last")
        ref = self.lake.write(
            "tushare",
            "stock_basic",
            combined,
            mode="overwrite",
            metadata={"request": {"dataset": "stock_basic", "list_status": "L,D,P"}},
        )
        for row in log_rows:
            row["snapshot_id"] = ref.snapshot_id
            self._append_tushare_call_log(row)
        return ref

    def update_tushare_trading_calendar(self, **options: Any) -> SnapshotRef:
        refs, _ = self._execute_logged_tushare_request(
            TushareUpdateJob(
                table="trade_cal",
                kind="general",
                start_date=options.get("start_date"),
                end_date=options.get("end_date"),
                mode="overwrite",
                item_key="table",
                item_value="trade_cal",
            ),
        )
        if not refs:
            raise DatasetNotFoundError("Tushare trade_cal returned no rows")
        return refs[0]

    def scan_tushare_updates(
        self,
        specs: tuple[TushareTableUpdateSpec, ...] | list[TushareTableUpdateSpec],
        *,
        start_date: Any,
        end_date: Any | None = None,
        **_: Any,
    ) -> TushareUpdateReport:
        requested_start = as_date(start_date)
        requested_end = as_date(end_date or date.today())
        jobs: list[TushareUpdateJob] = []
        plans: list[TushareUpdatePlan] = []
        log = self.tushare_api_call_log(
            columns=(
                "table",
                "item_key",
                "item_value",
                "status",
                "data_max_time",
                "request_end_date",
            )
        )

        for spec in sorted(specs, key=_spec_sort_key):
            kind = spec.kind or _infer_tushare_kind(spec.table)
            if kind == "price":
                table_jobs = self._scan_tushare_price_jobs(
                    spec,
                    requested_start=requested_start,
                    requested_end=requested_end,
                    log=log,
                )
            elif kind in {"fundamental", "fundamental_vip"}:
                table_jobs = self._scan_tushare_fundamental_jobs(
                    spec,
                    requested_start=requested_start,
                    requested_end=requested_end,
                    log=log,
                )
            else:
                table_jobs = (
                    TushareUpdateJob(
                        table=spec.table,
                        kind=kind,
                        start_date=requested_start,
                        end_date=requested_end,
                        filters={},
                        item_key="table",
                        item_value=spec.table,
                    ),
                )
            jobs.extend(table_jobs)
            pending_items = tuple(job.item for job in table_jobs)
            effective_start = min(
                (job.start_date for job in table_jobs if job.start_date is not None),
                default=None,
            )
            plans.append(
                TushareUpdatePlan(
                    table=spec.table,
                    kind=kind,
                    requested_start=requested_start,
                    requested_end=requested_end,
                    effective_start=effective_start,
                    pending_items=pending_items,
                    reason="call log resume" if table_jobs else "call log up to date",
                    estimated_job_count=len(table_jobs),
                    status="pending" if table_jobs else "up_to_date",
                    universe=spec.universe.name if spec.universe else None,
                    trading_calendar=(
                        spec.trading_calendar.name if spec.trading_calendar else None
                    ),
                )
            )
        return TushareUpdateReport(
            generated_at=datetime.now(UTC),
            source="tushare",
            requested_start=requested_start,
            requested_end=requested_end,
            plans=tuple(plans),
            jobs=tuple(jobs),
        )

    def execute_tushare_update_report(
        self,
        report: TushareUpdateReport,
        *,
        mode: WriteMode = "append",
        progress: Any | None = None,
        continue_on_error: bool = False,
        **_: Any,
    ) -> tuple[SnapshotRef, ...]:
        refs: list[SnapshotRef] = []
        total = len(report.jobs)
        for index, job in enumerate(report.jobs, start=1):
            job_mode = job.mode or mode
            try:
                job_refs, event = self._execute_logged_tushare_request(
                    job, mode=job_mode
                )
                refs.extend(job_refs)
            except Exception as exc:
                event = {
                    "table": job.table,
                    "item": job.item or job.item_value,
                    "status": "failed",
                    "rows_written": 0,
                    "error": str(exc),
                }
                if progress is not None:
                    progress({**event, "completed": index, "total": total})
                if not continue_on_error:
                    raise
                continue
            if progress is not None:
                progress({**event, "completed": index, "total": total})
        return tuple(refs)

    def update_tushare_all(self, table: str = "daily", **options: Any) -> SnapshotRef:
        return self.update(
            "tushare",
            DataRequest(dataset=table, options=options),
            mode="overwrite",
        )

    def tushare_api_call_log(
        self, columns: tuple[str, ...] | None = None
    ) -> pl.DataFrame:
        try:
            return self.lake.read("tushare", TUSHARE_CALL_LOG_TABLE, columns=columns)
        except DatasetNotFoundError:
            if columns is None:
                return pl.DataFrame(schema=CALL_LOG_SCHEMA)
            return pl.DataFrame(
                schema={
                    column: dtype
                    for column, dtype in CALL_LOG_SCHEMA.items()
                    if column in columns
                }
            )

    def tushare_update_tables(self) -> pl.DataFrame:
        try:
            return self.lake.read("tushare", TUSHARE_UPDATE_TABLES)
        except DatasetNotFoundError:
            return pl.DataFrame(schema=UPDATE_TABLE_SCHEMA)

    def register_tushare_update_table(
        self,
        table: str,
        *,
        kind: str | None = None,
        enabled: bool = True,
    ) -> SnapshotRef:
        table = table.strip()
        if not table:
            raise ValueError("table is required")
        now = datetime.now(UTC).isoformat()
        existing = self.tushare_update_tables()
        if not existing.is_empty():
            existing = existing.filter(pl.col("table") != table)
        created_at = now
        row = {
            "table": table,
            "kind": kind or _infer_tushare_kind(table),
            "enabled": enabled,
            "created_at": created_at,
            "updated_at": now,
        }
        data = pl.concat(
            [existing, pl.DataFrame([row], schema=UPDATE_TABLE_SCHEMA)],
            how="diagonal_relaxed",
        )
        return self.lake.write(
            "tushare",
            TUSHARE_UPDATE_TABLES,
            data.sort("table"),
            mode="overwrite",
            metadata={"system": "tushare_update_tables"},
        )

    def remove_tushare_update_table(self, table: str) -> SnapshotRef:
        existing = self.tushare_update_tables()
        if existing.is_empty():
            return self.lake.write(
                "tushare",
                TUSHARE_UPDATE_TABLES,
                existing,
                mode="overwrite",
                metadata={"system": "tushare_update_tables"},
            )
        data = existing.filter(pl.col("table") != table)
        return self.lake.write(
            "tushare",
            TUSHARE_UPDATE_TABLES,
            data,
            mode="overwrite",
            metadata={"system": "tushare_update_tables"},
        )

    def tushare_update_specs(self) -> tuple[TushareTableUpdateSpec, ...]:
        tables = self.tushare_update_tables()
        if tables.is_empty():
            return ()
        universe = TushareUniverseRef(name="stock_basic", table="stock_basic")
        calendar = TushareTradingCalendarRef(name="trade_cal", table="trade_cal")
        specs: list[TushareTableUpdateSpec] = []
        for row in tables.filter(pl.col("enabled")).iter_rows(named=True):
            table = str(row["table"])
            kind = cast(TushareTableKind, row["kind"] or _infer_tushare_kind(table))
            specs.append(
                TushareTableUpdateSpec(
                    table=table,
                    kind=kind,
                    universe=universe,
                    trading_calendar=calendar if kind == "price" else None,
                )
            )
        return tuple(specs)

    def _source(self, source: str | DataSource) -> DataSource:
        if isinstance(source, str):
            return self.registry.resolve(source)
        return source

    def _scan_tushare_price_jobs(
        self,
        spec: TushareTableUpdateSpec,
        *,
        requested_start: date,
        requested_end: date,
        log: pl.DataFrame,
    ) -> tuple[TushareUpdateJob, ...]:
        latest = _latest_logged_date(log, table=spec.table, item_key="trade_date")
        start = (
            max(requested_start, latest + timedelta(days=1))
            if latest
            else requested_start
        )
        calendar = self._tushare_calendar_dates(
            spec.trading_calendar,
            start_date=start,
            end_date=requested_end,
        )
        return tuple(
            TushareUpdateJob(
                table=spec.table,
                kind="price",
                filters={"trade_date": tushare_date(value)},
                start_date=value,
                end_date=value,
                item=f"trade_date={tushare_date(value)}",
                item_key="trade_date",
                item_value=tushare_date(value),
                partition_column="time",
                partition_granularity="day",
                universe=spec.universe.name if spec.universe else None,
                trading_calendar=(
                    spec.trading_calendar.name if spec.trading_calendar else None
                ),
            )
            for value in calendar
        )

    def _scan_tushare_fundamental_jobs(
        self,
        spec: TushareTableUpdateSpec,
        *,
        requested_start: date,
        requested_end: date,
        log: pl.DataFrame,
    ) -> tuple[TushareUpdateJob, ...]:
        codes = self._tushare_universe_codes(spec.universe)
        local_latest_by_code = self._latest_local_dates_by_asset(spec.table)
        if not codes:
            latest = max(
                (
                    value
                    for value in (
                        _latest_logged_date(
                            log, table=spec.table, item_key="table"
                        ),
                        _latest_local_table_date(self.lake, spec.table),
                    )
                    if value is not None
                ),
                default=None,
            )
            if latest is not None and requested_end <= latest:
                return ()
            start = max(requested_start, latest) if latest else requested_start
            return (
                TushareUpdateJob(
                    table=spec.table,
                    kind=spec.kind or "fundamental",
                    filters={},
                    start_date=start,
                    end_date=requested_end,
                    item=f"table={spec.table}",
                    item_key="table",
                    item_value=spec.table,
                    partition_column="time",
                    partition_granularity="year",
                    universe=spec.universe.name if spec.universe else None,
                ),
            )
        jobs: list[TushareUpdateJob] = []
        for code in codes:
            latest = max(
                (
                    value
                    for value in (
                        _latest_logged_date(
                            log,
                            table=spec.table,
                            item_key="ts_code",
                            item_value=code,
                        ),
                        local_latest_by_code.get(code),
                    )
                    if value is not None
                ),
                default=None,
            )
            if latest is not None and requested_end <= latest:
                continue
            start = (
                max(requested_start, latest)
                if latest is not None
                else requested_start
            )
            if start > requested_end:
                continue
            jobs.append(
                TushareUpdateJob(
                    table=spec.table,
                    kind=spec.kind or "fundamental",
                    filters={"ts_code": code},
                    start_date=start,
                    end_date=requested_end,
                    item=f"ts_code={code}",
                    item_key="ts_code",
                    item_value=code,
                    partition_column="time",
                    partition_granularity="year",
                    universe=spec.universe.name if spec.universe else None,
                    trading_calendar=(
                        spec.trading_calendar.name if spec.trading_calendar else None
                    ),
                )
            )
        return tuple(jobs)

    def _latest_local_dates_by_asset(self, table: str) -> dict[str, date]:
        try:
            frame = self.lake.read("tushare", table, columns=("time", "asset_id"))
        except DatasetNotFoundError:
            return {}
        if not {"time", "asset_id"}.issubset(frame.columns):
            return {}
        latest = frame.group_by("asset_id").agg(pl.col("time").max().alias("time"))
        return {
            str(row["asset_id"]): row["time"]
            for row in latest.iter_rows(named=True)
            if isinstance(row["time"], date)
        }

    def _tushare_calendar_dates(
        self,
        ref: Any | None,
        *,
        start_date: date,
        end_date: date,
    ) -> tuple[date, ...]:
        table = ref.table if ref is not None else "trade_cal"
        try:
            frame = self.lake.read("tushare", table)
        except DatasetNotFoundError:
            return tuple(_date_range(start_date, end_date))
        if "is_open" in frame.columns:
            frame = frame.filter(
                pl.col("is_open").cast(pl.Boolean, strict=False).fill_null(False)
            )
        if "time" not in frame.columns:
            return tuple(_date_range(start_date, end_date))
        return tuple(
            value
            for value in frame.filter(
                (pl.col("time") >= pl.lit(start_date).cast(pl.Date))
                & (pl.col("time") <= pl.lit(end_date).cast(pl.Date))
            )
            .sort("time")["time"]
            .to_list()
            if isinstance(value, date)
        )

    def _tushare_universe_codes(self, ref: Any | None) -> tuple[str, ...]:
        table = ref.table if ref is not None else "stock_basic"
        requested_column = ref.code_column if ref is not None else "ts_code"
        try:
            frame = self.lake.read("tushare", table, columns=(requested_column,))
        except DatasetNotFoundError:
            return ()
        code_column = (
            requested_column if requested_column in frame.columns else "asset_id"
        )
        if code_column not in frame.columns:
            return ()
        return tuple(
            sorted(str(value) for value in frame[code_column].drop_nulls().unique())
        )

    def _execute_logged_tushare_request(
        self,
        job: TushareUpdateJob,
        *,
        mode: WriteMode | None = None,
    ) -> tuple[tuple[SnapshotRef, ...], dict[str, Any]]:
        source = self._source("tushare")
        request = DataRequest(
            dataset=job.table,
            filters=job.filters,
            start_date=job.start_date,
            end_date=job.end_date,
            options={"api_name": job.api_name} if job.api_name else {},
        )
        started = time.perf_counter()
        called_at = datetime.now(UTC)
        refs: tuple[SnapshotRef, ...] = ()
        try:
            data = source.read(request)
            duration_ms = int((time.perf_counter() - started) * 1000)
            status: TushareCallStatus = "empty" if data.is_empty() else "success"
            snapshot_id = ""
            if not data.is_empty():
                ref = self.lake.write(
                    "tushare",
                    job.table,
                    data,
                    mode=mode or job.mode,
                    metadata={"request": _request_payload(request)},
                    partition_column=job.partition_column,
                    partition_granularity=job.partition_granularity
                    if job.partition_column
                    else None,
                )
                refs = (ref,)
                snapshot_id = ref.snapshot_id
            self._append_tushare_call_log(
                _call_log_row(
                    job,
                    request=request,
                    called_at=called_at,
                    data=data,
                    status=status,
                    duration_ms=duration_ms,
                    snapshot_id=snapshot_id,
                )
            )
            return refs, {
                "table": job.table,
                "item": job.item or job.item_value,
                "status": status,
                "rows_written": data.height,
                "snapshot_id": snapshot_id,
            }
        except Exception as exc:
            duration_ms = int((time.perf_counter() - started) * 1000)
            self._append_tushare_call_log(
                _call_log_row(
                    job,
                    request=request,
                    called_at=called_at,
                    data=None,
                    status="failed",
                    duration_ms=duration_ms,
                    error=str(exc),
                )
            )
            raise

    def _append_tushare_call_log(self, row: Mapping[str, Any]) -> None:
        self.lake.write(
            "tushare",
            TUSHARE_CALL_LOG_TABLE,
            pl.DataFrame([dict(row)], schema=CALL_LOG_SCHEMA),
            mode="append",
            metadata={"system": "tushare_api_call_log"},
        )


def _request_payload(request: DataRequest) -> dict[str, Any]:
    return {
        "dataset": request.dataset,
        "fields": list(request.fields),
        "filters": dict(request.filters),
        "start_date": request.start_date,
        "end_date": request.end_date,
        "version": request.version,
        "snapshot": request.snapshot,
        "options": dict(request.options),
    }


def _infer_tushare_kind(table: str) -> TushareTableKind:
    if table in PRICE_TABLES:
        return "price"
    if table in FUNDAMENTAL_TABLES:
        return "fundamental"
    if table.endswith("_vip"):
        return "fundamental_vip"
    return "general"


def _spec_sort_key(spec: TushareTableUpdateSpec) -> tuple[int, str]:
    kind = spec.kind or _infer_tushare_kind(spec.table)
    priority = {
        "price": 0,
        "fundamental": 1,
        "fundamental_vip": 2,
        "general": 3,
    }.get(kind, 9)
    return priority, spec.table


def _latest_logged_date(
    log: pl.DataFrame,
    *,
    table: str,
    item_key: str,
    item_value: str | None = None,
) -> date | None:
    if log.is_empty():
        return None
    filtered = log.filter(
        (pl.col("table") == table)
        & (pl.col("item_key") == item_key)
        & (pl.col("status").is_in(["success", "empty"]))
    )
    if item_value is not None:
        filtered = filtered.filter(pl.col("item_value") == item_value)
    if filtered.is_empty():
        return None
    if item_key == "trade_date":
        values = [
            parse_tushare_date(value) for value in filtered["item_value"].to_list()
        ]
    else:
        values = [
            parse_date(value)
            for value in filtered["data_max_time"].drop_nulls().to_list()
        ]
        if not values:
            values = [
                parse_date(value)
                for value in filtered["request_end_date"].drop_nulls().to_list()
            ]
    dates = [value for value in values if isinstance(value, date)]
    return max(dates) if dates else None


def _latest_local_table_date(lake: LocalDataLake, table: str) -> date | None:
    try:
        frame = lake.read("tushare", table, columns=("time",))
    except DatasetNotFoundError:
        return None
    if "time" not in frame.columns or frame.is_empty():
        return None
    latest = frame["time"].drop_nulls().max()
    return latest if isinstance(latest, date) else None


def _date_range(start_date: date, end_date: date) -> tuple[date, ...]:
    days = (end_date - start_date).days
    if days < 0:
        return ()
    return tuple(start_date + timedelta(days=offset) for offset in range(days + 1))


def _tushare_date(value: Any) -> str:
    return tushare_date(value)


def _call_log_row(
    job: TushareUpdateJob,
    *,
    request: DataRequest,
    called_at: datetime,
    data: pl.DataFrame | None,
    status: TushareCallStatus,
    duration_ms: int,
    snapshot_id: str = "",
    error: str = "",
) -> dict[str, Any]:
    min_time: date | None = None
    max_time: date | None = None
    rows = 0
    if data is not None:
        data = _normalize_call_log_data(data)
        rows = data.height
        if "time" in data.columns and data.height:
            times = data["time"].drop_nulls()
            if not times.is_empty():
                time_min = times.min()
                time_max = times.max()
                min_time = time_min if isinstance(time_min, date) else None
                max_time = time_max if isinstance(time_max, date) else None
    params = dict(request.filters)
    if request.start_date is not None:
        params["start_date"] = _tushare_date(request.start_date)
    if request.end_date is not None:
        params["end_date"] = _tushare_date(request.end_date)
    request_hash = hashlib.sha256(
        json.dumps(
            {
                "table": job.table,
                "filters": dict(request.filters),
                "start_date": str(request.start_date),
                "end_date": str(request.end_date),
                "fields": list(request.fields),
            },
            sort_keys=True,
            default=str,
        ).encode("utf-8")
    ).hexdigest()
    return {
        "called_at": called_at.isoformat(),
        "api_name": job.api_name or job.table,
        "table": job.table,
        "kind": job.kind,
        "item_key": job.item_key,
        "item_value": job.item_value,
        "request_start_date": parse_date(request.start_date),
        "request_end_date": parse_date(request.end_date),
        "data_min_time": min_time,
        "data_max_time": max_time,
        "rows": rows,
        "status": status,
        "error": error,
        "duration_ms": duration_ms,
        "request_hash": request_hash,
        "snapshot_id": snapshot_id,
        "params_json": json.dumps(params, sort_keys=True, default=str),
        "fields_json": json.dumps(list(request.fields), sort_keys=True),
    }


def _normalize_call_log_data(data: pl.DataFrame) -> pl.DataFrame:
    return normalize_table_columns(data)
