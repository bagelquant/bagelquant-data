"""Polars-native data lake manager facade."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime
from typing import Any

import polars as pl

from bagelquant_data.datasource.base import DataRequest, DataSource
from bagelquant_data.datasource.registry import DataSourceRegistry, default_registry
from bagelquant_data.lake.local import LocalDataLake, WriteMode
from bagelquant_data.lake.snapshot import SnapshotRef
from bagelquant_data.lake.tushare_update import (
    TushareTableUpdateSpec,
    TushareUpdateJob,
    TushareUpdatePlan,
    TushareUpdateReport,
)


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
        return self.update(
            "tushare",
            DataRequest(dataset="stock_basic", options=options),
            mode="overwrite",
        )

    def update_tushare_trading_calendar(self, **options: Any) -> SnapshotRef:
        return self.update(
            "tushare",
            DataRequest(dataset="trade_cal", options=options),
            mode="overwrite",
        )

    def scan_tushare_updates(
        self,
        specs: tuple[TushareTableUpdateSpec, ...] | list[TushareTableUpdateSpec],
        *,
        start_date: Any,
        end_date: Any,
        **_: Any,
    ) -> TushareUpdateReport:
        jobs = tuple(
            TushareUpdateJob(
                table=spec.table,
                kind=spec.kind or "general",
                start_date=start_date,
                end_date=end_date,
                filters={},
            )
            for spec in specs
        )
        plans = tuple(
            TushareUpdatePlan(
                table=job.table,
                kind=job.kind,
                requested_start=_as_date(start_date),
                requested_end=_as_date(end_date),
                effective_start=_as_date(start_date),
                pending_items=(job.item,) if job.item else (),
                reason="requested",
                estimated_job_count=1,
                status="pending",
                universe=job.universe,
                trading_calendar=job.trading_calendar,
            )
            for job in jobs
        )
        return TushareUpdateReport(
            generated_at=datetime.now(UTC),
            source="tushare",
            requested_start=_as_date(start_date),
            requested_end=_as_date(end_date),
            plans=plans,
            jobs=jobs,
        )

    def execute_tushare_update_report(
        self,
        report: TushareUpdateReport,
        *,
        mode: WriteMode = "overwrite",
        **_: Any,
    ) -> tuple[SnapshotRef, ...]:
        return tuple(
            self.update(
                "tushare",
                DataRequest(
                    dataset=job.table,
                    filters=job.filters,
                    start_date=job.start_date,
                    end_date=job.end_date,
                ),
                mode=mode,
            )
            for job in report.jobs
        )

    def update_tushare_all(self, table: str = "daily", **options: Any) -> SnapshotRef:
        return self.update(
            "tushare",
            DataRequest(dataset=table, options=options),
            mode="overwrite",
        )

    def _source(self, source: str | DataSource) -> DataSource:
        if isinstance(source, str):
            return self.registry.resolve(source)
        return source


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


def _as_date(value: Any) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return datetime.fromisoformat(str(value)).date()
