"""Public DataLake facade."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from collections.abc import Callable
from typing import Any

import polars as pl

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.exceptions import ConfigurationError
from bagelquant_data.core.registry import FrameworkRegistries, default_registries
from bagelquant_data.core.request import RequestContext
from bagelquant_data.core.types import DateLike
from bagelquant_data.management.datasets import DatasetManager
from bagelquant_data.management.sources import SourceManager
from bagelquant_data.management.status import StatusManager
from bagelquant_data.pipeline.ingest import IngestionPipeline, IngestionReport
from bagelquant_data.pipeline.planner import plan_update
from bagelquant_data.pipeline.update import (
    DatasetUpdateWork,
    UpdateProgress,
    UpdateReport,
    combine_reports,
    update_dataset,
    update_datasets,
)
from bagelquant_data.query import LakeQuery
from bagelquant_data.query.raw import RawQueryService
from bagelquant_data.storage.metadata import MetadataStore
from bagelquant_data.storage.parquet import ParquetStore
from bagelquant_data.storage.paths import LakePaths
from bagelquant_data.storage.rejected import RejectedStore
from bagelquant_data.storage.staging import StagingStore


class DataLake:
    """Source-agnostic local data lake facade."""

    def __init__(
        self, root: str | Path, registries: FrameworkRegistries | None = None
    ) -> None:
        self.paths = LakePaths.open(root)
        self.paths.ensure()
        self.registries = registries or default_registries()
        self.metadata = MetadataStore(self.paths.database)
        self.parquet = ParquetStore(self.paths, self.metadata)
        sources = SourceManager(self.registries, self.metadata)
        datasets = DatasetManager(self.metadata, self.paths)
        status = StatusManager(self.metadata, self.paths)
        raw = RawQueryService(self.parquet, self.metadata)
        self.query = LakeQuery(raw, datasets)
        self.admin = LakeAdmin(sources, datasets, status)
        self.update = LakeUpdater(self)
        self._pipeline = IngestionPipeline(
            registries=self.registries,
            parquet=self.parquet,
            metadata=self.metadata,
            staging=StagingStore(self.paths),
            rejected=RejectedStore(self.paths),
        )

    @classmethod
    def open(cls, root: str | Path = "data") -> "DataLake":
        """Open or create a local data lake."""

        return cls(root)

    def ingest(self, spec: DatasetSpec, frame: pl.DataFrame) -> IngestionReport:
        """Register and ingest a local frame."""

        self.admin.datasets.register(spec)
        return self._pipeline.ingest_frame(spec, frame, mode=spec.update_type)


@dataclass
class LakeAdmin:
    """Public data-lake management API."""

    sources: SourceManager
    datasets: DatasetManager
    status: StatusManager

    def summary(self) -> dict[str, Any]:
        return self.status.summary()

    def rebuild_manifest(self, dataset: str, *, source: str) -> dict[str, Any]:
        return self.status.rebuild_manifest(dataset, source=source)

    def validate_manifest(self, dataset: str, *, source: str) -> dict[str, Any]:
        return self.status.validate_manifest(dataset, source=source)

    def runs(self, limit: int = 20) -> list[dict[str, Any]]:
        return self.status.runs(limit)

    def failures(
        self, dataset: str | None = None, source: str | None = None
    ) -> list[dict[str, Any]]:
        return self.status.failures(dataset=dataset, source=source)

    def pending_update_jobs(
        self,
        dataset: str | None = None,
        source: str | None = None,
    ) -> list[dict[str, Any]]:
        return self.status.pending_update_jobs(dataset=dataset, source=source)

    def rejected(self, dataset: str, *, source: str) -> list[dict[str, Any]]:
        return self.status.rejected(dataset, source=source)


@dataclass
class LakeUpdater:
    """Public dataset update API."""

    lake: DataLake

    def dataset(
        self,
        dataset: str,
        *,
        source: str,
        start: DateLike = "1999-12-31",
        end: DateLike | None = None,
        progress_callback: Callable[[UpdateProgress], None] | None = None,
        **kwargs: Any,
    ) -> IngestionReport:
        spec = self.lake.admin.datasets.get(dataset, source=source)
        adapter = self.lake.admin.sources.get(source)
        context = _request_context(
            source=source,
            dataset=dataset,
            kwargs={
                **kwargs,
                "start": start,
                "end": end,
                "progress_callback": progress_callback,
            },
        )
        planned = plan_update(
            spec=spec,
            raw=RawQueryService(self.lake.parquet, self.lake.metadata),
            start=context.start if spec.update_type != "general" else None,
            end=context.end if spec.update_type != "general" else None,
            today=context.options.get("today"),
            ids=context.options.get("ids"),
            params=context.options.get("params"),
        )
        return update_dataset(
            spec=spec,
            source_adapter=adapter,
            pipeline=self.lake._pipeline,
            context=context,
            requests=planned.requests,
        )

    def datasets(
        self,
        datasets: list[str],
        *,
        source: str,
        start: DateLike = "1999-12-31",
        end: DateLike | None = None,
        confirm: bool = True,
        progress_callback: Callable[[UpdateProgress], None] | None = None,
        **kwargs: Any,
    ) -> UpdateReport:
        raw = RawQueryService(self.lake.parquet, self.lake.metadata)
        jobs: list[
            tuple[DatasetSpec, RequestContext, tuple[dict[str, object], ...]]
        ] = []
        for dataset in datasets:
            spec = self.lake.admin.datasets.get(dataset, source=source)
            context = _request_context(
                source=source,
                dataset=dataset,
                kwargs={
                    **kwargs,
                    "start": start,
                    "end": end,
                    "progress_callback": progress_callback,
                },
            )
            planned = plan_update(
                spec=spec,
                raw=raw,
                start=context.start if spec.update_type != "general" else None,
                end=context.end if spec.update_type != "general" else None,
                today=context.options.get("today"),
                ids=context.options.get("ids"),
                params=context.options.get("params"),
            )
            jobs.append((spec, context, planned.requests))

        _print_job_summary(jobs, self.lake.metadata)
        selected_type = _confirm_update_jobs() if confirm else "incremental"
        if selected_type == "quit":
            return combine_reports(source, [])
        selected = [
            job
            for job in jobs
            if (
                selected_type == "incremental"
                and job[0].update_type in {"by_daily", "by_asset"}
            )
            or job[0].update_type == selected_type
        ]
        adapter = self.lake.admin.sources.get(source) if selected else None
        if not selected or adapter is None:
            return combine_reports(source, [])
        return update_datasets(
            source_adapter=adapter,
            pipeline=self.lake._pipeline,
            works=tuple(
                DatasetUpdateWork(spec, context, requests)
                for spec, context, requests in selected
            ),
        )

    def source(
        self,
        source: str,
        *,
        start: DateLike = "1999-12-31",
        end: DateLike | None = None,
        confirm: bool = True,
        progress_callback: Callable[[UpdateProgress], None] | None = None,
        **kwargs: Any,
    ) -> UpdateReport:
        names = [
            row["name"]
            for row in self.lake.admin.datasets.list(source)
            if row["enabled"]
        ]
        return self.datasets(
            names,
            source=source,
            start=start,
            end=end,
            confirm=confirm,
            progress_callback=progress_callback,
            **kwargs,
        )


def _print_job_summary(
    jobs: list[tuple[DatasetSpec, RequestContext, tuple[dict[str, object], ...]]],
    metadata: MetadataStore,
) -> None:
    print("Planned update jobs:")
    for spec, _, requests in jobs:
        details = _request_range(spec, requests)
        pending = len(
            metadata.pending_update_jobs(source=spec.source, dataset=spec.name)
        )
        suffix = f", {details}" if details else ""
        retry = f", {pending} pending retry job(s)" if pending else ""
        print(
            f"- {spec.name} ({spec.update_type}): "
            f"{len(requests)} new request(s){retry}{suffix}"
        )


def _request_range(spec: DatasetSpec, requests: tuple[dict[str, object], ...]) -> str:
    if not requests:
        return "no work"
    if spec.update_type == "by_daily":
        key = spec.date_param or "date"
        values = [str(request[key]) for request in requests if key in request]
        return f"dates {min(values)} to {max(values)}" if values else ""
    if spec.update_type == "by_asset":
        assets = {str(request["id"]) for request in requests if "id" in request}
        starts = [str(request["start"]) for request in requests if "start" in request]
        ends = [str(request["end"]) for request in requests if "end" in request]
        date_range = f", dates {min(starts)} to {max(ends)}" if starts and ends else ""
        return f"{len(assets)} asset(s){date_range}"
    return "full refresh"


def _confirm_update_jobs() -> str:
    choices = {
        "1": "incremental",
        "2": "by_daily",
        "3": "by_asset",
        "4": "general",
        "5": "quit",
    }
    while True:
        print("1. all\n2. by daily only\n3. by asset only\n4. refresh general\n5. quit")
        choice = input("Select update jobs: ").strip()
        if choice in choices:
            return choices[choice]
        print("Invalid selection. Enter a number from 1 to 5.")


def _request_context(
    source: str, dataset: str, kwargs: dict[str, Any]
) -> RequestContext:
    known = {
        "start": kwargs.pop("start", None),
        "end": kwargs.pop("end", None),
        "assets": None,
    }
    workers = kwargs.pop("workers", None)
    batch_size = kwargs.pop("batch_size", None)
    max_in_flight = kwargs.pop("max_in_flight", None)
    max_buffer_mb = kwargs.pop("max_buffer_mb", None)
    source_options = kwargs.pop("source_options", None)
    progress = kwargs.pop("progress", None)
    progress_callback = kwargs.pop("progress_callback", None)
    max_retries = kwargs.pop("max_retries", None)
    retry_backoff_seconds = kwargs.pop("retry_backoff_seconds", None)
    today = kwargs.pop("today", None)
    ids = kwargs.pop("ids", None)
    params = kwargs.pop("params", None)
    if kwargs:
        keys = ", ".join(sorted(kwargs))
        raise ConfigurationError(f"Unsupported update option(s): {keys}")
    options: dict[str, Any] = {}
    if workers is not None:
        options["workers"] = workers
    if batch_size is not None:
        options["batch_size"] = batch_size
    if max_in_flight is not None:
        options["max_in_flight"] = max_in_flight
    if max_buffer_mb is not None:
        options["max_buffer_mb"] = max_buffer_mb
    if source_options is not None:
        options["source_options"] = source_options
    if progress is not None:
        options["progress"] = progress
    if progress_callback is not None:
        if not callable(progress_callback):
            raise ConfigurationError("progress_callback must be callable")
        options["progress_callback"] = progress_callback
    if max_retries is not None:
        options["max_retries"] = max_retries
    if retry_backoff_seconds is not None:
        options["retry_backoff_seconds"] = retry_backoff_seconds
    if today is not None:
        options["today"] = today
    if ids is not None:
        options["ids"] = ids
    if params is not None:
        options["params"] = params
    return RequestContext(source=source, dataset=dataset, options=options, **known)
