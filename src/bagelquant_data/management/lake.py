"""Public DataLake facade."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.exceptions import ConfigurationError
from bagelquant_data.core.registry import FrameworkRegistries, default_registries
from bagelquant_data.core.request import RequestContext
from bagelquant_data.management.datasets import DatasetManager
from bagelquant_data.management.sources import SourceManager
from bagelquant_data.management.status import StatusManager
from bagelquant_data.pipeline.ingest import IngestionPipeline, IngestionReport
from bagelquant_data.pipeline.planner import plan_update
from bagelquant_data.pipeline.update import UpdateReport, combine_reports, update_dataset
from bagelquant_data.query import LakeQuery
from bagelquant_data.query.raw import RawQueryService
from bagelquant_data.storage.metadata import MetadataStore
from bagelquant_data.storage.parquet import ParquetStore
from bagelquant_data.storage.paths import LakePaths
from bagelquant_data.storage.rejected import RejectedStore
from bagelquant_data.storage.staging import StagingStore


class DataLake:
    """Source-agnostic local data lake facade."""

    def __init__(self, root: str | Path, registries: FrameworkRegistries | None = None) -> None:
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

    def failures(self, dataset: str | None = None, source: str | None = None) -> list[dict[str, Any]]:
        return self.status.failures(dataset=dataset, source=source)

    def rejected(self, dataset: str, *, source: str) -> list[dict[str, Any]]:
        return self.status.rejected(dataset, source=source)


@dataclass
class LakeUpdater:
    """Public dataset update API."""

    lake: DataLake

    def dataset(self, dataset: str, *, source: str, **kwargs: Any) -> IngestionReport:
        spec = self.lake.admin.datasets.get(dataset, source=source)
        adapter = self.lake.admin.sources.get(source)
        context = _request_context(source=source, dataset=dataset, kwargs=kwargs)
        planned = plan_update(
            spec=spec,
            raw=RawQueryService(self.lake.parquet, self.lake.metadata),
            start=context.start,
            end=context.end,
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

    def datasets(self, datasets: list[str], *, source: str, **kwargs: Any) -> UpdateReport:
        reports = [self.dataset(dataset, source=source, **kwargs) for dataset in datasets]
        return combine_reports(source, reports)

    def source(self, source: str, **kwargs: Any) -> UpdateReport:
        names = [row["name"] for row in self.lake.admin.datasets.list(source) if row["enabled"]]
        return self.datasets(names, source=source, **kwargs)


def _request_context(source: str, dataset: str, kwargs: dict[str, Any]) -> RequestContext:
    known = {
        "start": kwargs.pop("start", None),
        "end": kwargs.pop("end", None),
        "assets": None,
    }
    workers = kwargs.pop("workers", None)
    batch_size = kwargs.pop("batch_size", None)
    source_options = kwargs.pop("source_options", None)
    progress = kwargs.pop("progress", None)
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
    if source_options is not None:
        options["source_options"] = source_options
    if progress is not None:
        options["progress"] = progress
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
