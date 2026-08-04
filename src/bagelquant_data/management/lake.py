"""Public DataLake facade."""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
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
from bagelquant_data.pipeline.scopes import (
    LedgerRequest,
    discover_request_param_sets,
    synchronize_requests,
)
from bagelquant_data.pipeline.update import (
    DatasetUpdateWork,
    PartitionChange,
    UpdateProgress,
    UpdateReport,
    combine_reports,
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
    def open(cls, root: str | Path = "data") -> DataLake:
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

    def validate_manifest(
        self, dataset: str, *, source: str, deep: bool = False
    ) -> dict[str, Any]:
        return self.status.validate_manifest(dataset, source=source, deep=deep)

    def runs(self, limit: int = 20) -> list[dict[str, Any]]:
        return self.status.runs(limit)

    def failures(
        self, dataset: str | None = None, source: str | None = None
    ) -> list[dict[str, Any]]:
        return self.status.failures(dataset=dataset, source=source)

    def update_scopes(
        self,
        dataset: str | None = None,
        source: str | None = None,
        status: str | Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        return self.status.update_scopes(dataset=dataset, source=source, status=status)

    def update_summary(
        self, dataset: str | None = None, source: str | None = None
    ) -> list[dict[str, Any]]:
        return self.status.update_summary(dataset=dataset, source=source)

    def provider_scope_checks(
        self, dataset: str | None = None, source: str | None = None
    ) -> list[dict[str, Any]]:
        return self.status.provider_scope_checks(dataset=dataset, source=source)

    def reset_update_scopes(
        self, scope_ids: Sequence[int], *, clear_watermark: bool = False
    ) -> int:
        return self.status.reset_update_scopes(
            scope_ids, clear_watermark=clear_watermark
        )

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
        report = self.datasets(
            [dataset],
            source=source,
            start=start,
            end=end,
            confirm=False,
            progress_callback=progress_callback,
            **kwargs,
        )
        return report.runs[0]

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
        self.lake.metadata.recover_stale_running_scopes()
        raw = RawQueryService(self.lake.parquet, self.lake.metadata)
        adapter = self.lake.admin.sources.get(source)
        works: list[DatasetUpdateWork] = []
        for dataset in dict.fromkeys(datasets):
            planning_started = time.perf_counter()
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
            discovered_param_sets, discovery_call = discover_request_param_sets(
                spec, adapter
            )
            requests = synchronize_requests(
                spec=spec,
                raw=raw,
                metadata=self.lake.metadata,
                start=context.start if spec.update_type != "general" else None,
                end=context.end if spec.update_type != "general" else None,
                today=context.options.get("today"),
                ids=context.options.get("ids"),
                params=context.options.get("params"),
                discovered_param_sets=discovered_param_sets,
            )
            works.append(
                DatasetUpdateWork(
                    spec=spec,
                    context=context,
                    requests=requests,
                    discovery_calls=(
                        () if discovery_call is None else (discovery_call,)
                    ),
                    planning_seconds=time.perf_counter() - planning_started,
                )
            )

        _print_job_summary(works)
        selected_type = _confirm_update_jobs() if confirm else "all"
        if selected_type == "quit":
            return combine_reports(source, [])
        selected = [
            work
            for work in works
            if (
                selected_type == "all"
                or (
                    selected_type == "incremental"
                    and work.spec.update_type in {"by_daily", "by_asset"}
                )
            )
            or work.spec.update_type == selected_type
        ]
        if not selected:
            return combine_reports(source, [])
        selected_datasets = tuple(work.spec.name for work in selected)
        before = _manifest_map(
            self.lake.metadata,
            source,
            selected_datasets,
        )
        leases = [(work.spec.source, work.spec.name, work.run_id) for work in selected]
        owner_id = next(
            (
                str(work.context.options["owner_id"])
                for work in selected
                if work.context.options.get("owner_id") is not None
            ),
            None,
        )
        self.lake.metadata.acquire_update_leases(leases, owner_id=owner_id)
        try:
            report = update_datasets(
                source_adapter=adapter,
                pipeline=self.lake._pipeline,
                works=tuple(selected),
            )
        finally:
            self.lake.metadata.release_update_leases(work.run_id for work in selected)
        after = _manifest_map(
            self.lake.metadata,
            source,
            selected_datasets,
        )
        return replace(report, changed_partitions=_partition_changes(before, after))

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
    works: Sequence[DatasetUpdateWork],
) -> None:
    print("Eligible update scopes:")
    for work in works:
        details = _request_range(work.spec, work.requests)
        suffix = f", {details}" if details else ""
        print(
            f"- {work.spec.name} ({work.spec.update_type}): "
            f"{len(work.requests)} scope(s){suffix}"
        )


def _request_range(spec: DatasetSpec, requests: tuple[LedgerRequest, ...]) -> str:
    if not requests:
        return "no work"
    if spec.update_type == "by_daily":
        key = spec.date_param or "date"
        values = [
            str(request.params[key]) for request in requests if key in request.params
        ]
        return f"dates {min(values)} to {max(values)}" if values else ""
    if spec.update_type == "by_asset":
        assets = {
            str(request.params["id"]) for request in requests if "id" in request.params
        }
        starts = [
            str(request.params["start"])
            for request in requests
            if "start" in request.params
        ]
        ends = [
            str(request.params["end"])
            for request in requests
            if "end" in request.params
        ]
        date_range = f", dates {min(starts)} to {max(ends)}" if starts and ends else ""
        return f"{len(assets)} asset(s){date_range}"
    return "full refresh"


def _confirm_update_jobs() -> str:
    choices = {
        "1": "all",
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
    owner_id = kwargs.pop("owner_id", None)
    cancel_requested = kwargs.pop("cancel_requested", None)
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
    if owner_id is not None:
        options["owner_id"] = str(owner_id)
    if cancel_requested is not None:
        if not callable(cancel_requested):
            raise ConfigurationError("cancel_requested must be callable")
        options["cancel_requested"] = cancel_requested
    return RequestContext(source=source, dataset=dataset, options=options, **known)


def _manifest_map(
    metadata: MetadataStore,
    source: str,
    datasets: Sequence[str],
) -> dict[tuple[str, str], dict[str, Any]]:
    return {
        (str(row["dataset"]), str(row["partition_path"])): row
        for dataset in dict.fromkeys(datasets)
        for row in metadata.manifest(source, dataset)
    }


def _partition_changes(
    before: dict[tuple[str, str], dict[str, Any]],
    after: dict[tuple[str, str], dict[str, Any]],
) -> tuple[PartitionChange, ...]:
    changes = []
    for key in sorted(set(before) | set(after)):
        old = before.get(key)
        new = after.get(key)
        old_hash = None if old is None else str(old["content_hash"])
        new_hash = None if new is None else str(new["content_hash"])
        if old_hash == new_hash:
            continue
        time_starts = [
            str(row["min_time"])
            for row in (old, new)
            if row is not None and row.get("min_time") is not None
        ]
        time_ends = [
            str(row["max_time"])
            for row in (old, new)
            if row is not None and row.get("max_time") is not None
        ]
        changes.append(
            PartitionChange(
                dataset=key[0],
                partition_path=key[1],
                before_hash=old_hash,
                after_hash=new_hash,
                min_time=min(time_starts, default=None),
                max_time=max(time_ends, default=None),
            )
        )
    return tuple(changes)
