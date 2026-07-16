"""Public DataLake facade."""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from collections.abc import Callable, Sequence
from typing import Any

import polars as pl

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.exceptions import ConfigurationError, StaleUpdatePlanError
from bagelquant_data.core.registry import FrameworkRegistries, default_registries
from bagelquant_data.core.request import RequestContext
from bagelquant_data.core.types import DateLike
from bagelquant_data.management.datasets import DatasetManager
from bagelquant_data.management.sources import SourceManager
from bagelquant_data.management.status import StatusManager
from bagelquant_data.pipeline.ingest import IngestionPipeline, IngestionReport
from bagelquant_data.pipeline.completeness import (
    AuditMode,
    UpdatePlan,
    build_update_plan,
    planning_state_fingerprint,
)
from bagelquant_data.pipeline.planner import plan_update
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

    def state_fingerprint(self, *, source: str) -> str:
        """Return the provider-free identity used to validate update plans."""

        return planning_state_fingerprint(self.lake.metadata, source)

    def plan(
        self,
        datasets: Sequence[str],
        *,
        source: str,
        start: DateLike = "1999-12-31",
        end: DateLike | None = None,
        audit: AuditMode = "fast",
        ids: Sequence[str] | None = None,
        params: dict[str, object] | None = None,
        today: DateLike | None = None,
    ) -> UpdatePlan:
        """Plan fast or full completeness work without provider calls."""

        names = list(dict.fromkeys(str(value) for value in datasets))
        specs = [
            self.lake.admin.datasets.get(name, source=source) for name in names
        ]
        return build_update_plan(
            specs=specs,
            raw=RawQueryService(self.lake.parquet, self.lake.metadata),
            metadata=self.lake.metadata,
            source=source,
            start=start,
            end=end,
            audit=audit,
            ids=ids,
            params=params,
            today=today,
        )

    def execute(
        self,
        plan: UpdatePlan,
        *,
        progress_callback: Callable[[UpdateProgress], None] | None = None,
        **kwargs: Any,
    ) -> UpdateReport:
        """Execute an unchanged, previously previewed update plan."""

        current = planning_state_fingerprint(self.lake.metadata, plan.source)
        if current != plan.state_fingerprint:
            raise StaleUpdatePlanError(
                "lake state changed after preview; create and confirm a new update plan"
            )
        if not plan.datasets:
            return combine_reports(plan.source, [])
        before = _manifest_map(self.lake.metadata, plan.source)
        works = []
        for planned in plan.datasets:
            spec = self.lake.admin.datasets.get(
                planned.dataset, source=plan.source
            )
            context = _request_context(
                source=plan.source,
                dataset=planned.dataset,
                kwargs={
                    **kwargs,
                    "start": plan.start,
                    "end": plan.end,
                    "progress_callback": progress_callback,
                },
            )
            works.append(
                DatasetUpdateWork(spec, context, planned.requests)
            )
        adapter = self.lake.admin.sources.get(plan.source)
        report = update_datasets(
            source_adapter=adapter,
            pipeline=self.lake._pipeline,
            works=tuple(works),
        )
        after = _manifest_map(self.lake.metadata, plan.source)
        report = replace(
            report,
            changed_partitions=_partition_changes(before, after),
        )
        if plan.audit == "full":
            for run in report.runs:
                if run.status == "success" and run.pending_job_count == 0:
                    self.lake.metadata.record_audit_watermark(
                        source=plan.source,
                        dataset=run.dataset,
                        start=plan.start,
                        end=plan.end,
                        state_fingerprint=plan.state_fingerprint,
                    )
        return report

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
        audit = kwargs.pop("audit", "fast")
        ids = kwargs.pop("ids", None)
        params = kwargs.pop("params", None)
        today = kwargs.pop("today", None)
        plan = self.plan(
            [dataset], source=source, start=start, end=end, audit=audit,
            ids=ids, params=params, today=today,
        )
        report = self.execute(
            plan, progress_callback=progress_callback, **kwargs
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


def _manifest_map(
    metadata: MetadataStore, source: str
) -> dict[tuple[str, str], dict[str, Any]]:
    return {
        (str(row["dataset"]), str(row["partition_path"])): row
        for row in metadata.manifest(source)
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
        row = new or old or {}
        changes.append(
            PartitionChange(
                dataset=key[0],
                partition_path=key[1],
                before_hash=old_hash,
                after_hash=new_hash,
                min_time=(
                    None if row.get("min_time") is None else str(row["min_time"])
                ),
                max_time=(
                    None if row.get("max_time") is None else str(row["max_time"])
                ),
            )
        )
    return tuple(changes)
