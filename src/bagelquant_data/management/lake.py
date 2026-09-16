"""Public DataLake facade."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime
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
    compact_daily_range_backfill,
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
        MetadataStore.check_compatibility(self.paths.database)
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

    def ingest(
        self,
        spec: DatasetSpec,
        frame: pl.DataFrame,
        *,
        mode: str = "incremental",
        ingested_at=None,
    ) -> IngestionReport:
        """Register and ingest a local frame."""

        self.admin.datasets.register(spec)
        return self._pipeline.ingest_frame(
            spec, frame, mode=mode, ingested_at=ingested_at
        )


@dataclass
class LakeAdmin:
    """Public data-lake management API."""

    sources: SourceManager
    datasets: DatasetManager
    status: StatusManager

    def recovery_status(self, dataset: str, *, source: str, deep: bool = False) -> list[dict]:
        from bagelquant_data.storage.recovery import inspect_partition
        store = ParquetStore(self.status.paths, self.status.metadata)
        return [inspect_partition(store, source, dataset, row["partition_path"], deep=deep) for row in self.status.metadata.manifest(source, dataset)]

    def repair_partitions(
        self,
        dataset: str,
        *,
        source: str,
        partitions: Sequence[str],
        cancel_check: Callable[[], bool] | None = None,
    ) -> list[dict]:
        from uuid import uuid4
        from bagelquant_data.storage.recovery import repair_partition
        run_id = uuid4().hex
        metadata = self.status.metadata
        metadata.acquire_update_leases([(source, dataset, run_id)], owner_id=run_id)
        try:
            store = ParquetStore(self.status.paths, metadata)
            repaired = []
            for partition in dict.fromkeys(partitions):
                if cancel_check is not None and cancel_check():
                    break
                repaired.append(repair_partition(store, source, dataset, partition))
            return repaired
        finally:
            metadata.release_update_leases([run_id])

    def summary(self) -> dict[str, Any]:
        return self.status.summary()

    def rebuild_manifest(self, dataset: str, *, source: str) -> dict[str, Any]:
        return self.status.rebuild_manifest(dataset, source=source)

    def validate_manifest(
        self, dataset: str, *, source: str, deep: bool = False
    ) -> dict[str, Any]:
        return self.status.validate_manifest(dataset, source=source, deep=deep)

    def validate_dataset(
        self, dataset: str, *, source: str, deep: bool = True
    ) -> dict[str, Any]:
        """Validate files, schema, keys, and partition contracts."""

        spec = self.datasets.get(dataset, source=source)
        return self.status.validate_dataset(spec, deep=deep)

    def validate_datasets(
        self,
        datasets: Sequence[str] | None = None,
        *,
        source: str,
        deep: bool = False,
    ) -> dict[str, Any]:
        """Validate several registered datasets with shared metadata and inventory."""

        names = (
            [str(row["name"]) for row in self.datasets.list(source)]
            if datasets is None
            else list(dict.fromkeys(datasets))
        )
        specs = [self.datasets.get(dataset, source=source) for dataset in names]
        return self.status.validate_datasets(specs, deep=deep)

    def quarantine_partitions(
        self,
        dataset: str,
        *,
        source: str,
        partition_paths: Sequence[str],
        reason: str,
        confirm: bool = False,
        repair_id: str | None = None,
    ) -> dict[str, Any]:
        """Quarantine suspect canonical partitions without deleting them."""

        spec = self.datasets.get(dataset, source=source)
        return self.status.quarantine_partitions(
            spec,
            partition_paths,
            reason=reason,
            confirm=confirm,
            repair_id=repair_id,
        )

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

    def coverage(self, dataset: str, *, source: str, start: DateLike, end: DateLike) -> dict:
        from bagelquant_data.pipeline.scopes import inspect_coverage
        store = ParquetStore(self.status.paths, self.status.metadata)
        return inspect_coverage(self.datasets.get(dataset, source=source),
                                RawQueryService(store, self.status.metadata), self.status.metadata,
                                start=start, end=end)

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
        progress_callback: Callable[[UpdateProgress], None] | None = None,
        **kwargs: Any,
    ) -> UpdateReport:
        self.lake.metadata.recover_stale_running_scopes()
        raw = RawQueryService(self.lake.parquet, self.lake.metadata)
        adapter = self.lake.admin.sources.get(source)
        works: list[DatasetUpdateWork] = []
        for dataset in dict.fromkeys(datasets):
            planning_started = time.perf_counter()
            if progress_callback is not None:
                progress_callback(
                    UpdateProgress(dataset, "planning", 0, 0, 0, 0, 0, "running")
                )
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
            from bagelquant_data.pipeline.initialization import prepare_initialization

            prepare_initialization(
                self.lake.metadata,
                spec,
                context.options["mode"],
                context.start,
                context.end,
            )
            if progress_callback is not None:
                progress_callback(
                    UpdateProgress(dataset, "discovery", 0, 0, 0, 0, 0, "running")
                )
            discovered_param_sets, discovery_call = discover_request_param_sets(
                spec, adapter
            )
            raw_source_options = {
                **spec.request_options,
                **dict(context.options.get("source_options") or {}),
            }
            raw_source_options["refresh"] = context.options.get("mode") == "refresh"
            if raw_source_options is not None and not isinstance(
                raw_source_options, Mapping
            ):
                raise ConfigurationError("source_options must be a mapping")
            requests = synchronize_requests(
                spec=spec,
                raw=raw,
                metadata=self.lake.metadata,
                start=context.start if spec.update_type != "general" else None,
                end=context.end,
                today=context.options.get("today"),
                ids=context.options.get("ids"),
                params=context.options.get("params"),
                discovered_param_sets=discovered_param_sets,
                source_options=raw_source_options,
            )
            requests = compact_daily_range_backfill(
                spec,
                requests,
                raw_source_options,
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

        if not works:
            return combine_reports(source, [])
        selected_datasets = tuple(work.spec.name for work in works)
        before = _manifest_map(
            self.lake.metadata,
            source,
            selected_datasets,
        )
        leases = [(work.spec.source, work.spec.name, work.run_id) for work in works]
        owner_id = next(
            (
                str(work.context.options["owner_id"])
                for work in works
                if work.context.options.get("owner_id") is not None
            ),
            None,
        )
        self.lake.metadata.acquire_update_leases(leases, owner_id=owner_id)
        try:
            report = update_datasets(
                source_adapter=adapter,
                pipeline=self.lake._pipeline,
                works=tuple(works),
            )
        finally:
            self.lake.metadata.release_update_leases(work.run_id for work in works)
        from bagelquant_data.pipeline.initialization import finish_initialization

        for run in report.runs:
            work = next(w for w in works if w.spec.name == run.dataset)
            if (
                work.context.options["mode"] == "initialize"
                and run.status in {"success", "no_data"}
                and run.remaining_scope_count == 0
            ):
                finish_initialization(self.lake.metadata, source, run.dataset)
        after = _manifest_map(
            self.lake.metadata,
            source,
            selected_datasets,
        )
        changes = _partition_changes(before, after)
        run_ids = {run.run_id for run in report.runs}
        bounds = {}
        for row in self.lake.metadata._rows(
            "select c.dataset,c.run_id,b.partition_path,"
            "b.min_available,b.max_available,b.min_observation,b.max_observation,c.pit_date "
            "from version_batches b join version_commits c on c.seq=b.commit_seq "
            "where c.source=? and c.status='committed'", (source,),
        ):
            if row["run_id"] in run_ids:
                bounds.setdefault((row["dataset"], row["partition_path"]), []).append(row)
        return replace(report, changed_partitions=tuple(
            replace(change,
                    min_time=_version_batch_change_bounds(bounds[key])[0],
                    max_time=_version_batch_change_bounds(bounds[key])[1])
            if (key := (change.dataset, change.partition_path)) in bounds else change
            for change in changes
        ))


def _as_date(value: DateLike) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _request_context(
    source: str, dataset: str, kwargs: dict[str, Any]
) -> RequestContext:
    known = {
        "start": kwargs.pop("start", None),
        "end": kwargs.pop("end", None),
        "assets": None,
    }
    mode = kwargs.pop("mode", "incremental")
    ingested_at = kwargs.pop("ingested_at", None)
    if mode not in {"initialize", "incremental", "refresh"}:
        raise ConfigurationError("mode must be initialize, incremental, or refresh")
    workers = kwargs.pop("workers", None)
    batch_size = kwargs.pop("batch_size", None)
    max_in_flight = kwargs.pop("max_in_flight", None)
    max_buffer_mb = kwargs.pop("max_buffer_mb", None)
    source_options = kwargs.pop("source_options", None)
    progress_callback = kwargs.pop("progress_callback", None)
    max_retries = kwargs.pop("max_retries", None)
    retry_backoff_seconds = kwargs.pop("retry_backoff_seconds", None)
    today = kwargs.pop("today", None)
    ids = kwargs.pop("ids", None)
    params = kwargs.pop("params", None)
    owner_id = kwargs.pop("owner_id", None)
    cancel_requested = kwargs.pop("cancel_requested", None)
    baseline_repair = kwargs.pop("baseline_repair", None)
    if kwargs:
        keys = ", ".join(sorted(kwargs))
        raise ConfigurationError(f"Unsupported update option(s): {keys}")
    options: dict[str, Any] = {"mode": mode}
    if ingested_at is not None:
        options["ingested_at"] = ingested_at
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
    if baseline_repair is not None:
        options["baseline_repair"] = bool(baseline_repair)
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


def _version_batch_change_bounds(
    rows: Sequence[Mapping[str, Any]],
) -> tuple[str, str]:
    """Return the observation range affected by committed version batches.

    Raw versions are physically partitioned by availability date.  A repair
    committed today may nevertheless replace observations from years ago, so
    downstream invalidation must prefer observation bounds over availability
    bounds.
    """
    return (
        min(
            str(
                row.get("min_observation")
                or row.get("min_available")
                or row["pit_date"]
            )
            for row in rows
        ),
        max(
            str(
                row.get("max_observation")
                or row.get("max_available")
                or row["pit_date"]
            )
            for row in rows
        ),
    )


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
