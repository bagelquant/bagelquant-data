"""Public standalone Data lake and concern-specific APIs."""

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
from bagelquant_data.execution import ExecutionOptions
from bagelquant_data.management.catalog import LakeCatalog
from bagelquant_data.management.datasets import DatasetManager, _spec_from_mapping
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
from bagelquant_data.query import _RawReader
from bagelquant_data.query.raw import RawQueryService
from bagelquant_data.storage.data_meta import DataMetaStore
from bagelquant_data.storage.parquet import ParquetStore
from bagelquant_data.storage.paths import LakePaths
from bagelquant_data.storage.rejected import RejectedStore


class DataLake:
    """Raw and DataItem APIs backed by one explicitly configured metadata store."""

    def __init__(
        self,
        *,
        data_meta_path: str | Path,
        lake_path: str | Path,
        read_only: bool = False,
    ) -> None:
        self._paths = LakePaths.open(
            data_meta_path=data_meta_path, lake_path=lake_path, read_only=read_only
        )
        DataMetaStore.check_compatibility(self._paths.data_meta_path)
        self._paths.ensure()
        self._data_meta = DataMetaStore(data_meta_path=self._paths.data_meta_path, read_only=read_only)
        self._data_meta.bind_lake(self._paths.lake)
        self._registries: FrameworkRegistries = default_registries()
        self._parquet = ParquetStore(self._paths, self._data_meta)
        self._datasets = DatasetManager(self._data_meta, self._paths)
        self.catalog = LakeCatalog(
            self._data_meta, SourceManager(self._registries, self._data_meta)
        )
        self._reader = _RawReader(
            RawQueryService(self._parquet, self._data_meta), self._datasets
        )
        self._pipeline = IngestionPipeline(
            registries=self._registries,
            parquet=self._parquet,
            metadata=self._data_meta,
            rejected=RejectedStore(self._paths),
        )
        self.raw = RawAPI(self)
        self.integrity = IntegrityAPI(self)
        from bagelquant_data.inputs import InputsAPI
        from bagelquant_data.items import ItemAPI

        self.inputs = InputsAPI(self)
        self.items = ItemAPI(self)

    @classmethod
    def open(
        cls,
        *,
        data_meta_path: str | Path,
        lake_path: str | Path,
        read_only: bool = False,
    ) -> DataLake:
        return cls(
            data_meta_path=data_meta_path, lake_path=lake_path, read_only=read_only
        )

    def close(self) -> None:
        """Connections are operation-scoped; discard runtime extension instances."""
        self._registries = default_registries()

    @classmethod
    def restore(
        cls,
        *,
        backup_data_meta_path: str | Path,
        backup_lake_path: str | Path,
        data_meta_path: str | Path,
        lake_path: str | Path,
    ) -> DataLake:
        """Restore a verified same-schema backup to two new caller-owned paths."""
        source = cls.open(
            data_meta_path=backup_data_meta_path,
            lake_path=backup_lake_path,
            read_only=True,
        )
        source.integrity.verify_backup()
        source.integrity.backup(data_meta_path=data_meta_path, lake_path=lake_path)
        restored = cls.open(data_meta_path=data_meta_path, lake_path=lake_path)
        # The newly created destination has no live publisher. Copied source
        # owner identities cannot hold permission to publish into this lake.
        for lease in restored.integrity.active_update_leases():
            restored.integrity.abandon_update_owner(
                str(lease["owner_id"]),
                reason="Restored backup has no running source publisher",
            )
        return restored

    @property
    def data_meta_path(self) -> Path:
        return self._paths.data_meta_path

    @property
    def lake_path(self) -> Path:
        return self._paths.lake

    def __enter__(self) -> DataLake:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


class RawAPI:
    """Dataset declarations, explicit updates and neutral Raw reads."""

    def __init__(self, lake: DataLake) -> None:
        self._lake = lake
        self._updater = _RawUpdater(lake)

    def register(
        self, spec: DatasetSpec, *, category_id: str | None = None
    ) -> DatasetSpec:
        from uuid import uuid4

        with self._lake._data_meta.dataset_writer(spec.source, spec.name, uuid4().hex):
            return self._register(spec, category_id=category_id)

    def _register(
        self, spec: DatasetSpec, *, category_id: str | None = None
    ) -> DatasetSpec:
        self._lake._data_meta.ensure_writable()
        if spec.source == "items":
            raise ConfigurationError("The items namespace is managed by lake.items")
        if category_id is not None:
            self._lake.catalog.raw_categories(spec.source).get(category_id)
        result = self._lake._datasets.register(spec)
        if category_id is not None:
            self._lake.catalog.raw_categories(spec.source).assign(
                spec.name, category_id
            )
        return result

    def register_toml(self, path: str | Path) -> DatasetSpec:
        return self.register_toml_text(Path(path).read_text(encoding="utf-8"))

    def register_toml_text(self, text: str) -> DatasetSpec:
        import tomllib

        return self.register(self.spec_from_mapping(tomllib.loads(text)))

    @staticmethod
    def spec_from_mapping(value: dict[str, Any]) -> DatasetSpec:
        spec = _spec_from_mapping(value)
        DatasetManager.validate_spec(spec)
        return spec

    @staticmethod
    def validate_spec(spec: DatasetSpec) -> None:
        DatasetManager.validate_spec(spec)

    def get(
        self, dataset: str, *, source: str, include_inactive: bool = False
    ) -> DatasetSpec:
        return self._lake._datasets.get(
            dataset, source=source, include_inactive=include_inactive
        )

    def list(
        self, source: str | None = None, *, include_inactive: bool = False
    ) -> list[dict[str, Any]]:
        return [
            row
            for row in self._lake._datasets.list(
                source, include_inactive=include_inactive
            )
            if row["source"] != "items"
        ]

    def enable(self, dataset: str, *, source: str) -> None:
        self._lake._data_meta.ensure_writable()
        self._lake._datasets.enable(dataset, source=source)

    def disable(self, dataset: str, *, source: str) -> None:
        self._lake._data_meta.ensure_writable()
        self._lake._datasets.disable(dataset, source=source)

    def remove(self, dataset: str, *, source: str) -> None:
        from uuid import uuid4
        store = self._lake._data_meta
        with store.dataset_writer(source, dataset, uuid4().hex), store.connect() as db:
            db.execute("begin immediate")
            dependencies = store._rows(
                "select d.item_name from item_dependencies d join item_definitions i on i.name=d.item_name where i.active=1 and d.kind='raw' and d.source=? and d.dataset=?",
                (source, dataset),
            )
            raw_dependents = [
                s.name
                for row in self.list(source)
                if (s := self.get(row["name"], source=source)).calendar == dataset
                or s.parameter_dataset == dataset
            ]
            if dependencies or raw_dependents:
                raise ConfigurationError(
                    f"Cannot unregister referenced dataset: {source}/{dataset}"
                )
            db.execute("update datasets set active=0,enabled=0 where source=? and name=?", (source,dataset))
            db.execute(
                "delete from catalog_assignments where kind='raw' and source=? and object_key=?",
                (source, dataset),
            )

    def ingest(
        self,
        spec: DatasetSpec,
        frame: pl.DataFrame,
        *,
        mode: str = "incremental",
        ingested_at=None,
    ) -> IngestionReport:
        self.register(spec)
        return self._lake._pipeline.ingest_frame(
            spec, frame, mode=mode, ingested_at=ingested_at
        )

    def initialize(
        self,
        dataset: str,
        *,
        source: str,
        end: DateLike,
        start: DateLike = "2000-01-01",
        config: ExecutionOptions | None = None,
        **options: Any,
    ) -> IngestionReport:
        return self.update(
            dataset,
            source=source,
            start=start,
            end=end,
            mode="initialize",
            config=config,
            **options,
        )

    def update(
        self,
        dataset: str,
        *,
        source: str,
        config: ExecutionOptions | None = None,
        **options: Any,
    ) -> IngestionReport:
        self._lake._data_meta.ensure_writable()
        if config is not None:
            self._merge_config(config, options)
        return self._updater.dataset(dataset, source=source, **options)

    def refresh(
        self,
        dataset: str,
        *,
        source: str,
        config: ExecutionOptions | None = None,
        **options: Any,
    ) -> IngestionReport:
        return self.update(
            dataset, source=source, mode="refresh", config=config, **options
        )

    def update_many(
        self,
        datasets: list[str],
        *,
        source: str,
        config: ExecutionOptions | None = None,
        **options: Any,
    ) -> UpdateReport:
        self._lake._data_meta.ensure_writable()
        if config is not None:
            self._merge_config(config, options)
        return self._updater.datasets(datasets, source=source, **options)

    @staticmethod
    def _merge_config(config: ExecutionOptions, options: dict[str, Any]) -> None:
        values = config.update_options()
        if conflict := values.keys() & options.keys():
            raise ConfigurationError(
                f"Execution configuration specified twice: {sorted(conflict)}"
            )
        options.update(values)

    def read(
        self,
        dataset: str,
        *,
        source: str,
        as_of=None,
        strict: bool = False,
        **options: Any,
    ) -> pl.LazyFrame:
        if as_of is not None:
            if options.get("as_of_date") is not None:
                raise ConfigurationError("Specify as_of once")
            options["as_of_date"] = as_of
        options["strict"] = strict
        if self.get(dataset, source=source).update_type == "general":
            return self._lake._reader.query_general(dataset, source=source, **options)
        options.setdefault(
            "view", "latest" if options.get("as_of_date") is not None else "history"
        )
        return self._lake._reader.query(dataset, source=source, **options)

    def read_versions(
        self, dataset: str, *, source: str, **options: Any
    ) -> pl.LazyFrame:
        return self.read(dataset, source=source, view="versions", **options)

    def observations(
        self, dataset: str, *, source: str, **options: Any
    ) -> pl.LazyFrame:
        return self._lake._reader.observations(dataset, source=source, **options)

    def snapshots(self, dataset: str, *, source: str) -> list[dict]:
        return self._lake._reader.snapshots(dataset, source=source)

    def version_evidence(
        self, dataset: str, *, source: str, **options: Any
    ) -> list[dict]:
        return self._lake._reader.version_evidence(dataset, source=source, **options)

    def manifest(self, dataset: str, *, source: str) -> list[dict[str, Any]]:
        return self._lake._data_meta.manifest(source, dataset)

    def definition_hash(self, dataset: str, *, source: str) -> str:
        return self._lake._data_meta.dataset_spec_hash(source, dataset)

    def schema(self, dataset: str, *, source: str) -> pl.Schema | None:
        return self._lake._parquet.canonical_schema(source, dataset)

    def status(self, dataset: str, *, source: str) -> dict[str, Any]:
        return self._lake.integrity._status.dataset(dataset, source=source)

    def status_many(
        self, datasets: Sequence[str] | None = None, *, source: str | None = None
    ) -> list[dict[str, Any]]:
        return self._lake.integrity._status.datasets(datasets, source=source)


class IntegrityAPI:
    """Read-only integrity evidence and explicitly invoked local repair."""

    def __init__(self, lake: DataLake) -> None:
        self._lake = lake
        self._status = StatusManager(lake._data_meta, lake._paths)

    def backup(
        self, *, data_meta_path: str | Path, lake_path: str | Path
    ) -> dict[str, Any]:
        """Export Data-owned evidence to explicitly supplied new paths."""
        from bagelquant_data.management.backup import export

        return export(
            self._lake._data_meta,
            self._lake._paths,
            data_meta_path=data_meta_path,
            lake_path=lake_path,
        )

    def verify_backup(self) -> dict[str, Any]:
        """Verify this opened Data backup and return its owned file inventory."""
        from bagelquant_data.management.backup import verify

        return verify(self._lake._data_meta, self._lake._paths)

    def scan(self, dataset: str, *, source: str, deep: bool = True) -> dict[str, Any]:
        return self._status.validate_dataset(
            self._lake._datasets.get(dataset, source=source), deep=deep
        )

    def scan_many(
        self, datasets: Sequence[str] | None = None, *, source: str, deep: bool = True
    ) -> dict[str, Any]:
        names = (
            [row["name"] for row in self._lake._datasets.list(source)]
            if datasets is None
            else list(dict.fromkeys(datasets))
        )
        return self._status.validate_datasets(
            [self._lake._datasets.get(name, source=source) for name in names], deep=deep
        )

    def validate_manifest(
        self, dataset: str, *, source: str, deep: bool = False
    ) -> dict[str, Any]:
        return self._status.validate_manifest(dataset, source=source, deep=deep)

    def recovery_status(
        self, dataset: str, *, source: str, deep: bool = False
    ) -> list[dict]:
        from bagelquant_data.storage.recovery import inspect_partition

        return [
            inspect_partition(
                self._lake._parquet, source, dataset, row["partition_path"], deep=deep
            )
            for row in self._lake._data_meta.manifest(source, dataset)
        ]

    def repair_partitions(
        self,
        dataset: str,
        *,
        source: str,
        partitions: Sequence[str],
        cancel_check: Callable[[], bool] | None = None,
    ) -> list[dict]:
        plan = self.repair_plan(dataset, source=source)
        selected = set(partitions)
        registered = {row["partition"] for row in plan["partitions"]}
        if selected - registered:
            raise ConfigurationError(
                f"Unregistered repair partitions: {sorted(selected - registered)}"
            )
        plan["partitions"] = [
            row for row in plan["partitions"] if row["partition"] in selected
        ]
        return self.repair(plan, cancel_check=cancel_check)

    def repair_plan(self, dataset: str, *, source: str) -> dict[str, Any]:
        from bagelquant_data.core.hashing import stable_record_hash

        statuses = self.recovery_status(dataset, source=source, deep=True)
        manifests = self._lake._data_meta.manifest(source, dataset)
        return {
            "source": source,
            "dataset": dataset,
            "partitions": statuses,
            "manifest_hash": stable_record_hash({"manifest": manifests}),
            "lake_id": self._lake._data_meta._rows(
                "select value from data_meta_state where key='lake_id'"
            )[0]["value"],
        }

    def repair(
        self, plan: Mapping[str, Any], *, cancel_check: Callable[[], bool] | None = None
    ) -> list[dict]:
        """Restore registered local evidence after validating a current repair plan."""
        from uuid import uuid4
        from bagelquant_data.core.hashing import stable_record_hash
        from bagelquant_data.storage.recovery import repair_partition

        store = self._lake._data_meta
        store.ensure_writable()
        if (
            plan.get("lake_id")
            != store._rows("select value from data_meta_state where key='lake_id'")[0][
                "value"
            ]
        ):
            raise ConfigurationError("Repair plan belongs to a different lake")
        source, dataset = str(plan["source"]), str(plan["dataset"])
        owner = uuid4().hex
        store.acquire_update_leases([(source, dataset, owner)], owner_id=owner)
        try:
            manifest = store.manifest(source, dataset)
            if plan.get("manifest_hash") != stable_record_hash({"manifest": manifest}):
                raise ConfigurationError(
                    "Repair plan is stale; scan the current committed manifest"
                )
            selected = list(
                dict.fromkeys(str(row["partition"]) for row in plan["partitions"])
            )
            registered = {row["partition_path"] for row in manifest}
            if set(selected) - registered:
                raise ConfigurationError(
                    "Repair requires registered partition evidence"
                )
            results = []
            for partition in selected:
                if cancel_check is not None and cancel_check():
                    break
                results.append(
                    repair_partition(self._lake._parquet, source, dataset, partition)
                )
            return results
        finally:
            store.release_update_leases([owner])

    def summary(self) -> dict[str, Any]:
        return self._status.summary()

    def runs(self, limit: int = 20) -> list[dict[str, Any]]:
        return self._status.runs(limit)

    def failures(
        self, dataset: str | None = None, source: str | None = None
    ) -> list[dict[str, Any]]:
        return self._status.failures(dataset=dataset, source=source)

    def update_scopes(
        self, dataset: str | None = None, source: str | None = None, status=None
    ) -> list[dict[str, Any]]:
        return self._status.update_scopes(dataset=dataset, source=source, status=status)

    def update_summary(
        self, dataset: str | None = None, source: str | None = None
    ) -> list[dict[str, Any]]:
        return self._status.update_summary(dataset=dataset, source=source)

    def provider_scope_checks(
        self, dataset: str | None = None, source: str | None = None
    ) -> list[dict[str, Any]]:
        return self._status.provider_scope_checks(dataset=dataset, source=source)

    def reset_update_scopes(
        self, scope_ids: Sequence[int], *, clear_watermark: bool = False
    ) -> int:
        self._lake._data_meta.ensure_writable()
        return self._status.reset_update_scopes(
            scope_ids, clear_watermark=clear_watermark
        )

    def coverage(
        self, dataset: str, *, source: str, start: DateLike, end: DateLike
    ) -> dict:
        from bagelquant_data.pipeline.scopes import inspect_coverage

        return inspect_coverage(
            self._lake._datasets.get(dataset, source=source),
            self._lake._reader._raw,
            self._lake._data_meta,
            start=start,
            end=end,
        )

    def rejected(self, dataset: str, *, source: str) -> list[dict[str, Any]]:
        return self._status.rejected(dataset, source=source)

    def active_update_leases(self) -> list[dict[str, Any]]:
        return self._lake._data_meta.active_update_leases()

    def abandon_update_owner(self, owner_id: str, *, reason: str) -> dict[str, int]:
        self._lake._data_meta.ensure_writable()
        return self._lake._data_meta.abandon_update_owner(owner_id, reason=reason)


@dataclass
class _RawUpdater:
    """Public dataset update API."""

    lake: DataLake

    def dataset(
        self,
        dataset: str,
        *,
        source: str,
        start: DateLike = "2000-01-01",
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
        start: DateLike = "2000-01-01",
        end: DateLike | None = None,
        progress_callback: Callable[[UpdateProgress], None] | None = None,
        **kwargs: Any,
    ) -> UpdateReport:
        raw = RawQueryService(self.lake._parquet, self.lake._data_meta)
        adapter = self.lake.catalog.sources.get(source)
        works: list[DatasetUpdateWork] = []
        for dataset in dict.fromkeys(datasets):
            planning_started = time.perf_counter()
            if progress_callback is not None:
                progress_callback(
                    UpdateProgress(dataset, "planning", 0, 0, 0, 0, 0, "running")
                )
            spec = self.lake.raw.get(dataset, source=source)
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
                self.lake._data_meta,
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
                metadata=self.lake._data_meta,
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
            self.lake._data_meta,
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
        self.lake._data_meta.acquire_update_leases(leases, owner_id=owner_id)
        try:
            report = update_datasets(
                source_adapter=adapter,
                pipeline=self.lake._pipeline,
                works=tuple(works),
            )
        finally:
            self.lake._data_meta.release_update_leases(work.run_id for work in works)
        from bagelquant_data.pipeline.initialization import finish_initialization

        for run in report.runs:
            work = next(w for w in works if w.spec.name == run.dataset)
            if (
                work.context.options["mode"] == "initialize"
                and run.status in {"success", "no_data"}
                and run.remaining_scope_count == 0
            ):
                finish_initialization(self.lake._data_meta, source, run.dataset)
        after = _manifest_map(
            self.lake._data_meta,
            source,
            selected_datasets,
        )
        changes = _partition_changes(before, after)
        run_ids = {run.run_id for run in report.runs}
        bounds = {}
        for row in self.lake._data_meta._rows(
            "select c.dataset,c.run_id,b.partition_path,"
            "b.min_available,b.max_available,b.min_observation,b.max_observation,c.pit_date "
            "from version_batches b join version_commits c on c.seq=b.commit_seq "
            "where c.source=? and c.status='committed'",
            (source,),
        ):
            if row["run_id"] in run_ids:
                bounds.setdefault((row["dataset"], row["partition_path"]), []).append(
                    row
                )
        return replace(
            report,
            changed_partitions=tuple(
                replace(
                    change,
                    min_time=_version_batch_change_bounds(bounds[key])[0],
                    max_time=_version_batch_change_bounds(bounds[key])[1],
                )
                if (key := (change.dataset, change.partition_path)) in bounds
                else change
                for change in changes
            ),
        )


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
    workers = kwargs.pop("workers", 1)
    batch_size = kwargs.pop("batch_size", None)
    max_in_flight = kwargs.pop("max_in_flight", None)
    max_buffer_mb = kwargs.pop("max_buffer_mb", None)
    max_buffer_bytes = kwargs.pop("max_buffer_bytes", None)
    source_options = kwargs.pop("source_options", None)
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
    for name, value in (
        ("workers", workers),
        ("batch_size", batch_size),
        ("max_in_flight", max_in_flight),
        ("max_buffer_bytes", max_buffer_bytes),
        ("max_buffer_mb", max_buffer_mb),
    ):
        if value is not None and (
            isinstance(value, bool) or not isinstance(value, int) or value < 1
        ):
            raise ConfigurationError(f"{name} must be a positive integer")
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
    if max_buffer_bytes is not None:
        options["max_buffer_bytes"] = max_buffer_bytes
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
    return RequestContext(source=source, dataset=dataset, options=options, **known)


def _manifest_map(
    metadata: DataMetaStore,
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
