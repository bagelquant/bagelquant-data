"""Canonical commit pipeline."""

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import polars as pl

from bagelquant_data.core.dataset import (
    DatasetSpec,
    incremental_key,
)
from bagelquant_data.core.hashing import stable_bucket
from bagelquant_data.core.registry import FrameworkRegistries
from bagelquant_data.core.schema import (
    align_frame,
    compatible_schema,
    concat_compatible_frames,
    normalize_all_null_columns,
)
from bagelquant_data.core.validation import Validator
from bagelquant_data.storage.parquet import (
    ParquetStore,
    PartitionWriteContext,
    PartitionWriteResult,
    finalize_partition_writes,
    partition_write_context,
    rollback_partition_writes,
)

MAX_PARQUET_WRITE_WORKERS = 4


@dataclass(frozen=True, slots=True)
class CommitResult:
    """Physical and logical results of one canonical commit."""

    rows_committed: int
    partitions_rewritten: int
    partitions_skipped: int
    bytes_written: int
    present_times: frozenset[str] = frozenset()
    asset_max_times: tuple[tuple[str, str], ...] = ()


def commit_frame(
    *,
    spec: DatasetSpec,
    frame: pl.LazyFrame,
    registries: FrameworkRegistries,
    parquet: ParquetStore,
    writer_executor: ThreadPoolExecutor | None = None,
) -> CommitResult:
    """Validate, deduplicate, partition, and write canonical records."""

    validator = cast(Validator, registries.validators.get("framework"))
    validator.validate(frame, spec)
    data = _derive_partition_columns(
        normalize_all_null_columns(frame.collect()), spec
    )
    stored_schema = parquet.canonical_schema(spec.source, spec.name)
    canonical_schema = (
        pl.Schema(data.schema)
        if spec.update_type == "general"
        else compatible_schema(
            schema
            for schema in (
                stored_schema,
                data.schema,
            )
            if schema is not None
        )
    )
    data = align_frame(data, canonical_schema)
    data = _deduplicate(data, spec)
    manifests = {
        str(row["partition_path"]): row
        for row in parquet.metadata.manifest(spec.source, spec.name)
    }
    write_context = partition_write_context(canonical_schema)
    if spec.update_type == "general":
        final = _sort(data, spec)
        result = parquet.write_partition_file_result(
            spec,
            final,
            Path("data.parquet"),
            {},
            existing_manifest=manifests.get("data.parquet"),
            retain_backup=True,
            write_context=write_context,
        )
        try:
            parquet.commit_metadata(
                spec,
                canonical_schema,
                [result.manifest] if result.rewritten else [],
                replace_manifests=result.rewritten,
                write_context=write_context,
            )
        except BaseException:
            rollback_partition_writes([result])
            raise
        finalize_partition_writes([result])
        commit = CommitResult(
            rows_committed=final.height,
            partitions_rewritten=int(result.rewritten),
            partitions_skipped=int(not result.rewritten),
            bytes_written=result.bytes_written,
        )
    elif spec.update_type == "by_daily":
        commit = _write_grouped(
            data,
            spec,
            parquet,
            ("year", "month"),
            canonical_schema,
            manifests,
            write_context,
            writer_executor,
        )
    elif spec.update_type == "by_asset":
        commit = _write_grouped(
            data,
            spec,
            parquet,
            ("year", "bucket"),
            canonical_schema,
            manifests,
            write_context,
            writer_executor,
        )
    else:
        raise ValueError(f"Unsupported update_type: {spec.update_type}")
    return commit


def _write_grouped(
    data: pl.DataFrame,
    spec: DatasetSpec,
    parquet: ParquetStore,
    group_columns: tuple[str, ...],
    canonical_schema: pl.Schema,
    existing_manifests: dict[str, dict[str, Any]],
    write_context: PartitionWriteContext,
    writer_executor: ThreadPoolExecutor | None,
) -> CommitResult:
    row_count = 0
    manifests: list[dict[str, Any]] = []
    rewritten = 0
    skipped = 0
    bytes_written = 0
    coverage_frames: list[pl.DataFrame] = []
    writes_by_index: dict[int, PartitionWriteResult] = {}
    futures: dict[Future[PartitionWriteResult], int] = {}
    failure: BaseException | None = None
    owns_executor = writer_executor is None
    executor = writer_executor or ThreadPoolExecutor(
        max_workers=MAX_PARQUET_WRITE_WORKERS,
        thread_name_prefix="bagelquant-parquet",
    )
    try:
        try:
            for index, (values, group) in enumerate(
                data.group_by(group_columns, maintain_order=True)
            ):
                if len(futures) >= MAX_PARQUET_WRITE_WORKERS:
                    completed, _ = wait(futures, return_when=FIRST_COMPLETED)
                    failure = _collect_write_results(
                        completed, futures, writes_by_index, failure
                    )
                else:
                    completed = {future for future in futures if future.done()}
                    failure = _collect_write_results(
                        completed, futures, writes_by_index, failure
                    )
                if failure is not None:
                    break
                if not isinstance(values, tuple):
                    values = (values,)
                partition_values: dict[str, object] = dict(
                    zip(group_columns, values, strict=True)
                )
                path = _partition_path(spec, partition_values)
                existing_manifest = existing_manifests.get(path.as_posix())
                final = _merge_partition(
                    existing=(
                        None
                        if existing_manifest is None
                        else _read_existing(parquet, spec, path)
                    ),
                    incoming=group,
                    spec=spec,
                )
                final = align_frame(final, canonical_schema)
                coverage_frames.append(final.select("time", "asset_id"))
                future = executor.submit(
                    _write_partition,
                    parquet=parquet,
                    spec=spec,
                    frame=final,
                    path=path,
                    partition_values=partition_values,
                    existing_manifest=existing_manifest,
                    write_context=write_context,
                )
                futures[future] = index
                row_count += final.height
        except BaseException as error:
            failure = error
        completed, _ = wait(futures)
        failure = _collect_write_results(
            completed, futures, writes_by_index, failure
        )
    finally:
        if owns_executor:
            executor.shutdown(wait=True, cancel_futures=False)

    writes = [writes_by_index[index] for index in sorted(writes_by_index)]
    if failure is not None:
        rollback_partition_writes(writes)
        raise failure
    for result in writes:
        if result.rewritten:
            manifests.append(result.manifest)
            rewritten += 1
            bytes_written += result.bytes_written
        else:
            skipped += 1
    try:
        present_times, asset_max_times = _coverage(
            coverage_frames,
            spec,
        )
        parquet.commit_metadata(
            spec,
            canonical_schema,
            manifests,
            write_context=write_context,
        )
    except BaseException:
        rollback_partition_writes(writes)
        raise
    finalize_partition_writes(writes)
    return CommitResult(
        rows_committed=row_count,
        partitions_rewritten=rewritten,
        partitions_skipped=skipped,
        bytes_written=bytes_written,
        present_times=frozenset(present_times),
        asset_max_times=tuple(sorted(asset_max_times.items())),
    )


def _write_partition(
    *,
    parquet: ParquetStore,
    spec: DatasetSpec,
    frame: pl.DataFrame,
    path: Path,
    partition_values: dict[str, object],
    existing_manifest: dict[str, Any] | None,
    write_context: PartitionWriteContext,
) -> PartitionWriteResult:
    """Sort and publish one partition entirely inside a writer worker."""

    return parquet.write_partition_file_result(
        spec,
        _sort(frame, spec),
        path,
        partition_values,
        existing_manifest=existing_manifest,
        retain_backup=True,
        write_context=write_context,
    )


def _collect_write_results(
    completed: set[Future[PartitionWriteResult]],
    futures: dict[Future[PartitionWriteResult], int],
    results: dict[int, PartitionWriteResult],
    failure: BaseException | None,
) -> BaseException | None:
    """Collect every settled writer while retaining the first failure."""

    for future in completed:
        index = futures.pop(future)
        try:
            results[index] = future.result()
        except BaseException as error:
            if failure is None:
                failure = error
    return failure


def _coverage(
    frames: list[pl.DataFrame],
    spec: DatasetSpec,
) -> tuple[set[str], dict[str, str]]:
    if not frames:
        return set(), {}
    frame = pl.concat(frames, how="vertical", rechunk=False)
    if spec.update_type == "by_daily":
        return {str(value) for value in frame["time"].unique()}, {}
    if spec.update_type == "by_asset":
        return (
            set(),
            {
                str(asset_id): str(maximum)
                for asset_id, maximum in frame.group_by("asset_id")
                .agg(pl.col("time").max())
                .iter_rows()
            },
        )
    return set(), {}


def _merge_partition(
    *,
    existing: pl.DataFrame | None,
    incoming: pl.DataFrame,
    spec: DatasetSpec,
) -> pl.DataFrame:
    if existing is None:
        return incoming
    merged = concat_compatible_frames([existing, incoming])
    return _deduplicate(merged, spec)


def _read_existing(
    parquet: ParquetStore, spec: DatasetSpec, relative_path: Path
) -> pl.DataFrame | None:
    path = parquet.paths.dataset_root(spec.source, spec.name) / relative_path
    if not path.exists():
        return None
    return pl.read_parquet(path)


def _sort(frame: pl.DataFrame, spec: DatasetSpec) -> pl.DataFrame:
    key = incremental_key(spec)
    if key:
        return frame.sort(*key)
    return frame


def _deduplicate(frame: pl.DataFrame, spec: DatasetSpec) -> pl.DataFrame:
    key = incremental_key(spec)
    if key is None:
        return frame.unique(maintain_order=True)
    return frame.unique(subset=list(key), keep="last", maintain_order=True)


def _derive_partition_columns(
    frame: pl.DataFrame, spec: DatasetSpec
) -> pl.DataFrame:
    if spec.update_type == "by_daily":
        return frame.with_columns(
            pl.col("time").dt.year().cast(pl.Int16).alias("year"),
            pl.col("time").dt.month().cast(pl.Int8).alias("month"),
        )
    if spec.update_type == "by_asset":
        assets = frame.select(
            pl.col("asset_id").cast(pl.String).unique()
        )["asset_id"]
        bucket_map = pl.DataFrame(
            {
                "asset_id": assets,
                "bucket": [
                    stable_bucket(value, spec.asset_bucket_count) for value in assets
                ],
            },
            schema_overrides={"asset_id": pl.String, "bucket": pl.Int16},
        )
        return frame.with_columns(
            pl.col("time").dt.year().cast(pl.Int16).alias("year"),
            pl.col("asset_id").cast(pl.String),
        ).join(bucket_map, on="asset_id", how="left")
    return frame


def _partition_path(spec: DatasetSpec, values: dict[str, object]) -> Path:
    if spec.update_type == "by_daily":
        return (
            Path(f"year={values['year']}")
            / f"month={int(str(values['month'])):02d}"
            / "data.parquet"
        )
    if spec.update_type == "by_asset":
        return (
            Path(f"year={values['year']}")
            / f"bucket={int(str(values['bucket'])):02d}"
            / "data.parquet"
        )
    return Path("data.parquet")
