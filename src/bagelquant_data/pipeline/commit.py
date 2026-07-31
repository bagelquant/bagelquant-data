"""Canonical commit pipeline."""

from __future__ import annotations

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
    PartitionWriteResult,
    finalize_partition_writes,
    rollback_partition_writes,
)


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
    manifests = {
        str(row["partition_path"]): row
        for row in parquet.metadata.manifest(spec.source, spec.name)
    }
    if spec.update_type == "general":
        final = _deduplicate(data, spec)
        final = _sort(final, spec)
        result = parquet.write_partition_file_result(
            spec,
            final,
            Path("data.parquet"),
            {},
            existing_manifest=manifests.get("data.parquet"),
            retain_backup=True,
        )
        try:
            parquet.commit_metadata(
                spec,
                canonical_schema,
                [result.manifest] if result.rewritten else [],
                replace_manifests=result.rewritten,
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
        )
    elif spec.update_type == "by_asset":
        commit = _write_grouped(
            data,
            spec,
            parquet,
            ("year", "bucket"),
            canonical_schema,
            manifests,
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
) -> CommitResult:
    row_count = 0
    manifests: list[dict[str, Any]] = []
    rewritten = 0
    skipped = 0
    bytes_written = 0
    present_times: set[str] = set()
    asset_max_times: dict[str, str] = {}
    writes: list[PartitionWriteResult] = []
    try:
        for values, group in data.group_by(group_columns, maintain_order=True):
            if not isinstance(values, tuple):
                values = (values,)
            partition_values: dict[str, object] = dict(
                zip(group_columns, values, strict=True)
            )
            path = _partition_path(spec, partition_values)
            final = _merge_partition(
                existing=_read_existing(parquet, spec, path),
                incoming=group,
                spec=spec,
            )
            final = align_frame(final, canonical_schema)
            final = _sort(final, spec)
            _accumulate_coverage(
                final,
                spec,
                present_times=present_times,
                asset_max_times=asset_max_times,
            )
            result = parquet.write_partition_file_result(
                spec,
                final,
                path,
                partition_values,
                existing_manifest=existing_manifests.get(path.as_posix()),
                retain_backup=True,
            )
            writes.append(result)
            if result.rewritten:
                manifests.append(result.manifest)
                rewritten += 1
                bytes_written += result.bytes_written
            else:
                skipped += 1
            row_count += final.height
        parquet.commit_metadata(spec, canonical_schema, manifests)
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


def _accumulate_coverage(
    frame: pl.DataFrame,
    spec: DatasetSpec,
    *,
    present_times: set[str],
    asset_max_times: dict[str, str],
) -> None:
    if spec.update_type == "by_daily":
        present_times.update(str(value) for value in frame["time"].unique())
        return
    if spec.update_type != "by_asset":
        return
    for asset_id, maximum in frame.group_by("asset_id").agg(
        pl.col("time").max()
    ).iter_rows():
        value = str(maximum)
        current = asset_max_times.get(str(asset_id))
        if current is None or current < value:
            asset_max_times[str(asset_id)] = value


def _merge_partition(
    *,
    existing: pl.DataFrame | None,
    incoming: pl.DataFrame,
    spec: DatasetSpec,
) -> pl.DataFrame:
    if existing is None:
        return _deduplicate(incoming, spec)
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
