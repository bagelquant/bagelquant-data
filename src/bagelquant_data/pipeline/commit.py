"""Canonical commit pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import polars as pl

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.deduplication import DeduplicationStrategy
from bagelquant_data.core.partitioning import PartitionStrategy
from bagelquant_data.core.registry import FrameworkRegistries
from bagelquant_data.core.types import DateLike
from bagelquant_data.core.validation import Validator
from bagelquant_data.storage.parquet import ParquetStore


def commit_frame(
    *,
    spec: DatasetSpec,
    frame: pl.LazyFrame,
    registries: FrameworkRegistries,
    parquet: ParquetStore,
    mode: str = "upsert",
    update_start: DateLike | None = None,
    update_end: DateLike | None = None,
    replace_assets: set[str] | None = None,
) -> int:
    """Validate, deduplicate, partition, and write canonical records."""

    validator = cast(Validator, registries.validators.get("framework"))
    partitioner = cast(PartitionStrategy, registries.partition_strategies.get(spec.partition_strategy))
    deduper = cast(DeduplicationStrategy, registries.deduplication_strategies.get(spec.deduplication))
    prepared = partitioner.derive_columns(frame, spec)
    validator.validate(prepared, spec)
    data = prepared.collect()
    if spec.partition_strategy == "year_month":
        return _write_grouped(
            data,
            spec,
            parquet,
            partitioner,
            ("year", "month"),
            deduper=deduper,
            mode=mode,
            update_start=update_start,
            update_end=update_end,
            replace_assets=replace_assets,
        )
    if spec.partition_strategy == "year_bucket":
        return _write_grouped(
            data,
            spec,
            parquet,
            partitioner,
            ("year", "bucket"),
            deduper=deduper,
            mode=mode,
            update_start=update_start,
            update_end=update_end,
            replace_assets=replace_assets,
        )
    if spec.partition_strategy == "ten_year_range":
        return _write_grouped(
            data,
            spec,
            parquet,
            partitioner,
            ("year_range",),
            deduper=deduper,
            mode=mode,
            update_start=update_start,
            update_end=update_end,
            replace_assets=replace_assets,
        )
    final = _merge_partition(
        existing=_read_existing(parquet, spec, Path("data.parquet")),
        incoming=data,
        spec=spec,
        deduper=deduper,
        mode=mode,
        update_start=update_start,
        update_end=update_end,
        replace_assets=replace_assets,
    )
    final = _sort(final, spec)
    parquet.write_partition(spec, final, Path("data.parquet"), {})
    return final.height


def _write_grouped(
    data: pl.DataFrame,
    spec: DatasetSpec,
    parquet: ParquetStore,
    partitioner: PartitionStrategy,
    group_columns: tuple[str, ...],
    *,
    deduper: DeduplicationStrategy,
    mode: str,
    update_start: DateLike | None,
    update_end: DateLike | None,
    replace_assets: set[str] | None,
) -> int:
    row_count = 0
    manifests: list[dict[str, Any]] = []
    for values, group in data.group_by(group_columns, maintain_order=True):
        if not isinstance(values, tuple):
            values = (values,)
        partition_values: dict[str, object] = dict(zip(group_columns, values, strict=True))
        path = partitioner.path_for_values(spec, partition_values)
        final = _merge_partition(
            existing=_read_existing(parquet, spec, path),
            incoming=group,
            spec=spec,
            deduper=deduper,
            mode=mode,
            update_start=update_start,
            update_end=update_end,
            replace_assets=replace_assets,
        )
        final = _sort(final, spec)
        _, manifest = parquet.write_partition_file(spec, final, path, partition_values)
        manifests.append(manifest)
        row_count += final.height
    parquet.metadata.upsert_manifests(manifests)
    return row_count


def _merge_partition(
    *,
    existing: pl.DataFrame | None,
    incoming: pl.DataFrame,
    spec: DatasetSpec,
    deduper: DeduplicationStrategy,
    mode: str,
    update_start: DateLike | None,
    update_end: DateLike | None,
    replace_assets: set[str] | None,
) -> pl.DataFrame:
    if existing is None or mode == "snapshot_replace":
        return deduper.apply(incoming.lazy(), spec).collect()
    if mode == "replace_asset":
        existing = _remove_replace_asset_window(
            existing,
            assets=replace_assets or _assets_from_frame(incoming),
            start=update_start,
            end=update_end,
        )
    merged = pl.concat([existing, incoming], how="diagonal_relaxed")
    return deduper.apply(merged.lazy(), spec).collect()


def _remove_replace_asset_window(
    frame: pl.DataFrame,
    *,
    assets: set[str],
    start: DateLike | None,
    end: DateLike | None,
) -> pl.DataFrame:
    if not assets or "asset_id" not in frame.columns:
        return frame
    replace_expr = pl.col("asset_id").cast(pl.String).is_in(sorted(assets))
    if start is not None and "time" in frame.columns:
        replace_expr = replace_expr & (pl.col("time") >= _date_literal(start))
    if end is not None and "time" in frame.columns:
        replace_expr = replace_expr & (pl.col("time") <= _date_literal(end))
    return frame.filter(~replace_expr)


def _read_existing(parquet: ParquetStore, spec: DatasetSpec, relative_path: Path) -> pl.DataFrame | None:
    path = parquet.paths.dataset_root(spec.source, spec.name) / relative_path
    if not path.exists():
        return None
    return pl.read_parquet(path)


def _sort(frame: pl.DataFrame, spec: DatasetSpec) -> pl.DataFrame:
    if spec.sort_columns:
        return frame.sort(*spec.sort_columns)
    return frame


def _assets_from_frame(frame: pl.DataFrame) -> set[str]:
    if "asset_id" not in frame.columns:
        return set()
    return {str(value) for value in frame.get_column("asset_id").drop_nulls().unique().to_list()}


def _date_literal(value: DateLike) -> Any:
    return pl.lit(value).cast(pl.Date, strict=False)
