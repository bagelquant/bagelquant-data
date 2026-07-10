"""Canonical commit pipeline."""

from __future__ import annotations

from pathlib import Path
import shutil
from typing import Any, cast

import polars as pl

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.deduplication import DeduplicationStrategy
from bagelquant_data.core.hashing import stable_bucket
from bagelquant_data.core.registry import FrameworkRegistries
from bagelquant_data.core.validation import Validator
from bagelquant_data.storage.parquet import ParquetStore


def commit_frame(
    *,
    spec: DatasetSpec,
    frame: pl.LazyFrame,
    registries: FrameworkRegistries,
    parquet: ParquetStore,
) -> int:
    """Validate, deduplicate, partition, and write canonical records."""

    validator = cast(Validator, registries.validators.get("framework"))
    deduper = cast(DeduplicationStrategy, registries.deduplication_strategies.get(spec.deduplication))
    prepared = _derive_partition_columns(frame, spec)
    validator.validate(prepared, spec)
    data = prepared.collect()
    if spec.update_type == "general":
        final = deduper.apply(data.lazy(), spec).collect()
        final = _sort(final, spec)
        shutil.rmtree(parquet.paths.dataset_root(spec.source, spec.name), ignore_errors=True)
        _, manifest = parquet.write_partition_file(spec, final, Path("data.parquet"), {})
        parquet.metadata.replace_manifests(spec.source, spec.name, [manifest])
        return final.height
    if spec.update_type == "by_daily":
        return _write_grouped(
            data,
            spec,
            parquet,
            ("year", "month"),
            deduper=deduper,
        )
    if spec.update_type == "by_id":
        return _write_grouped(
            data,
            spec,
            parquet,
            ("year", "bucket"),
            deduper=deduper,
        )
    raise ValueError(f"Unsupported update_type: {spec.update_type}")


def _write_grouped(
    data: pl.DataFrame,
    spec: DatasetSpec,
    parquet: ParquetStore,
    group_columns: tuple[str, ...],
    *,
    deduper: DeduplicationStrategy,
) -> int:
    row_count = 0
    manifests: list[dict[str, Any]] = []
    for values, group in data.group_by(group_columns, maintain_order=True):
        if not isinstance(values, tuple):
            values = (values,)
        partition_values: dict[str, object] = dict(zip(group_columns, values, strict=True))
        path = _partition_path(spec, partition_values)
        final = _merge_partition(
            existing=_read_existing(parquet, spec, path),
            incoming=group,
            spec=spec,
            deduper=deduper,
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
) -> pl.DataFrame:
    if existing is None:
        return deduper.apply(incoming.lazy(), spec).collect()
    merged = pl.concat([existing, incoming], how="diagonal_relaxed")
    return deduper.apply(merged.lazy(), spec).collect()


def _read_existing(parquet: ParquetStore, spec: DatasetSpec, relative_path: Path) -> pl.DataFrame | None:
    path = parquet.paths.dataset_root(spec.source, spec.name) / relative_path
    if not path.exists():
        return None
    return pl.read_parquet(path)


def _sort(frame: pl.DataFrame, spec: DatasetSpec) -> pl.DataFrame:
    if spec.sort_columns:
        return frame.sort(*spec.sort_columns)
    return frame


def _derive_partition_columns(frame: pl.LazyFrame, spec: DatasetSpec) -> pl.LazyFrame:
    if spec.update_type == "by_daily":
        return frame.with_columns(
            pl.col("time").dt.year().cast(pl.Int16).alias("year"),
            pl.col("time").dt.month().cast(pl.Int8).alias("month"),
        )
    if spec.update_type == "by_id":
        return frame.with_columns(
            pl.col("time").dt.year().cast(pl.Int16).alias("year"),
            pl.col("asset_id")
            .cast(pl.String)
            .map_elements(lambda value: stable_bucket(value, spec.batch_count), return_dtype=pl.Int16)
            .alias("bucket"),
        )
    return frame


def _partition_path(spec: DatasetSpec, values: dict[str, object]) -> Path:
    if spec.update_type == "by_daily":
        return Path(f"year={values['year']}") / f"month={int(str(values['month'])):02d}" / "data.parquet"
    if spec.update_type == "by_id":
        return Path(f"year={values['year']}") / f"batch={int(str(values['bucket'])):02d}" / "data.parquet"
    return Path("data.parquet")
