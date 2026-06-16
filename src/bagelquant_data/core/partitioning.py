"""Partition strategies."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

import polars as pl

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.hashing import stable_bucket


class PartitionStrategy(Protocol):
    """Derive partition values and paths."""

    def derive_columns(self, frame: pl.LazyFrame, spec: DatasetSpec) -> pl.LazyFrame:
        """Add partition columns."""
        ...

    def paths_for_query(self, spec: DatasetSpec, query: object) -> list[Path]:
        """Return candidate partition paths."""
        ...

    def path_for_values(self, spec: DatasetSpec, values: dict[str, object]) -> Path:
        """Return partition path for values."""
        ...


class SingleFilePartition:
    """One canonical file per dataset."""

    def derive_columns(self, frame: pl.LazyFrame, spec: DatasetSpec) -> pl.LazyFrame:
        return frame

    def paths_for_query(self, spec: DatasetSpec, query: object) -> list[Path]:
        return [Path("data.parquet")]

    def path_for_values(self, spec: DatasetSpec, values: dict[str, object]) -> Path:
        return Path("data.parquet")


class YearMonthPartition:
    """Partition by year and month of canonical time."""

    def derive_columns(self, frame: pl.LazyFrame, spec: DatasetSpec) -> pl.LazyFrame:
        return frame.with_columns(
            pl.col("time").dt.year().cast(pl.Int16).alias("year"),
            pl.col("time").dt.month().cast(pl.Int8).alias("month"),
        )

    def paths_for_query(self, spec: DatasetSpec, query: object) -> list[Path]:
        return []

    def path_for_values(self, spec: DatasetSpec, values: dict[str, object]) -> Path:
        return Path(f"year={values['year']}") / f"month={int(str(values['month'])):02d}" / "data.parquet"


class YearBucketPartition:
    """Partition by year(time) and stable asset bucket."""

    def derive_columns(self, frame: pl.LazyFrame, spec: DatasetSpec) -> pl.LazyFrame:
        bucket_count = int(spec.partition_options.get("bucket_count", 32))
        return frame.with_columns(
            pl.col("time").dt.year().cast(pl.Int16).alias("year"),
            pl.col("asset_id")
            .cast(pl.String)
            .map_elements(lambda value: stable_bucket(value, bucket_count), return_dtype=pl.Int16)
            .alias("bucket"),
        )

    def paths_for_query(self, spec: DatasetSpec, query: object) -> list[Path]:
        return []

    def path_for_values(self, spec: DatasetSpec, values: dict[str, object]) -> Path:
        return Path(f"year={values['year']}") / f"bucket={int(str(values['bucket'])):02d}" / "data.parquet"


class TenYearRangePartition:
    """Partition by 10-year ranges of canonical time."""

    def derive_columns(self, frame: pl.LazyFrame, spec: DatasetSpec) -> pl.LazyFrame:
        chunk_years = int(spec.partition_options.get("chunk_years", 10))
        year = pl.col("time").dt.year()
        start_year = (year // chunk_years) * chunk_years
        end_year = start_year + chunk_years - 1
        return frame.with_columns(
            pl.format("{}-{}", start_year.cast(pl.String), end_year.cast(pl.String)).alias("year_range")
        )

    def paths_for_query(self, spec: DatasetSpec, query: object) -> list[Path]:
        return []

    def path_for_values(self, spec: DatasetSpec, values: dict[str, object]) -> Path:
        return Path(f"year_range={values['year_range']}") / "data.parquet"
