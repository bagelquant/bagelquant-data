"""Canonical Parquet storage."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import polars as pl

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.hashing import frame_content_hash
from bagelquant_data.storage.atomic import atomic_write_parquet
from bagelquant_data.storage.metadata import MetadataStore
from bagelquant_data.storage.paths import LakePaths


class ParquetStore:
    """Read and write canonical lake Parquet files."""

    def __init__(self, paths: LakePaths, metadata: MetadataStore) -> None:
        self.paths = paths
        self.metadata = metadata

    def write_partition(
        self,
        spec: DatasetSpec,
        frame: pl.DataFrame,
        relative_path: Path,
        partition_values: dict[str, Any] | None = None,
    ) -> Path:
        path, manifest = self.write_partition_file(spec, frame, relative_path, partition_values)
        self.metadata.upsert_manifest(**manifest)
        return path

    def write_partition_file(
        self,
        spec: DatasetSpec,
        frame: pl.DataFrame,
        relative_path: Path,
        partition_values: dict[str, Any] | None = None,
    ) -> tuple[Path, dict[str, Any]]:
        path = self.paths.dataset_root(spec.source, spec.name) / relative_path
        atomic_write_parquet(frame, path)
        time_values = (
            frame.select(pl.min("time").alias("min_time"), pl.max("time").alias("max_time")).row(0)
            if "time" in frame.columns and frame.height
            else (None, None)
        )
        return path, {
            "source": spec.source,
            "dataset": spec.name,
            "partition_path": relative_path.as_posix(),
            "partition_values": partition_values or {},
            "row_count": frame.height,
            "file_size_bytes": path.stat().st_size,
            "min_time": str(time_values[0]) if time_values[0] is not None else None,
            "max_time": str(time_values[1]) if time_values[1] is not None else None,
            "content_hash": frame_content_hash(frame),
            "schema_hash": _schema_hash(frame),
        }

    def scan_dataset(self, source: str, dataset: str, paths: list[Path] | None = None) -> pl.LazyFrame:
        root = self.paths.dataset_root(source, dataset)
        if paths:
            files = [root / path for path in paths]
        else:
            files = sorted(root.glob("**/*.parquet"))
        if not files:
            from bagelquant_data.core.exceptions import DatasetNotFoundError

            raise DatasetNotFoundError(f"No canonical data for {source}/{dataset}")
        return pl.scan_parquet([str(path) for path in files])


def _schema_hash(frame: pl.DataFrame) -> str:
    payload = "|".join(f"{name}:{dtype}" for name, dtype in frame.schema.items())
    return hashlib.blake2b(payload.encode("utf-8"), digest_size=16).hexdigest()
