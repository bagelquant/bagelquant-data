"""Status and inspection API."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import polars as pl

from bagelquant_data.core.hashing import frame_content_hash
from bagelquant_data.storage.metadata import MetadataStore
from bagelquant_data.storage.paths import LakePaths


class StatusManager:
    """Manifest-driven status queries."""

    def __init__(self, metadata: MetadataStore, paths: LakePaths) -> None:
        self.metadata = metadata
        self.paths = paths

    def summary(self) -> dict[str, Any]:
        datasets = self.metadata.list_datasets()
        manifest = self.metadata.manifest()
        return {
            "sources": len(self.metadata.list_sources()),
            "datasets": len(datasets),
            "partitions": len(manifest),
            "rows": sum(int(row["row_count"]) for row in manifest),
            "bytes": sum(int(row["file_size_bytes"]) for row in manifest),
        }

    def dataset(
        self, dataset: str, *, source: str, deep: bool = False
    ) -> dict[str, Any]:
        manifest = self.metadata.manifest(source, dataset)
        return {
            "source": source,
            "dataset": dataset,
            "file_count": len(manifest),
            "partition_count": len(manifest),
            "total_size": sum(int(row["file_size_bytes"]) for row in manifest),
            "row_count": sum(int(row["row_count"]) for row in manifest),
            "minimum_time": min(
                (row["min_time"] for row in manifest if row["min_time"]), default=None
            ),
            "maximum_time": max(
                (row["max_time"] for row in manifest if row["max_time"]), default=None
            ),
            "last_update": max((row["updated_at"] for row in manifest), default=None),
            "deep": deep,
        }

    def partitions(self, dataset: str, *, source: str) -> list[dict[str, Any]]:
        return self.metadata.manifest(source, dataset)

    def runs(self, limit: int = 20) -> list[dict[str, Any]]:
        return self.metadata.runs(limit)

    def failures(
        self, dataset: str | None = None, source: str | None = None
    ) -> list[dict[str, Any]]:
        return [
            run
            for run in self.metadata.runs(1000)
            if run["status"] != "success"
            and (dataset is None or run["dataset"] == dataset)
            and (source is None or run["source"] == source)
        ]

    def pending_update_jobs(
        self,
        dataset: str | None = None,
        source: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return unresolved logical source jobs awaiting a later retry."""

        return self.metadata.pending_update_jobs(source=source, dataset=dataset)

    def rejected(self, dataset: str, *, source: str) -> list[dict[str, Any]]:
        return self.metadata.rejected(source, dataset)

    def files(self, dataset: str, *, source: str) -> list[dict[str, Any]]:
        root = self.paths.dataset_root(source, dataset)
        rows = self.partitions(dataset, source=source)
        for row in rows:
            path = root / row["partition_path"]
            row["path"] = str(path)
            row["exists"] = path.exists()
        return rows

    def rebuild_manifest(self, dataset: str, *, source: str) -> dict[str, Any]:
        root = self.paths.dataset_root(source, dataset)
        manifests: list[dict[str, Any]] = []
        for path in sorted(root.glob("**/*.parquet")):
            relative_path = path.relative_to(root)
            frame = pl.read_parquet(path)
            time_values = (
                frame.select(
                    pl.min("time").alias("min_time"), pl.max("time").alias("max_time")
                ).row(0)
                if "time" in frame.columns and frame.height
                else (None, None)
            )
            manifests.append(
                {
                    "source": source,
                    "dataset": dataset,
                    "partition_path": relative_path.as_posix(),
                    "partition_values": _partition_values(relative_path),
                    "row_count": frame.height,
                    "file_size_bytes": path.stat().st_size,
                    "min_time": str(time_values[0])
                    if time_values[0] is not None
                    else None,
                    "max_time": str(time_values[1])
                    if time_values[1] is not None
                    else None,
                    "content_hash": frame_content_hash(frame),
                    "schema_hash": _schema_hash(frame),
                }
            )
        self.metadata.replace_manifests(source, dataset, manifests)
        return {
            "source": source,
            "dataset": dataset,
            "files_scanned": len(manifests),
            "rows": sum(int(row["row_count"]) for row in manifests),
            "bytes": sum(int(row["file_size_bytes"]) for row in manifests),
        }

    def validate_manifest(self, dataset: str, *, source: str) -> dict[str, Any]:
        files = self.files(dataset, source=source)
        missing = [row["partition_path"] for row in files if not row["exists"]]
        return {
            "source": source,
            "dataset": dataset,
            "manifest_files": len(files),
            "missing_files": missing,
            "valid": not missing,
        }


def _partition_values(relative_path: Path) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for part in relative_path.parts[:-1]:
        key, sep, value = part.partition("=")
        if sep:
            values[key] = _partition_scalar(value)
    return values


def _partition_scalar(value: str) -> object:
    try:
        return int(value)
    except ValueError:
        return value


def _schema_hash(frame: pl.DataFrame) -> str:
    payload = "|".join(f"{name}:{dtype}" for name, dtype in frame.schema.items())
    return hashlib.blake2b(payload.encode("utf-8"), digest_size=16).hexdigest()
