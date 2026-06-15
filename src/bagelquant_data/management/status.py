"""Status and inspection API."""

from __future__ import annotations

from typing import Any

from bagelquant_data.storage.metadata import MetadataStore


class StatusManager:
    """Manifest-driven status queries."""

    def __init__(self, metadata: MetadataStore) -> None:
        self.metadata = metadata

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

    def dataset(self, dataset: str, *, source: str, deep: bool = False) -> dict[str, Any]:
        manifest = self.metadata.manifest(source, dataset)
        return {
            "source": source,
            "dataset": dataset,
            "file_count": len(manifest),
            "partition_count": len(manifest),
            "total_size": sum(int(row["file_size_bytes"]) for row in manifest),
            "row_count": sum(int(row["row_count"]) for row in manifest),
            "minimum_time": min((row["min_time"] for row in manifest if row["min_time"]), default=None),
            "maximum_time": max((row["max_time"] for row in manifest if row["max_time"]), default=None),
            "last_update": max((row["updated_at"] for row in manifest), default=None),
            "deep": deep,
        }

    def partitions(self, dataset: str, *, source: str) -> list[dict[str, Any]]:
        return self.metadata.manifest(source, dataset)

    def runs(self, limit: int = 20) -> list[dict[str, Any]]:
        return self.metadata.runs(limit)

    def failures(self, dataset: str | None = None, source: str | None = None) -> list[dict[str, Any]]:
        return [run for run in self.metadata.runs(1000) if run["status"] != "success"]

    def rejected(self, dataset: str, *, source: str) -> list[dict[str, Any]]:
        return []

    def files(self, dataset: str, *, source: str) -> list[dict[str, Any]]:
        return self.partitions(dataset, source=source)
