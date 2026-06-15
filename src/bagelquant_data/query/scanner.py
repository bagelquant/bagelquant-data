"""Parquet scan planning."""

from __future__ import annotations

from pathlib import Path

from bagelquant_data.storage.metadata import MetadataStore


def manifest_paths(metadata: MetadataStore, source: str, dataset: str) -> list[Path]:
    """Return known manifest paths for a dataset."""

    return [Path(row["partition_path"]) for row in metadata.manifest(source, dataset)]
