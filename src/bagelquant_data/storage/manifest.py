"""Manifest read helpers."""

from __future__ import annotations

from bagelquant_data.storage.metadata import MetadataStore


class ManifestStore:
    """Thin manifest facade over SQLite."""

    def __init__(self, metadata: MetadataStore) -> None:
        self.metadata = metadata

    def list(self, source: str | None = None, dataset: str | None = None) -> list[dict[str, object]]:
        return self.metadata.manifest(source=source, dataset=dataset)
