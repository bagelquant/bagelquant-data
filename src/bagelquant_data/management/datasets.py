"""Dataset management API."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.exceptions import DatasetNotFoundError, DestructiveOperationError
from bagelquant_data.storage.metadata import MetadataStore
from bagelquant_data.storage.paths import LakePaths


class DatasetManager:
    """Register and inspect dataset specifications."""

    def __init__(self, metadata: MetadataStore, paths: LakePaths) -> None:
        self.metadata = metadata
        self.paths = paths
        self._specs: dict[tuple[str, str], DatasetSpec] = {}

    def add(self, spec: DatasetSpec) -> None:
        self.validate_spec(spec)
        self._specs[spec.key] = spec
        self.metadata.upsert_dataset(spec)

    def add_from_yaml(self, path: str | Path) -> DatasetSpec:
        spec = DatasetSpec.from_yaml(path)
        self.add(spec)
        return spec

    def get(self, dataset: str, *, source: str) -> DatasetSpec:
        key = (source, dataset)
        if key in self._specs:
            return self._specs[key]
        row = self.metadata.get_dataset(source, dataset)
        if row is None:
            raise DatasetNotFoundError(f"Dataset is not registered: {source}/{dataset}")
        spec = DatasetSpec.from_mapping(__import__("json").loads(row["spec_json"]))
        self._specs[key] = spec
        return spec

    def list(self, source: str | None = None) -> list[dict[str, Any]]:
        return self.metadata.list_datasets(source)

    def enable(self, dataset: str, *, source: str) -> None:
        self.metadata.set_dataset_enabled(source, dataset, True)

    def disable(self, dataset: str, *, source: str) -> None:
        self.metadata.set_dataset_enabled(source, dataset, False)

    def validate_spec(self, spec: DatasetSpec) -> None:
        if not spec.reference and ("asset_id" not in spec.required_columns or "time" not in spec.required_columns):
            pass

    def remove(self, dataset: str, *, source: str, delete_data: bool = False, confirm: bool = False) -> None:
        if delete_data and not confirm:
            raise DestructiveOperationError("Pass confirm=True to delete canonical data")
        self.metadata.remove_dataset(source, dataset)
        self._specs.pop((source, dataset), None)
        if delete_data:
            shutil.rmtree(self.paths.dataset_root(source, dataset), ignore_errors=True)
