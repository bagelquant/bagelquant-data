"""Dataset management API."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.exceptions import DatasetNotFoundError, DatasetSpecError, DestructiveOperationError
from bagelquant_data.storage.metadata import MetadataStore
from bagelquant_data.storage.paths import LakePaths


class DatasetManager:
    """Register and inspect dataset specifications."""

    def __init__(self, metadata: MetadataStore, paths: LakePaths) -> None:
        self.metadata = metadata
        self.paths = paths
        self._specs: dict[tuple[str, str], DatasetSpec] = {}

    def add(
        self,
        spec: DatasetSpec | str,
        update_type: str | None = None,
        reference: str | bool | None = None,
        **kwargs: Any,
    ) -> DatasetSpec:
        if isinstance(spec, str):
            spec = DatasetSpec(spec, update_type or "general", reference, **kwargs)
        elif update_type is not None or reference is not None or kwargs:
            raise DatasetSpecError("Pass either a DatasetSpec or compact dataset registration arguments")
        self.validate_spec(spec)
        self._specs[spec.key] = spec
        self.metadata.upsert_dataset(spec)
        return spec

    def register(
        self,
        name: str,
        update_type: str,
        reference: str | bool | None = None,
        **kwargs: Any,
    ) -> DatasetSpec:
        """Register a dataset with the compact public API."""

        return self.add(name, update_type, reference, **kwargs)

    def edit(self, spec: DatasetSpec) -> None:
        """Replace a registered dataset specification."""

        self.add(spec)

    def add_from_yaml(self, path: str | Path) -> DatasetSpec:
        spec = DatasetSpec.from_yaml(path)
        self.add(spec)
        return spec

    def get(self, dataset: str, *, source: str) -> DatasetSpec:
        key = (source, dataset)
        if key in self._specs:
            return self._specs[key]
        row = self.metadata.get_dataset(source, dataset)
        if row is None and source != "custom":
            row = self.metadata.get_dataset("custom", dataset)
        if row is None:
            raise DatasetNotFoundError(f"Dataset is not registered: {source}/{dataset}")
        payload = __import__("json").loads(row["spec_json"])
        payload["source"] = source
        spec = DatasetSpec.from_mapping(payload)
        self._specs[key] = spec
        return spec

    def list(self, source: str | None = None) -> list[dict[str, Any]]:
        return self.metadata.list_datasets(source)

    def enable(self, dataset: str, *, source: str) -> None:
        self.metadata.set_dataset_enabled(source, dataset, True)

    def disable(self, dataset: str, *, source: str) -> None:
        self.metadata.set_dataset_enabled(source, dataset, False)

    def validate_spec(self, spec: DatasetSpec) -> None:
        if spec.update_type in {"by_daily", "by_id"} and (
            "asset_id" not in spec.required_columns or "time" not in spec.required_columns
        ):
            raise DatasetSpecError(f"{spec.source}/{spec.name} non-reference datasets require asset_id and time")
        if spec.update_type not in {"general", "by_daily", "by_id"}:
            raise DatasetSpecError(f"{spec.source}/{spec.name} unsupported update_type: {spec.update_type}")
        if spec.update_type == "by_daily" and not spec.calendar_dataset:
            raise DatasetSpecError(f"{spec.source}/{spec.name} by_daily datasets require calendar_dataset")
        if spec.update_type == "by_id":
            if not spec.id_dataset:
                raise DatasetSpecError(f"{spec.source}/{spec.name} by_id datasets require id_dataset")
            if spec.batch_count < 1:
                raise DatasetSpecError(f"{spec.source}/{spec.name} batch_count must be positive")
        if spec.data_kind not in {"generic", "price", "fundamental", "event", "reference"}:
            raise DatasetSpecError(f"{spec.source}/{spec.name} unsupported data_kind: {spec.data_kind}")

    def remove(self, dataset: str, *, source: str, delete_data: bool = False, confirm: bool = False) -> None:
        if delete_data and not confirm:
            raise DestructiveOperationError("Pass confirm=True to delete canonical data")
        self.metadata.remove_dataset(source, dataset)
        self._specs.pop((source, dataset), None)
        if delete_data:
            shutil.rmtree(self.paths.dataset_root(source, dataset), ignore_errors=True)

    def delete(self, dataset: str, *, source: str, delete_data: bool = False, confirm: bool = False) -> None:
        """Delete a dataset registration, optionally deleting canonical data."""

        self.remove(dataset, source=source, delete_data=delete_data, confirm=confirm)
