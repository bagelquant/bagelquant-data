"""Minimal dataset registration API."""

from __future__ import annotations

import json
import shutil
import tomllib
from pathlib import Path
from typing import Any

from bagelquant_data.core.dataset import DatasetSpec, dataset_key
from bagelquant_data.core.exceptions import (
    DatasetNotFoundError,
    DatasetSpecError,
    DestructiveOperationError,
)
from bagelquant_data.storage.metadata import MetadataStore
from bagelquant_data.storage.paths import LakePaths


class DatasetManager:
    """Register and inspect plain TOML-backed dataset specifications."""

    def __init__(self, metadata: MetadataStore, paths: LakePaths) -> None:
        self.metadata = metadata
        self.paths = paths
        self._specs: dict[tuple[str, str], DatasetSpec] = {}

    def register(self, spec: DatasetSpec) -> DatasetSpec:
        self.validate_spec(spec)
        self._specs[dataset_key(spec)] = spec
        self.metadata.upsert_dataset(spec)
        return spec

    def register_toml(self, path: str | Path) -> DatasetSpec:
        with Path(path).open("rb") as file:
            return self.register(_spec_from_mapping(tomllib.load(file)))

    def get(self, dataset: str, *, source: str) -> DatasetSpec:
        key = (source, dataset)
        if key in self._specs:
            return self._specs[key]
        row = self.metadata.get_dataset(source, dataset)
        if row is None:
            raise DatasetNotFoundError(f"Dataset is not registered: {source}/{dataset}")
        spec = _spec_from_mapping(json.loads(row["spec_json"]), stored=True)
        self._specs[key] = spec
        return spec

    def list(self, source: str | None = None) -> list[dict[str, Any]]:
        return self.metadata.list_datasets(source)

    def enable(self, dataset: str, *, source: str) -> None:
        self.metadata.set_dataset_enabled(source, dataset, True)

    def disable(self, dataset: str, *, source: str) -> None:
        self.metadata.set_dataset_enabled(source, dataset, False)

    def validate_spec(self, spec: DatasetSpec) -> None:
        if spec.update_type not in {"general", "by_daily", "by_asset"}:
            raise DatasetSpecError(
                f"{spec.source}/{spec.name} has unsupported update_type: {spec.update_type}"
            )
        if spec.update_type == "by_daily" and not spec.calendar:
            raise DatasetSpecError(
                f"{spec.source}/{spec.name} by_daily requires calendar"
            )
        if spec.date_param is not None and spec.update_type != "by_daily":
            raise DatasetSpecError(
                f"{spec.source}/{spec.name} date_param is only valid for by_daily"
            )
        if spec.date_param is not None and not spec.date_param:
            raise DatasetSpecError(
                f"{spec.source}/{spec.name} date_param cannot be empty"
            )
        if spec.update_type == "by_asset" and not spec.asset_list:
            raise DatasetSpecError(
                f"{spec.source}/{spec.name} by_asset requires asset_list"
            )
        if spec.update_type != "by_asset" and (
            spec.revision_lookback_days != 730 or spec.revision_refresh_days != 30
        ):
            raise DatasetSpecError(
                f"{spec.source}/{spec.name} revision settings are only valid for by_asset"
            )
        if spec.revision_lookback_days <= 0 or spec.revision_refresh_days <= 0:
            raise DatasetSpecError(
                f"{spec.source}/{spec.name} revision settings must be positive"
            )
        mappings = spec.field_mappings
        if not isinstance(mappings, dict) or not all(
            isinstance(source, str) and source and isinstance(target, str) and target
            for source, target in mappings.items()
        ):
            raise DatasetSpecError(
                f"{spec.source}/{spec.name} field_mappings must map non-empty strings"
            )
        if len(set(mappings.values())) != len(mappings):
            raise DatasetSpecError(
                f"{spec.source}/{spec.name} field_mappings cannot reuse destinations"
            )
        if spec.update_type != "general":
            missing_targets = sorted({"time", "asset_id"} - set(mappings.values()))
            if missing_targets:
                raise DatasetSpecError(
                    f"{spec.source}/{spec.name} field_mappings must map to: {', '.join(missing_targets)}"
                )

    def remove(
        self,
        dataset: str,
        *,
        source: str,
        delete_data: bool = False,
        confirm: bool = False,
    ) -> None:
        if delete_data and not confirm:
            raise DestructiveOperationError(
                "Pass confirm=True to delete canonical data"
            )
        self.metadata.remove_dataset(source, dataset)
        self._specs.pop((source, dataset), None)
        if delete_data:
            shutil.rmtree(self.paths.dataset_root(source, dataset), ignore_errors=True)


def _spec_from_mapping(value: dict[str, Any], *, stored: bool = False) -> DatasetSpec:
    allowed = {
        "name",
        "update_type",
        "source",
        "calendar",
        "date_param",
        "asset_list",
        "primary_key_extra",
        "source_api_params",
        "source_api_param_sets",
        "field_mappings",
        "revision_lookback_days",
        "revision_refresh_days",
    }
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise DatasetSpecError(f"Unsupported dataset fields: {', '.join(unknown)}")
    missing = [field for field in ("name", "update_type") if field not in value]
    if missing:
        raise DatasetSpecError(
            f"Dataset declaration is missing fields: {', '.join(missing)}"
        )
    extra = value.get("primary_key_extra", ())
    if isinstance(extra, str):
        extra = (extra,)
    source_api_params = value.get("source_api_params", {})
    if not isinstance(source_api_params, dict):
        raise DatasetSpecError("source_api_params must be a TOML table")
    source_api_param_sets = value.get("source_api_param_sets")
    if source_api_param_sets is None:
        source_api_param_sets = ()
    elif stored and source_api_param_sets == []:
        # JSON serializes an empty tuple as a list. Treat that persisted value
        # as the same optional no-fan-out setting used by DatasetSpec.
        source_api_param_sets = ()
    elif not isinstance(source_api_param_sets, list) or not source_api_param_sets:
        raise DatasetSpecError(
            "source_api_param_sets must be a non-empty array of TOML tables"
        )
    if source_api_param_sets:
        if not all(isinstance(param_set, dict) for param_set in source_api_param_sets):
            raise DatasetSpecError(
                "source_api_param_sets must contain only TOML tables"
            )
        if any(
            isinstance(value, list) and not value
            for param_set in source_api_param_sets
            for value in param_set.values()
        ):
            raise DatasetSpecError("source_api_param_sets cannot contain empty lists")
    field_mapping_tables = value.get("field_mappings")
    if field_mapping_tables is None:
        field_mappings: dict[str, str] = {}
    elif isinstance(field_mapping_tables, dict):
        # TOML declarations use one [field_mappings] table, which persists as
        # the same mapping in metadata.
        field_mappings = dict(field_mapping_tables)
    else:
        raise DatasetSpecError("field_mappings must be a TOML table")
    return DatasetSpec(
        name=str(value["name"]),
        update_type=str(value["update_type"]),
        source=str(value.get("source", "custom")),
        calendar=None if value.get("calendar") is None else str(value["calendar"]),
        date_param=None
        if value.get("date_param") is None
        else str(value["date_param"]),
        asset_list=None
        if value.get("asset_list") is None
        else str(value["asset_list"]),
        primary_key_extra=tuple(str(field) for field in extra),
        source_api_params=dict(source_api_params),
        source_api_param_sets=tuple(
            dict(param_set) for param_set in source_api_param_sets
        ),
        field_mappings=field_mappings,
        revision_lookback_days=int(value.get("revision_lookback_days", 730)),
        revision_refresh_days=int(value.get("revision_refresh_days", 30)),
    )
