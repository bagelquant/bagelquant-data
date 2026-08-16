"""Minimal dataset registration API."""

from __future__ import annotations

import json
import os
import shutil
import tomllib
import uuid
from pathlib import Path
from typing import Any

from bagelquant_data.core.dataset import (
    ASSET_BUCKET_COUNT,
    DatasetSpec,
    RequestDiscoverySpec,
    dataset_key,
)
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
        existing = self.metadata.get_dataset(spec.source, spec.name)
        if existing is not None:
            current = _spec_from_mapping(json.loads(existing["spec_json"]), stored=True)
            if (
                current.update_type == "by_asset"
                and spec.update_type == "by_asset"
                and current.asset_bucket_count != spec.asset_bucket_count
                and self.metadata.manifest(spec.source, spec.name)
            ):
                raise DatasetSpecError(
                    f"{spec.source}/{spec.name} asset_bucket_count cannot change "
                    "while canonical data exists; clear the dataset before registering "
                    "the new partition layout"
                )
        self._specs[dataset_key(spec)] = spec
        self.metadata.upsert_dataset(spec)
        return spec

    def register_toml(self, path: str | Path) -> DatasetSpec:
        with Path(path).open("rb") as file:
            return self.register(_spec_from_mapping(tomllib.load(file)))

    def register_toml_text(self, text: str) -> DatasetSpec:
        """Register a TOML declaration supplied by a non-filesystem authority."""

        return self.register(_spec_from_mapping(tomllib.loads(text)))

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
        if spec.source_api is not None and (
            not isinstance(spec.source_api, str) or not spec.source_api.strip()
        ):
            raise DatasetSpecError("source_api cannot be empty")
        if discovery := spec.request_discovery:
            if not isinstance(discovery.params, dict):
                raise DatasetSpecError("request_discovery.params must be a mapping")
            if not all(
                isinstance(value, str) and value.strip()
                for value in (
                    discovery.api,
                    discovery.result_field,
                    discovery.target_param,
                )
            ):
                raise DatasetSpecError(
                    "request_discovery api, result_field, and target_param cannot be empty"
                )
            reserved = set(spec.source_api_params)
            reserved.update(
                key
                for parameter_set in spec.source_api_param_sets
                for key in parameter_set
            )
            if discovery.target_param in reserved:
                raise DatasetSpecError(
                    "request_discovery.target_param conflicts with target request parameters"
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
        if spec.request_date_field is not None and (
            spec.update_type != "by_asset" or not spec.request_date_field
        ):
            raise DatasetSpecError(
                f"{spec.source}/{spec.name} request_date_field is only valid "
                "for by_asset and cannot be empty"
            )
        if spec.update_type == "by_asset" and not spec.asset_list:
            raise DatasetSpecError(
                f"{spec.source}/{spec.name} by_asset requires asset_list"
            )
        if (
            not isinstance(spec.asset_bucket_count, int)
            or isinstance(spec.asset_bucket_count, bool)
            or spec.asset_bucket_count <= 0
        ):
            raise DatasetSpecError(
                f"{spec.source}/{spec.name} asset_bucket_count must be a positive integer"
            )
        if (
            spec.update_type != "by_asset"
            and spec.asset_bucket_count != ASSET_BUCKET_COUNT
        ):
            raise DatasetSpecError(
                f"{spec.source}/{spec.name} asset_bucket_count is only valid for by_asset"
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

    def clear_dataset_data(
        self, dataset: str, *, source: str, confirm: bool = False
    ) -> dict[str, int]:
        """Delete stored data and current state, retaining the dataset declaration."""

        if not confirm:
            raise DestructiveOperationError("Pass confirm=True to clear dataset data")
        self.get(dataset, source=source)
        roots = [
            (self.paths.lake / source, self.paths.dataset_root(source, dataset)),
            (self.paths.staging / source, self.paths.staging / source / dataset),
            (self.paths.rejected / source, self.paths.rejected / source / dataset),
        ]
        trash = self.paths.tmp / "deletions" / uuid.uuid4().hex
        staged: list[tuple[Path, Path]] = []
        try:
            for index, (base, path) in enumerate(roots):
                if not path.resolve().is_relative_to(base.resolve()):
                    raise DestructiveOperationError(
                        f"Dataset path escapes lake root: {source}/{dataset}"
                    )
                if not path.exists():
                    continue
                target = trash / str(index)
                target.parent.mkdir(parents=True, exist_ok=True)
                os.replace(path, target)
                staged.append((path, target))
            result = self.metadata.clear_dataset_data(source, dataset)
        except Exception:
            for path, target in reversed(staged):
                path.parent.mkdir(parents=True, exist_ok=True)
                if target.exists():
                    os.replace(target, path)
            raise
        result["directories"] = len(staged)
        shutil.rmtree(trash, ignore_errors=True)
        return result


def _spec_from_mapping(value: dict[str, Any], *, stored: bool = False) -> DatasetSpec:
    allowed = {
        "name",
        "update_type",
        "source",
        "description",
        "source_api",
        "calendar",
        "date_param",
        "request_date_field",
        "asset_list",
        "primary_key_extra",
        "source_api_params",
        "source_api_param_sets",
        "request_discovery",
        "field_mappings",
        "asset_bucket_count",
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
    discovery_value = value.get("request_discovery")
    if discovery_value is None:
        request_discovery = None
    elif not isinstance(discovery_value, dict):
        raise DatasetSpecError("request_discovery must be a TOML table")
    else:
        required = {"api", "params", "result_field", "target_param"}
        missing_discovery = sorted(required - set(discovery_value))
        unknown_discovery = sorted(set(discovery_value) - required)
        if missing_discovery or unknown_discovery:
            raise DatasetSpecError(
                "request_discovery fields invalid: "
                f"missing={missing_discovery}, unknown={unknown_discovery}"
            )
        params = discovery_value["params"]
        if not isinstance(params, dict):
            raise DatasetSpecError("request_discovery.params must be a TOML table")
        request_discovery = RequestDiscoverySpec(
            api=str(discovery_value["api"]),
            params=dict(params),
            result_field=str(discovery_value["result_field"]),
            target_param=str(discovery_value["target_param"]),
        )
    asset_bucket_count = value.get("asset_bucket_count", ASSET_BUCKET_COUNT)
    if not isinstance(asset_bucket_count, int) or isinstance(
        asset_bucket_count, bool
    ):
        raise DatasetSpecError("asset_bucket_count must be a positive integer")
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
        description=str(value.get("description", "")),
        source_api=(
            None if value.get("source_api") is None else str(value["source_api"])
        ),
        calendar=None if value.get("calendar") is None else str(value["calendar"]),
        date_param=None
        if value.get("date_param") is None
        else str(value["date_param"]),
        request_date_field=None
        if value.get("request_date_field") is None
        else str(value["request_date_field"]),
        asset_list=None
        if value.get("asset_list") is None
        else str(value["asset_list"]),
        primary_key_extra=tuple(str(field) for field in extra),
        source_api_params=dict(source_api_params),
        source_api_param_sets=tuple(
            dict(param_set) for param_set in source_api_param_sets
        ),
        request_discovery=request_discovery,
        field_mappings=field_mappings,
        asset_bucket_count=asset_bucket_count,
        revision_lookback_days=int(value.get("revision_lookback_days", 730)),
        revision_refresh_days=int(value.get("revision_refresh_days", 30)),
    )
