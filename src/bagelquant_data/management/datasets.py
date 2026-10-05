"""Minimal dataset registration API."""

from __future__ import annotations

import json
import tomllib
from pathlib import Path
from typing import Any

from bagelquant_data.core.dataset import (
    DatasetSpec,
    RequestDiscoverySpec,
)
from bagelquant_data.core.exceptions import (
    DatasetNotFoundError,
    DatasetSpecError,
)
from bagelquant_data.storage.data_meta import DataMetaStore
from bagelquant_data.storage.paths import LakePaths


class DatasetManager:
    """Register and inspect plain TOML-backed dataset specifications."""

    def __init__(self, metadata: DataMetaStore, paths: LakePaths) -> None:
        self.metadata = metadata
        self.paths = paths

    def register(self, spec: DatasetSpec) -> DatasetSpec:
        self.validate_spec(spec)
        self.metadata.upsert_dataset(spec)
        return spec

    def register_toml(self, path: str | Path) -> DatasetSpec:
        with Path(path).open("rb") as file:
            return self.register(_spec_from_mapping(tomllib.load(file)))

    def register_toml_text(self, text: str) -> DatasetSpec:
        """Register a TOML declaration supplied by a non-filesystem authority."""

        return self.register(_spec_from_mapping(tomllib.loads(text)))

    def get(
        self, dataset: str, *, source: str, include_inactive: bool = False
    ) -> DatasetSpec:
        if include_inactive:
            rows = self.metadata._rows(
                "select * from datasets where source=? and name=?", (source, dataset)
            )
            row = rows[0] if rows else None
        else:
            row = self.metadata.get_dataset(source, dataset)
        if row is None:
            raise DatasetNotFoundError(f"Dataset is not registered: {source}/{dataset}")
        spec = _spec_from_mapping(json.loads(row["spec_json"]), stored=True)
        return spec

    def list(
        self, source: str | None = None, *, include_inactive: bool = False
    ) -> list[dict[str, Any]]:
        if include_inactive:
            return (
                self.metadata._rows("select * from datasets order by source,name")
                if source is None
                else self.metadata._rows(
                    "select * from datasets where source=? order by name", (source,)
                )
            )
        return self.metadata.list_datasets(source)

    def enable(self, dataset: str, *, source: str) -> None:
        self.metadata.set_dataset_enabled(source, dataset, True)

    def disable(self, dataset: str, *, source: str) -> None:
        self.metadata.set_dataset_enabled(source, dataset, False)

    @staticmethod
    def validate_spec(spec: DatasetSpec) -> None:
        for identity in (spec.source, spec.name):
            if (
                not identity
                or identity in {".", ".."}
                or any(c in identity for c in "/\\:")
            ):
                raise DatasetSpecError(
                    "Dataset identities must be nonempty safe path components"
                )
        if spec.update_type not in {"general", "by_date"}:
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
        if (
            spec.update_type == "by_date"
            and spec.date_kind == "trading"
            and not spec.calendar
        ):
            raise DatasetSpecError(
                f"{spec.source}/{spec.name} by_date requires calendar"
            )
        if spec.date_param is not None and spec.update_type != "by_date":
            raise DatasetSpecError(
                f"{spec.source}/{spec.name} date_param is only valid for by_date"
            )
        if spec.update_type == "general" and (
            spec.date_params or spec.request_date_field
        ):
            raise DatasetSpecError(
                "date_params and request_date_field are only valid for by_date"
            )
        if spec.date_param is not None and not spec.date_param:
            raise DatasetSpecError(
                f"{spec.source}/{spec.name} date_param cannot be empty"
            )
        if spec.date_kind not in {"trading", "calendar"}:
            raise DatasetSpecError("date_kind must be trading or calendar")
        if spec.recent_recheck_days < 0:
            raise DatasetSpecError("recent_recheck_days cannot be negative")
        if not isinstance(spec.request_options, dict):
            raise DatasetSpecError("request_options must be a mapping")
        if spec.parameter_dataset and not all(
            (spec.parameter_name, spec.parameter_field)
        ):
            raise DatasetSpecError(
                "parameter fanout requires a field and parameter name"
            )
        from zoneinfo import ZoneInfo

        ZoneInfo(spec.availability_timezone)
        if spec.availability_day_offset < 0 and spec.availability_cutoff_time is None:
            raise DatasetSpecError(
                "negative availability_day_offset requires an explicit cutoff time"
            )
        if spec.availability_cutoff_time is not None:
            from datetime import time

            try:
                cutoff = time.fromisoformat(spec.availability_cutoff_time)
            except (TypeError, ValueError) as error:
                raise DatasetSpecError(
                    "availability_cutoff_time must be local HH:MM:SS"
                ) from error
            if (
                cutoff.tzinfo is not None
                or cutoff.isoformat() != spec.availability_cutoff_time
            ):
                raise DatasetSpecError(
                    "availability_cutoff_time must be local HH:MM:SS"
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
        nullable_keys = set(spec.nullable_primary_key_extra)
        extra_keys = set(spec.primary_key_extra)
        if len(nullable_keys) != len(spec.nullable_primary_key_extra):
            raise DatasetSpecError(
                "nullable_primary_key_extra cannot contain duplicate fields"
            )
        if missing_nullable := sorted(nullable_keys - extra_keys):
            raise DatasetSpecError(
                "nullable_primary_key_extra must be a subset of primary_key_extra: "
                + ", ".join(missing_nullable)
            )
        if spec.update_type != "general":
            missing_targets = sorted({"time", "asset_id"} - set(mappings.values()))
            if missing_targets:
                raise DatasetSpecError(
                    f"{spec.source}/{spec.name} field_mappings must map to: {', '.join(missing_targets)}"
                )

    def remove(self, dataset: str, *, source: str) -> None:
        """Unregister the active declaration while retaining committed evidence."""
        self.metadata.ensure_writable()
        self.metadata.remove_dataset(source, dataset)


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
        "primary_key_extra",
        "nullable_primary_key_extra",
        "source_api_params",
        "source_api_param_sets",
        "request_discovery",
        "field_mappings",
        "date_kind",
        "date_params",
        "source_time_fields",
        "parameter_dataset",
        "parameter_field",
        "parameter_name",
        "recent_recheck_days",
        "request_options",
        "availability_timezone",
        "availability_day_offset",
        "availability_cutoff_time",
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
    nullable_extra = value.get("nullable_primary_key_extra", ())
    if isinstance(nullable_extra, str):
        nullable_extra = (nullable_extra,)
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
        primary_key_extra=tuple(str(field) for field in extra),
        nullable_primary_key_extra=tuple(str(field) for field in nullable_extra),
        source_api_params=dict(source_api_params),
        source_api_param_sets=tuple(
            dict(param_set) for param_set in source_api_param_sets
        ),
        request_discovery=request_discovery,
        field_mappings=field_mappings,
        date_kind=str(value.get("date_kind", "trading")),
        date_params=tuple(value.get("date_params", ())),
        source_time_fields=tuple(value.get("source_time_fields", ())),
        parameter_dataset=value.get("parameter_dataset"),
        parameter_field=str(value.get("parameter_field", "asset_id")),
        parameter_name=str(value.get("parameter_name", "ts_code")),
        recent_recheck_days=int(value.get("recent_recheck_days", 3)),
        request_options=dict(value.get("request_options", {})),
        availability_timezone=str(value.get("availability_timezone", "UTC")),
        availability_day_offset=int(value.get("availability_day_offset", 0)),
        availability_cutoff_time=value.get("availability_cutoff_time"),
    )
