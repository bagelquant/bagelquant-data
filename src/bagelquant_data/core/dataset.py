"""Minimal dataset declaration types."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class RequestDiscoverySpec:
    """Provider request that yields values used to fan out target requests."""

    api: str
    params: dict[str, object]
    result_field: str
    target_param: str


@dataclass(frozen=True, slots=True)
class DatasetSpec:
    """Identity and update references for one local dataset."""

    name: str
    update_type: str
    source: str = "custom"
    description: str = ""
    calendar: str | None = None
    primary_key_extra: tuple[str, ...] = ()
    nullable_primary_key_extra: tuple[str, ...] = ()
    source_api_params: dict[str, object] = field(default_factory=dict)
    source_api_param_sets: tuple[dict[str, object], ...] = ()
    date_param: str | None = None
    request_date_field: str | None = None
    field_mappings: dict[str, str] = field(default_factory=dict)
    source_api: str | None = None
    request_discovery: RequestDiscoverySpec | None = None
    date_kind: str = "trading"
    date_params: tuple[str, ...] = ()
    source_time_fields: tuple[str, ...] = ()
    parameter_dataset: str | None = None
    parameter_field: str = "asset_id"
    parameter_name: str = "ts_code"
    recent_recheck_days: int = 3
    request_options: dict[str, object] = field(default_factory=dict)
    availability_timezone: str = "UTC"
    availability_day_offset: int = 0


def dataset_key(spec: DatasetSpec) -> tuple[str, str]:
    """Return the metadata identity for a dataset."""

    return spec.source, spec.name


def incremental_key(spec: DatasetSpec) -> tuple[str, ...] | None:
    """Return the canonical key for an incremental dataset."""

    if spec.update_type == "general":
        return None
    return "time", "asset_id", *spec.primary_key_extra


def record_key(spec: DatasetSpec) -> tuple[str, ...]:
    """Stable observation identity, independent of when a revision was learned."""

    return "source_time", "asset_id", *spec.primary_key_extra
