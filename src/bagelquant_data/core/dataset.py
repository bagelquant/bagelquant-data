"""Minimal dataset declaration types."""

from __future__ import annotations

from dataclasses import dataclass, field

ASSET_BUCKET_COUNT = 32


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
    asset_list: str | None = None
    primary_key_extra: tuple[str, ...] = ()
    source_api_params: dict[str, object] = field(default_factory=dict)
    source_api_param_sets: tuple[dict[str, object], ...] = ()
    date_param: str | None = None
    request_date_field: str | None = None
    field_mappings: dict[str, str] = field(default_factory=dict)
    asset_bucket_count: int = ASSET_BUCKET_COUNT
    revision_lookback_days: int = 730
    revision_refresh_days: int = 30
    historical_empty_is_error: bool = False
    source_api: str | None = None
    request_discovery: RequestDiscoverySpec | None = None


def dataset_key(spec: DatasetSpec) -> tuple[str, str]:
    """Return the metadata identity for a dataset."""

    return spec.source, spec.name


def incremental_key(spec: DatasetSpec) -> tuple[str, ...] | None:
    """Return the canonical key for an incremental dataset."""

    if spec.update_type == "general":
        return None
    return "time", "asset_id", *spec.primary_key_extra
