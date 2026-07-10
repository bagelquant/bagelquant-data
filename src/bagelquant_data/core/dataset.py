"""Minimal dataset declaration types."""

from __future__ import annotations

from dataclasses import dataclass


ASSET_BUCKET_COUNT = 32


@dataclass(frozen=True, slots=True)
class DatasetSpec:
    """Identity and update references for one local dataset."""

    name: str
    update_type: str
    source: str = "custom"
    calendar: str | None = None
    asset_list: str | None = None
    primary_key_extra: tuple[str, ...] = ()


def dataset_key(spec: DatasetSpec) -> tuple[str, str]:
    """Return the metadata identity for a dataset."""

    return spec.source, spec.name


def incremental_key(spec: DatasetSpec) -> tuple[str, ...] | None:
    """Return the canonical key for an incremental dataset."""

    if spec.update_type == "general":
        return None
    return "time", "asset_id", *spec.primary_key_extra
