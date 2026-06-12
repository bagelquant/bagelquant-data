"""Data contracts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

import polars as pl

from bagelquant_data.metadata.schema import DatasetSchema
from bagelquant_data.utils.exceptions import ContractValidationError

PanelKind = Literal["numeric_panel", "category_panel"]


@dataclass(frozen=True, slots=True)
class DatasetIdentity:
    """Stable identity for a logical dataset."""

    name: str
    provider: str | None = None
    version: str | None = None
    snapshot: str | None = None


@dataclass(frozen=True, slots=True)
class DataContract:
    """A reproducible contract for a dataset."""

    identity: DatasetIdentity
    schema: DatasetSchema | None = None
    owner: str | None = None
    freshness: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


def normalize_universe(
    universe: Sequence[Any] | pl.DataFrame,
) -> tuple[Any, ...] | pl.DataFrame:
    """Normalize static universes while preserving dynamic membership frames."""

    if isinstance(universe, pl.DataFrame):
        required = {"time", "asset_id", "active"}
        missing = required - set(universe.columns)
        if missing:
            raise ContractValidationError(
                f"dynamic universe missing columns: {sorted(missing)}"
            )
        return universe.clone()
    if isinstance(universe, (str, bytes)):
        raise ContractValidationError("universe must be a sequence, not a string")
    return tuple(universe)
