"""Data contracts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

import pandas as pd

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
    universe: Sequence[Any] | pd.DataFrame,
) -> tuple[Any, ...] | pd.DataFrame:
    """Normalize static universes while preserving dynamic membership frames."""

    if isinstance(universe, pd.DataFrame):
        return universe.copy(deep=True)
    if isinstance(universe, (str, bytes)):
        raise ContractValidationError("universe must be a sequence, not a string")
    return tuple(universe)
