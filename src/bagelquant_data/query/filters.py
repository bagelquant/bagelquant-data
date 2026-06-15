"""Query filter models."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from bagelquant_data.core.types import DateLike


@dataclass(frozen=True, slots=True)
class QueryFilter:
    """Canonical query filters."""

    start: DateLike | None = None
    end: DateLike | None = None
    assets: Sequence[str] | None = None
    columns: Sequence[str] | None = None
