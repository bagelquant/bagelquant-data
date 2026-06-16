"""Request planning models."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from bagelquant_data.core.types import DateLike


@dataclass(frozen=True, slots=True)
class RequestContext:
    """Context passed to request planners and source adapters."""

    source: str
    dataset: str
    start: DateLike | None = None
    end: DateLike | None = None
    assets: Sequence[str] | None = None
    options: Mapping[str, Any] = field(default_factory=dict)
