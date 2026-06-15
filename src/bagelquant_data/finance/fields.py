"""Financial field metadata."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class FinancialFieldKind(str, Enum):
    """Generic financial field semantics."""

    FLOW_YTD = "flow_ytd"
    FLOW_PERIOD = "flow_period"
    STOCK = "stock"
    PER_SHARE = "per_share"
    RATIO = "ratio"
    COUNT = "count"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class FinancialFieldSpec:
    """Financial field metadata separate from storage."""

    source: str
    dataset: str
    field: str
    kind: FinancialFieldKind
    unit: str | None = None
    currency: str | None = None
