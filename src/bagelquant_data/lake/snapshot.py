"""Lake snapshot references."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class SnapshotRef:
    """Reference to a recoverable dataset snapshot."""

    source: str
    dataset: str
    snapshot_id: str
    year: int | None = None
    month: int | None = None
    day: int | None = None
    quarter: int | None = None
    path: Path | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] | None = None
