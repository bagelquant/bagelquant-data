"""SQLite-backed lock model placeholder."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class PartitionLock:
    """A canonical partition write lock."""

    source: str
    dataset: str
    partition_path: str
    owner: str
    acquired_at: datetime
    expires_at: datetime
