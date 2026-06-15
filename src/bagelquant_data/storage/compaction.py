"""Compaction placeholders for V1 APIs."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CompactionReport:
    """Summary of a compaction operation."""

    source: str
    dataset: str
    partitions: int
    rows: int
