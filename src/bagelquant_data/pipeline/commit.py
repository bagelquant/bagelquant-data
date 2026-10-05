"""Canonical immutable version commit result."""

from __future__ import annotations
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CommitResult:
    rows_committed: int
    partitions_rewritten: int
    partitions_skipped: int
    bytes_written: int
    present_times: frozenset[str] = frozenset()
    bytes_read: int = 0
    peak_partition_in_flight: int = 0
