"""Caller-selected local execution limits; hardware policy belongs to callers."""

from __future__ import annotations

from dataclasses import dataclass
import math


@dataclass(frozen=True, slots=True)
class ExecutionOptions:
    """Bound Data workers and admitted buffers, not total process memory."""

    workers: int = 1
    max_in_flight: int | None = None
    batch_size: int | None = None
    max_buffer_bytes: int = 64 * 1024 * 1024
    commit_batch_rows: int | None = None
    commit_interval_seconds: float = 30.0

    def __post_init__(self) -> None:
        for name in ("workers", "max_in_flight", "batch_size", "max_buffer_bytes", "commit_batch_rows"):
            value = getattr(self, name)
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 1
            ):
                raise ValueError(f"{name} must be a positive integer")
        if (isinstance(self.commit_interval_seconds, bool)
                or not isinstance(self.commit_interval_seconds, (int, float))
                or not math.isfinite(self.commit_interval_seconds)
                or self.commit_interval_seconds <= 0):
            raise ValueError("commit_interval_seconds must be positive and finite")

    def update_options(self) -> dict[str, int | float]:
        result = {
            "workers": self.workers,
            "max_in_flight": self.max_in_flight or self.workers,
            "max_buffer_bytes": self.max_buffer_bytes,
            "commit_interval_seconds": self.commit_interval_seconds,
        }
        if self.batch_size is not None:
            result["batch_size"] = self.batch_size
        if self.commit_batch_rows is not None:
            result["commit_batch_rows"] = self.commit_batch_rows
        return result
