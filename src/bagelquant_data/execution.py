"""Caller-selected local execution limits; hardware policy belongs to callers."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ExecutionOptions:
    """Bound Data workers and admitted buffers, not total process memory."""

    workers: int = 1
    max_in_flight: int | None = None
    batch_size: int | None = None
    max_buffer_bytes: int = 64 * 1024 * 1024

    def __post_init__(self) -> None:
        for name in ("workers", "max_in_flight", "batch_size", "max_buffer_bytes"):
            value = getattr(self, name)
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 1
            ):
                raise ValueError(f"{name} must be a positive integer")

    def update_options(self) -> dict[str, int]:
        result = {
            "workers": self.workers,
            "max_in_flight": self.max_in_flight or self.workers,
            "max_buffer_bytes": self.max_buffer_bytes,
        }
        if self.batch_size is not None:
            result["batch_size"] = self.batch_size
        return result
