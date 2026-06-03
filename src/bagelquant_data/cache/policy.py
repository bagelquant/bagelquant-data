"""Cache policies."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CachePolicy:
    """Simple cache policy."""

    enabled: bool = True
    ttl_seconds: int | None = None
