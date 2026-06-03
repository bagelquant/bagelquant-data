"""In-memory cache."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class MemoryCache:
    """Small in-memory cache for local workflows."""

    _values: dict[str, Any] = field(default_factory=dict)

    def get(self, key: str) -> Any | None:
        """Return a cached value."""

        return self._values.get(key)

    def set(self, key: str, value: Any) -> None:
        """Store a cached value."""

        self._values[key] = value
