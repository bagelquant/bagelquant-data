"""Distributed cache extension point."""

from typing import Any


class DistributedCache:
    """Placeholder for future distributed cache backends."""

    def get(self, key: str) -> Any | None:
        """Return a cached value if available."""

        return None

    def set(self, key: str, value: Any) -> None:
        """Store a value in a future backend."""

        raise NotImplementedError("DistributedCache is an interface placeholder in V1")
