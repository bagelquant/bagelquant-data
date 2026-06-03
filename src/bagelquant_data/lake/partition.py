"""Lake partition helpers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class PartitionSpec:
    """Logical partition declaration."""

    keys: tuple[str, ...] = field(default_factory=tuple)

    def path(self, values: Mapping[str, Any]) -> str:
        """Build a hive-style partition path."""

        parts = []
        for key in self.keys:
            if key not in values:
                raise KeyError(f"Missing partition value: {key}")
            parts.append(f"{key}={values[key]}")
        return "/".join(parts)
