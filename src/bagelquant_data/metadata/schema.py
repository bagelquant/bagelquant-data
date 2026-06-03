"""Dataset schema declarations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class FieldSchema:
    """A single dataset field declaration."""

    name: str
    dtype: str
    nullable: bool = True
    description: str | None = None


@dataclass(frozen=True, slots=True)
class DatasetSchema:
    """A dataset schema independent of physical storage."""

    fields: tuple[FieldSchema, ...] = field(default_factory=tuple)
    primary_key: tuple[str, ...] = field(default_factory=tuple)
    metadata: dict[str, Any] = field(default_factory=dict)

    def field_names(self) -> tuple[str, ...]:
        """Return schema field names in declaration order."""

        return tuple(field.name for field in self.fields)
