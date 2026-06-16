"""Plugin registries."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Generic, TypeVar

from bagelquant_data.core.deduplication import (
    ExactRecordHashDeduplication,
    NoDeduplication,
    PrimaryKeyLastDeduplication,
)
from bagelquant_data.core.normalization import StandardNormalizer
from bagelquant_data.core.partitioning import (
    SingleFilePartition,
    YearBucketPartition,
    YearMonthPartition,
)
from bagelquant_data.core.validation import FrameworkValidator

T = TypeVar("T")


@dataclass
class Registry(Generic[T]):
    """Named object registry."""

    _items: dict[str, T] = field(default_factory=dict)

    def register(self, name: str, value: T) -> None:
        self._items[name] = value

    def get(self, name: str) -> T:
        try:
            return self._items[name]
        except KeyError as exc:
            raise KeyError(f"Unknown registry item: {name}") from exc

    def list(self) -> tuple[str, ...]:
        return tuple(sorted(self._items))


@dataclass
class FrameworkRegistries:
    """All extension registries used by the framework."""

    sources: Registry[object] = field(default_factory=lambda: Registry[object]())
    normalizers: Registry[object] = field(default_factory=lambda: Registry[object]())
    validators: Registry[object] = field(default_factory=lambda: Registry[object]())
    partition_strategies: Registry[object] = field(default_factory=lambda: Registry[object]())
    deduplication_strategies: Registry[object] = field(default_factory=lambda: Registry[object]())
    financial_fields: Registry[object] = field(default_factory=lambda: Registry[object]())


def default_registries() -> FrameworkRegistries:
    """Return registries with built-in plugins installed."""

    registries = FrameworkRegistries()
    registries.normalizers.register("standard", StandardNormalizer())
    registries.validators.register("framework", FrameworkValidator())
    registries.partition_strategies.register("single_file", SingleFilePartition())
    registries.partition_strategies.register("year_month", YearMonthPartition())
    registries.partition_strategies.register("year_bucket", YearBucketPartition())
    registries.deduplication_strategies.register("none", NoDeduplication())
    registries.deduplication_strategies.register("exact_record_hash", ExactRecordHashDeduplication())
    registries.deduplication_strategies.register("primary_key_last", PrimaryKeyLastDeduplication())
    return registries
