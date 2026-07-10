"""Plugin registries."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Generic, TypeVar

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
    validators: Registry[object] = field(default_factory=lambda: Registry[object]())


def default_registries() -> FrameworkRegistries:
    """Return registries with built-in plugins installed."""

    registries = FrameworkRegistries()
    registries.validators.register("framework", FrameworkValidator())
    return registries
