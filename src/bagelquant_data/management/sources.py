"""Source management API."""

from __future__ import annotations

from typing import Any

from bagelquant_data.core.exceptions import SourceNotFoundError
from bagelquant_data.core.registry import FrameworkRegistries
from bagelquant_data.storage.metadata import MetadataStore


class SourceManager:
    """Register and configure source adapters."""

    def __init__(self, registries: FrameworkRegistries, metadata: MetadataStore) -> None:
        self.registries = registries
        self.metadata = metadata

    def register(self, source: object) -> None:
        name = getattr(source, "name")
        if callable(name):
            name = name()
        saved_options = self.metadata.source_options(str(name))
        if saved_options and hasattr(source, "configure"):
            source.configure(**saved_options)  # type: ignore[attr-defined]
        self.registries.sources.register(str(name), source)
        self.metadata.upsert_source(
            str(name),
            type(source).__name__,
            configured=bool(saved_options),
        )

    def add(self, source: object) -> None:
        """Register a source adapter."""

        self.register(source)

    def remove(self, name: str) -> None:
        self.registries.sources._items.pop(name, None)
        self.metadata.remove_source(name)

    def delete(self, name: str) -> None:
        """Delete a source registration."""

        self.remove(name)

    def list(self) -> list[dict[str, Any]]:
        return self.metadata.list_sources()

    def get(self, name: str) -> object:
        try:
            return self.registries.sources.get(name)
        except KeyError as exc:
            raise SourceNotFoundError(f"Source is not registered: {name}") from exc

    def configure(self, name: str, **options: Any) -> None:
        source = self.get(name)
        source.configure(**options)  # type: ignore[attr-defined]
        saved = self.metadata.source_options(name)
        saved.update(options)
        self.metadata.upsert_source(name, type(source).__name__, configured=True, options=saved)

    def edit(self, name: str, **options: Any) -> None:
        """Edit persisted source configuration."""

        self.configure(name, **options)

    def enable(self, name: str) -> None:
        self.metadata.set_source_enabled(name, True)

    def disable(self, name: str) -> None:
        self.metadata.set_source_enabled(name, False)

    def configure_tushare(self, token: str) -> None:
        self.configure("tushare", token=token)

    def test(self, name: str) -> None:
        self.get(name).test_connection()  # type: ignore[attr-defined]
