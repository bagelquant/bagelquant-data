"""Source management API."""

from __future__ import annotations

from typing import Any

from bagelquant_data.core.exceptions import ConfigurationError, SourceNotFoundError
from bagelquant_data.core.registry import FrameworkRegistries
from bagelquant_data.storage.data_meta import DataMetaStore


class SourceManager:
    """Register and configure source adapters."""

    def __init__(
        self, registries: FrameworkRegistries, metadata: DataMetaStore
    ) -> None:
        self.registries = registries
        self.metadata = metadata

    def register(self, source: object) -> None:
        self.metadata.ensure_writable()
        name = getattr(source, "name")
        if callable(name):
            name = name()
        if (
            not isinstance(name, str)
            or not name
            or name in {".", "..", "items"}
            or any(c in name for c in "/\\:")
        ):
            raise ConfigurationError(
                "Source name must be a safe path component and cannot be 'items'"
            )
        saved_options = {
            key: value
            for key, value in self.metadata.source_options(str(name)).items()
            if value != "<redacted>"
        }
        if saved_options and hasattr(source, "configure"):
            source.configure(**saved_options)  # type: ignore[attr-defined]
        self.registries.sources.register(str(name), source)
        self.metadata.upsert_source(
            str(name),
            type(source).__name__,
            configured=bool(saved_options),
        )

    def remove(self, name: str) -> None:
        self.metadata.ensure_writable()
        if self.metadata.list_datasets(name):
            raise ConfigurationError(f"Cannot unregister a nonempty source: {name}")
        if self.metadata._rows(
            "select 1 from category_nodes where kind='raw' and source=?", (name,)
        ):
            raise ConfigurationError(
                f"Cannot unregister source with categories: {name}"
            )
        self.registries.sources._items.pop(name, None)
        self.metadata.remove_source(name)

    def list(self) -> list[dict[str, Any]]:
        return [row for row in self.metadata.list_sources() if row["name"] != "items"]

    def get(self, name: str) -> object:
        try:
            return self.registries.sources.get(name)
        except KeyError as exc:
            raise SourceNotFoundError(f"Source is not registered: {name}") from exc

    def configure(self, name: str, **options: Any) -> None:
        self.metadata.ensure_writable()
        source = self.get(name)
        source.configure(**options)  # type: ignore[attr-defined]
        saved = self.metadata.source_options(name)
        saved.update(options)
        self.metadata.upsert_source(
            name, type(source).__name__, configured=True, options=saved
        )

    def enable(self, name: str) -> None:
        self.metadata.ensure_writable()
        self.metadata.set_source_enabled(name, True)

    def disable(self, name: str) -> None:
        self.metadata.ensure_writable()
        self.metadata.set_source_enabled(name, False)

    def test(self, name: str) -> None:
        self.get(name).test_connection()  # type: ignore[attr-defined]
