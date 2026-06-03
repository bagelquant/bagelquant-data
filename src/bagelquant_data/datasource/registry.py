"""Data source registry."""

from __future__ import annotations

from dataclasses import dataclass, field

from bagelquant_data.datasource.base import DataSource
from bagelquant_data.utils.exceptions import DatasetNotFoundError


@dataclass(slots=True)
class DataSourceRegistry:
    """Register and resolve named data sources."""

    _sources: dict[str, DataSource] = field(default_factory=dict)

    def register(self, source: DataSource, *, replace: bool = False) -> None:
        """Register a source by its public name."""

        if source.name in self._sources and not replace:
            raise ValueError(f"Data source already registered: {source.name}")
        self._sources[source.name] = source

    def resolve(self, name: str) -> DataSource:
        """Resolve a source by name."""

        try:
            return self._sources[name]
        except KeyError as exc:
            raise DatasetNotFoundError(f"Unknown data source: {name}") from exc

    def names(self) -> tuple[str, ...]:
        """Return registered source names."""

        return tuple(sorted(self._sources))


default_registry = DataSourceRegistry()
