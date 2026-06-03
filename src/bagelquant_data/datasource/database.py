"""Database data source extension point."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from bagelquant_data.datasource.base import DataRequest
from bagelquant_data.utils.exceptions import DataSourceError


class DatabaseDataSource:
    """Placeholder interface for future database-backed sources."""

    name = "database"

    def read(self, request: DataRequest) -> Any:
        """Database reads are not implemented in V1."""

        raise DataSourceError("DatabaseDataSource is an interface placeholder in V1")

    def exists(self, dataset: str) -> bool:
        """Return false for the placeholder implementation."""

        return False

    def describe(self, dataset: str) -> Mapping[str, Any]:
        """Return placeholder metadata."""

        return {"provider": self.name, "dataset": dataset, "implemented": False}
