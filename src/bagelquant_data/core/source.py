"""Source adapter protocol."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

import polars as pl

class DataSource(Protocol):
    """Generic external source adapter."""

    @property
    def name(self) -> str:
        """Source name."""
        ...

    def configure(self, **options: Any) -> None:
        """Configure credentials and runtime options."""
        ...

    def test_connection(self) -> None:
        """Raise when the source cannot be reached."""
        ...

    def fetch(self, dataset: str, request: Mapping[str, Any]) -> pl.DataFrame:
        """Fetch one source response."""
        ...
