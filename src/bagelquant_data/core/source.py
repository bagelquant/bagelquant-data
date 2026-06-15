"""Source adapter protocol."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Protocol

import polars as pl

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.request import RequestContext


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

    def fetch(self, source_dataset: str, request: Mapping[str, Any]) -> pl.DataFrame:
        """Fetch one source response."""
        ...

    def plan_requests(
        self, dataset: DatasetSpec, context: RequestContext
    ) -> Iterable[Mapping[str, Any]]:
        """Plan source requests for a dataset."""
        ...
