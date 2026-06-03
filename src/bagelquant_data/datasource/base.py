"""Data source abstractions."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol

import pandas as pd


@dataclass(frozen=True, slots=True)
class DataRequest:
    """Provider-neutral data request."""

    dataset: str
    fields: tuple[str, ...] = ()
    filters: Mapping[str, Any] = field(default_factory=dict)
    start_date: Any | None = None
    end_date: Any | None = None
    version: str | None = None
    snapshot: str | None = None
    options: Mapping[str, Any] = field(default_factory=dict)


class DataSource(Protocol):
    """Abstract provider interface."""

    name: str

    def read(self, request: DataRequest) -> pd.DataFrame:
        """Read provider data for a request."""
        raise NotImplementedError

    def exists(self, dataset: str) -> bool:
        """Return whether a dataset is supported."""
        raise NotImplementedError

    def describe(self, dataset: str) -> Mapping[str, Any]:
        """Return provider metadata for a dataset."""
        raise NotImplementedError
