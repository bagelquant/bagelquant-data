"""Public query facade."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import polars as pl

from bagelquant_data.core.types import DateLike
from bagelquant_data.query.field import FieldQueryService, ResolveRule
from bagelquant_data.query.observations import observations as build_observations
from bagelquant_data.query.raw import RawQueryService
from bagelquant_data.query.records import RecordsQueryService
from bagelquant_data.query.reference import ReferenceQueryService


class QueryFacade:
    """User-facing query API."""

    def __init__(self, raw_service: RawQueryService) -> None:
        self._raw = raw_service
        self._field = FieldQueryService(raw_service)
        self._reference = ReferenceQueryService(raw_service)
        self._records = RecordsQueryService(raw_service)

    def raw(self, dataset: str, **kwargs: Any) -> pl.LazyFrame:
        return self._raw.raw(dataset, **kwargs)

    def field(
        self,
        dataset: str,
        field: str,
        *,
        source: str,
        start: DateLike | None = None,
        end: DateLike | None = None,
        assets: Sequence[str] | None = None,
        resolve: ResolveRule | None = None,
        value_name: str | None = None,
        collect: bool = False,
    ) -> pl.LazyFrame | pl.DataFrame:
        return self._field.field(
            dataset,
            field,
            source=source,
            start=start,
            end=end,
            assets=assets,
            resolve=resolve,
            value_name=value_name,
            collect=collect,
        )

    def fields(self, dataset: str, fields: Sequence[str], **kwargs: Any) -> dict[str, pl.LazyFrame | pl.DataFrame]:
        return self._field.fields(dataset, fields, **kwargs)

    def reference(self, dataset: str, *, source: str, collect: bool = False) -> pl.LazyFrame | pl.DataFrame:
        return self._reference.reference(dataset, source=source, collect=collect)

    def records(self, dataset: str, *, source: str, limit: int = 100) -> pl.DataFrame:
        return self._records.records(dataset, source=source, limit=limit)

    def observations(self, **kwargs: Any) -> pl.LazyFrame:
        return build_observations(**kwargs)
