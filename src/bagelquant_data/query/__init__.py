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


class LakeQuery:
    """Point-in-time aware canonical query API."""

    def __init__(self, raw_service: RawQueryService, finance: Any | None = None) -> None:
        self._raw = raw_service
        self._field = FieldQueryService(raw_service)
        self._reference = ReferenceQueryService(raw_service)
        self._records = RecordsQueryService(raw_service)
        self.fundamentals = finance

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

    def panel(
        self,
        dataset: str,
        field: str,
        **kwargs: Any,
    ) -> pl.LazyFrame | pl.DataFrame:
        """Return one field as a long `time | asset_id | value` panel."""

        kwargs.setdefault("value_name", "value")
        return self.field(dataset, field, **kwargs)

    def price(self, dataset: str, field: str, **kwargs: Any) -> pl.LazyFrame | pl.DataFrame:
        """Return price-like panel data using the same contract as `field`."""

        return self.panel(dataset, field, **kwargs)

    def fundamental(self, dataset: str, field: str, **kwargs: Any) -> pl.LazyFrame | pl.DataFrame:
        """Return point-in-time fundamental events or latest aligned values.

        Pass `observations=...` to align with the generic finance `latest` API.
        Without observations this returns event-level `asset_id | time | period | value`.
        """

        observations = kwargs.pop("observations", None)
        if self.fundamentals is None:
            raise RuntimeError("Fundamental query support is not configured")
        if observations is not None:
            return self.fundamentals.latest(dataset, field, observations=observations, **kwargs)
        return self.fundamentals.field(dataset, field, **kwargs)

    def events(
        self,
        dataset: str,
        *,
        source: str,
        start: DateLike | None = None,
        end: DateLike | None = None,
        assets: Sequence[str] | None = None,
        event_type: str | Sequence[str] | None = None,
        collect: bool = False,
    ) -> pl.LazyFrame | pl.DataFrame:
        """Query append-only/event-style canonical records."""

        lf = self.raw(dataset, source=source, start=start, end=end, assets=assets)
        if event_type is not None and "event_type" in lf.collect_schema().names():
            values = [event_type] if isinstance(event_type, str) else list(event_type)
            lf = lf.filter(pl.col("event_type").is_in(values))
        return lf.collect() if collect else lf

    def reference(self, dataset: str, *, source: str, collect: bool = False) -> pl.LazyFrame | pl.DataFrame:
        return self._reference.reference(dataset, source=source, collect=collect)

    def records(self, dataset: str, *, source: str, limit: int = 100) -> pl.DataFrame:
        return self._records.records(dataset, source=source, limit=limit)

    def observations(self, **kwargs: Any) -> pl.LazyFrame:
        return build_observations(**kwargs)


QueryFacade = LakeQuery
