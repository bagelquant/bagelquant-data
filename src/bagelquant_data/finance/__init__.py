"""Generic financial transformation API."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import polars as pl

from bagelquant_data.core.types import DateLike
from bagelquant_data.finance.fields import FinancialFieldKind, FinancialFieldSpec
from bagelquant_data.finance.flows import ytd_to_period
from bagelquant_data.finance.point_in_time import asof
from bagelquant_data.finance.ratios import ratio
from bagelquant_data.finance.rolling import trailing
from bagelquant_data.finance.shares import weighted_average
from bagelquant_data.finance.stocks import average_stock
from bagelquant_data.query.raw import RawQueryService


class FinanceFacade:
    """User-facing generic finance API."""

    def __init__(self, raw_service: RawQueryService) -> None:
        self._raw = raw_service

    def field(
        self,
        dataset: str,
        field: str,
        *,
        source: str,
        start: DateLike | None = None,
        end: DateLike | None = None,
        assets: Sequence[str] | None = None,
        value_name: str = "value",
    ) -> pl.LazyFrame:
        return self._raw.raw(
            dataset,
            source=source,
            start=start,
            end=end,
            assets=assets,
            columns=("asset_id", "time", "period", field),
        ).select("asset_id", "time", "period", pl.col(field).alias(value_name))

    def asof(self, data: pl.LazyFrame, observations: pl.LazyFrame, **kwargs: Any) -> pl.LazyFrame | pl.DataFrame:
        return asof(data, observations, **kwargs)

    def latest(
        self,
        dataset: str,
        field: str,
        *,
        source: str,
        observations: pl.LazyFrame,
        value_name: str | None = None,
        collect: bool = False,
    ) -> pl.LazyFrame | pl.DataFrame:
        events = self.field(dataset, field, source=source, value_name=value_name or field)
        return asof(events, observations, value_column=value_name or field, output_name=value_name or field, collect=collect)

    ytd_to_period = staticmethod(ytd_to_period)
    trailing = staticmethod(trailing)
    average_stock = staticmethod(average_stock)
    weighted_average = staticmethod(weighted_average)
    ratio = staticmethod(ratio)


__all__ = [
    "FinanceFacade",
    "FinancialFieldKind",
    "FinancialFieldSpec",
    "asof",
    "average_stock",
    "ratio",
    "trailing",
    "weighted_average",
    "ytd_to_period",
]
