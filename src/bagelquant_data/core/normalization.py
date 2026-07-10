"""Canonical normalization."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import polars as pl

from bagelquant_data.core.dataset import DatasetSpec


@dataclass(frozen=True, slots=True)
class NormalizeContext:
    """Normalization context."""

    source: str
    dataset: str
    run_id: str | None = None


@dataclass(frozen=True, slots=True)
class NormalizeResult:
    """Accepted and rejected normalized records."""

    accepted: pl.LazyFrame
    rejected: pl.LazyFrame


class Normalizer(Protocol):
    """Dataset normalizer protocol."""

    def normalize(
        self, frame: pl.LazyFrame, spec: DatasetSpec, context: NormalizeContext
    ) -> NormalizeResult:
        """Normalize source rows."""
        ...


class StandardNormalizer:
    """Map configured source fields into canonical columns."""

    def normalize(
        self, frame: pl.LazyFrame, spec: DatasetSpec, context: NormalizeContext
    ) -> NormalizeResult:
        lf = frame.rename(spec.field_mapping)
        names = lf.collect_schema().names()
        expressions: list[pl.Expr] = [
            pl.lit(context.source).alias("source"),
            pl.lit(spec.source_dataset).alias("source_dataset"),
        ]
        asset_column = spec.asset_column or _first_present(names, ("asset_id", "ts_code", "symbol", "code", "ticker"))
        time_column = spec.time_column or _first_present(names, ("time", "date", "trade_date", "ann_date", "cal_date"))
        if asset_column and asset_column in names and asset_column != "asset_id":
            expressions.append(pl.col(asset_column).cast(pl.String).alias("asset_id"))
        if time_column and time_column in names and time_column != "time":
            expressions.append(_date_expr(time_column).alias("time"))
        if spec.period_column and spec.period_column in names:
            expressions.append(_date_expr(spec.period_column).alias("period"))
        accepted = lf.with_columns(expressions)
        rejected = accepted.filter(pl.lit(False))
        return NormalizeResult(accepted=accepted, rejected=rejected)


def _date_expr(column: str) -> pl.Expr:
    return (
        pl.when(pl.col(column).cast(pl.String).str.len_chars() == 8)
        .then(pl.col(column).cast(pl.String).str.strptime(pl.Date, "%Y%m%d", strict=False))
        .otherwise(pl.col(column).cast(pl.Date, strict=False))
    )


def _first_present(names: list[str], candidates: tuple[str, ...]) -> str | None:
    for candidate in candidates:
        if candidate in names:
            return candidate
    return None
