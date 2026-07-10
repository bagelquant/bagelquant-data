"""Canonical normalization."""

from __future__ import annotations

from dataclasses import dataclass
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


class StandardNormalizer:
    """Infer canonical fields from common provider field names."""

    def normalize(
        self, frame: pl.LazyFrame, spec: DatasetSpec, context: NormalizeContext
    ) -> NormalizeResult:
        lf = frame
        names = lf.collect_schema().names()
        expressions: list[pl.Expr] = [
            pl.lit(context.source).alias("source"),
        ]
        asset_field = _first_present(names, ("asset_id", "ts_code", "symbol", "code", "ticker"))
        time_field = _first_present(names, ("time", "date", "trade_date", "ann_date", "cal_date"))
        if asset_field and asset_field in names and asset_field != "asset_id":
            expressions.append(pl.col(asset_field).cast(pl.String).alias("asset_id"))
        if time_field and time_field in names and time_field != "time":
            expressions.append(_date_expr(time_field).alias("time"))
        period_field = _first_present(names, ("period", "end_date", "f_ann_date"))
        if period_field and period_field in names and period_field != "period":
            expressions.append(_date_expr(period_field).alias("period"))
        accepted = lf.with_columns(expressions)
        rejected = accepted.filter(pl.lit(False))
        return NormalizeResult(accepted=accepted, rejected=rejected)


def _date_expr(field: str) -> pl.Expr:
    return (
        pl.when(pl.col(field).cast(pl.String).str.len_chars() == 8)
        .then(pl.col(field).cast(pl.String).str.strptime(pl.Date, "%Y%m%d", strict=False))
        .otherwise(pl.col(field).cast(pl.Date, strict=False))
    )


def _first_present(names: list[str], candidates: tuple[str, ...]) -> str | None:
    for candidate in candidates:
        if candidate in names:
            return candidate
    return None
