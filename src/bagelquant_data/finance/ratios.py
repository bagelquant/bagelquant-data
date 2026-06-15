"""Generic ratio operations."""

from __future__ import annotations

import polars as pl


def ratio(
    numerator: pl.LazyFrame,
    denominator: pl.LazyFrame,
    *,
    numerator_column: str,
    denominator_column: str,
    output_name: str = "value",
    zero_policy: str = "null",
) -> pl.LazyFrame:
    """Join numerator and denominator and compute a generic ratio."""

    keys = ["asset_id", "time"]
    if "period" in numerator.collect_schema().names() and "period" in denominator.collect_schema().names():
        keys.append("period")
    joined = numerator.join(denominator, on=keys, how="inner", suffix="__den")
    denominator_expr = pl.col(denominator_column)
    if zero_policy == "raise":
        zero_count = joined.filter(denominator_expr == 0).select(pl.len()).collect().item()
        if zero_count:
            raise ZeroDivisionError("Ratio denominator contains zero")
    value = pl.when(denominator_expr == 0).then(None if zero_policy == "null" else float("nan")).otherwise(
        pl.col(numerator_column) / denominator_expr
    )
    return joined.with_columns(value.alias(output_name)).select(*keys, output_name)
