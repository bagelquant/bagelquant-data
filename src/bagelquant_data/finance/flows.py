"""Generic flow transformations."""

from __future__ import annotations

import polars as pl


def ytd_to_period(
    data: pl.LazyFrame,
    *,
    value_column: str = "value",
    frequency: str = "quarter",
    output_name: str | None = None,
) -> pl.LazyFrame:
    """Convert cumulative YTD flow values into period values."""

    output = output_name or value_column
    sorted_data = data.sort("asset_id", "period", "time")
    year = pl.col("period").dt.year()
    previous = pl.col(value_column).shift(1).over("asset_id", year)
    period_value = pl.when(previous.is_null()).then(pl.col(value_column)).otherwise(pl.col(value_column) - previous)
    return sorted_data.with_columns(period_value.alias(output)).select(
        "asset_id", "time", "period", output
    )
