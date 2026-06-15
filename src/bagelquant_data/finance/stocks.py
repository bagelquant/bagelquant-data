"""Generic stock variable transformations."""

from __future__ import annotations

import polars as pl


def average_stock(
    data: pl.LazyFrame,
    *,
    value_column: str = "value",
    periods: int = 4,
    method: str = "endpoint",
    output_name: str | None = None,
) -> pl.LazyFrame:
    """Average stock variables such as assets, equity, inventory, or shares."""

    output = output_name or value_column
    sorted_data = data.sort("asset_id", "period", "time")
    if method == "endpoint":
        lagged = pl.col(value_column).shift(periods).over("asset_id")
        expr = ((pl.col(value_column) + lagged) / 2).alias(output)
    elif method == "period_mean":
        expr = pl.col(value_column).rolling_mean(periods).over("asset_id").alias(output)
    else:
        raise ValueError(f"Unsupported average_stock method: {method}")
    return sorted_data.with_columns(expr).select("asset_id", "time", "period", output)
