"""Generic rolling financial operations."""

from __future__ import annotations

import polars as pl


def trailing(
    data: pl.LazyFrame,
    *,
    value_column: str = "value",
    periods: int,
    operation: str,
    output_name: str | None = None,
    require_complete: bool = True,
) -> pl.LazyFrame:
    """Compute a trailing window over event-period rows."""

    output = output_name or value_column
    expr = {
        "sum": pl.col(value_column).rolling_sum(periods),
        "mean": pl.col(value_column).rolling_mean(periods),
        "min": pl.col(value_column).rolling_min(periods),
        "max": pl.col(value_column).rolling_max(periods),
        "first": pl.col(value_column).rolling_map(lambda values: values[0], window_size=periods),
        "last": pl.col(value_column).rolling_map(lambda values: values[-1], window_size=periods),
    }[operation]
    result = data.sort("asset_id", "period", "time").with_columns(
        expr.over("asset_id").alias(output),
        pl.len().rolling_sum(window_size=periods).over("asset_id").alias("__window_count"),
    )
    if require_complete:
        result = result.filter(pl.col("__window_count") >= periods)
    return result.drop("__window_count").select("asset_id", "time", "period", output)
