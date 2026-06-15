"""Generic weighted-average support."""

from __future__ import annotations

import polars as pl


def weighted_average(
    data: pl.LazyFrame,
    *,
    value_column: str,
    effective_time_column: str,
    period_start_column: str,
    period_end_column: str,
    output_name: str | None = None,
) -> pl.LazyFrame:
    """Compute a generic time-weighted average inside each asset/period."""

    output = output_name or value_column
    weighted = data.with_columns(
        (
            pl.col(period_end_column).cast(pl.Date).sub(pl.col(period_start_column).cast(pl.Date)).dt.total_days()
        ).alias("__days")
    ).with_columns((pl.col(value_column) * pl.col("__days")).alias("__weighted"))
    return weighted.group_by("asset_id", "period").agg(
        pl.max("time").alias("time"),
        (pl.sum("__weighted") / pl.sum("__days")).alias(output),
    ).select("asset_id", "time", "period", output)
