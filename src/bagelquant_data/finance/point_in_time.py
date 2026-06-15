"""Point-in-time alignment."""

from __future__ import annotations

import polars as pl


def asof(
    data: pl.LazyFrame,
    observations: pl.LazyFrame,
    *,
    value_column: str = "value",
    output_name: str | None = None,
    collect: bool = False,
) -> pl.LazyFrame | pl.DataFrame:
    """Align latest event whose availability time is not after observation time."""

    output = output_name or value_column
    events = data.select("asset_id", "time", pl.col(value_column)).sort("asset_id", "time")
    obs = observations.select("time", "asset_id").sort("asset_id", "time")
    result = obs.join_asof(
        events,
        on="time",
        by="asset_id",
        strategy="backward",
    ).select("time", "asset_id", pl.col(value_column).alias(output))
    return result.collect() if collect else result
