"""Observation grid construction."""

from __future__ import annotations

from collections.abc import Sequence

import polars as pl

from bagelquant_data.core.types import DateLike


def observations(
    *,
    start: DateLike,
    end: DateLike,
    frequency: str,
    assets: Sequence[str] | None = None,
    universe: str | pl.LazyFrame | None = None,
    calendar: str = "trade_cal",
) -> pl.LazyFrame:
    """Build a generic observation grid."""

    interval = {
        "daily": "1d",
        "week_end": "1w",
        "month_end": "1mo",
        "quarter_end": "3mo",
    }.get(frequency, frequency)
    dates = pl.DataFrame(
        {
            "time": pl.date_range(
                pl.lit(start).cast(pl.Date, strict=False),
                pl.lit(end).cast(pl.Date, strict=False),
                interval,
                eager=True,
            )
        }
    )
    asset_values = list(assets or [])
    if not asset_values and isinstance(universe, pl.LazyFrame):
        asset_values = universe.select("asset_id").unique().collect()["asset_id"].to_list()
    asset_frame = pl.DataFrame({"asset_id": asset_values})
    if asset_frame.is_empty():
        return dates.lazy().with_columns(pl.lit(None, dtype=pl.String).alias("asset_id")).filter(pl.lit(False))
    return dates.join(asset_frame, how="cross").lazy().select("time", "asset_id")
