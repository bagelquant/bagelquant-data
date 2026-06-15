"""Period helpers."""

from __future__ import annotations

import polars as pl


def with_period_year(data: pl.LazyFrame) -> pl.LazyFrame:
    """Add period year for downstream grouping."""

    return data.with_columns(pl.col("period").dt.year().alias("period_year"))
