"""Merge transforms."""

from __future__ import annotations

import polars as pl


def merge_on_index(left: pl.DataFrame, right: pl.DataFrame) -> pl.DataFrame:
    """Merge two long-form frames on shared key columns."""

    keys = [
        column
        for column in ("time", "asset_id")
        if column in left.columns and column in right.columns
    ]
    if not keys:
        raise ValueError("merge_on_index requires shared time/asset_id keys")
    return left.join(right, on=keys, how="outer")
