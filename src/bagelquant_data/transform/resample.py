"""Resampling transforms."""

from __future__ import annotations

import polars as pl


def resample_last(frame: pl.DataFrame, rule: str) -> pl.DataFrame:
    """Resample a long-form time panel using the last value per bucket."""

    if "time" not in frame.columns:
        raise ValueError("resample_last requires a time column")
    group_keys = ["time"]
    if "asset_id" in frame.columns:
        group_keys.append("asset_id")
    return (
        frame.sort("time")
        .group_by_dynamic(
            "time", every=rule, group_by=[key for key in group_keys if key != "time"]
        )
        .agg(pl.all().exclude(group_keys).last())
        .sort(group_keys)
    )
