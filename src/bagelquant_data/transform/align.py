"""Alignment transforms."""

from __future__ import annotations

import polars as pl


def align_frame(
    frame: pl.DataFrame,
    *,
    times: pl.Series | None = None,
    asset_ids: pl.Series | None = None,
) -> pl.DataFrame:
    """Align a long-form panel to optional time and asset_id grids."""

    if times is None and asset_ids is None:
        return frame.clone()
    grid = (
        pl.DataFrame({"time": times})
        if times is not None
        else frame.select("time").unique()
    )
    if asset_ids is not None:
        grid = grid.join(pl.DataFrame({"asset_id": asset_ids}), how="cross")
    elif "asset_id" in frame.columns:
        grid = grid.join(frame.select("asset_id").unique(), how="cross")
    keys = [
        column
        for column in ("time", "asset_id")
        if column in grid.columns and column in frame.columns
    ]
    return grid.join(frame, on=keys, how="left").sort(keys)
