"""Adapter utilities for provider implementations."""

from __future__ import annotations

import polars as pl


def immutable_frame(frame: pl.DataFrame) -> pl.DataFrame:
    """Return a defensive Polars DataFrame clone."""

    return frame.clone()
