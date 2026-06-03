"""Resampling transforms."""

from __future__ import annotations

import pandas as pd


def resample_last(frame: pd.DataFrame, rule: str) -> pd.DataFrame:
    """Resample a time-indexed frame using the last value."""

    return frame.resample(rule).last()
