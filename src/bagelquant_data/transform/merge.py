"""Merge transforms."""

from __future__ import annotations

import pandas as pd


def merge_on_index(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
    """Merge two frames on their index."""

    return left.join(right, how="outer")
