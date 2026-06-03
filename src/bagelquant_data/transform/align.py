"""Alignment transforms."""

from __future__ import annotations

import pandas as pd


def align_frame(
    frame: pd.DataFrame,
    *,
    index: pd.Index | None = None,
    columns: pd.Index | None = None,
) -> pd.DataFrame:
    """Align a frame to optional index and columns."""

    return frame.reindex(index=index or frame.index, columns=columns or frame.columns)
