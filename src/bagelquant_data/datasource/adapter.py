"""Adapter utilities for provider implementations."""

from __future__ import annotations

from typing import Any

import pandas as pd


def immutable_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Return a defensive DataFrame copy with non-writeable backing values."""

    copied = frame.copy(deep=True)
    values: Any = copied.to_numpy(copy=False)
    if hasattr(values, "flags"):
        values.flags.writeable = False
    return copied
