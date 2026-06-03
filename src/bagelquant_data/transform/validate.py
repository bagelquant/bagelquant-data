"""Validation transforms."""

from __future__ import annotations

import pandas as pd

from bagelquant_data.utils.exceptions import ContractValidationError


def require_columns(frame: pd.DataFrame, columns: set[str]) -> pd.DataFrame:
    """Validate required columns."""

    missing = columns.difference(frame.columns)
    if missing:
        raise ContractValidationError(f"Missing columns: {sorted(missing)}")
    return frame
