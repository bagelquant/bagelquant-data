"""Validation transforms."""

from __future__ import annotations

import polars as pl

from bagelquant_data.utils.exceptions import ContractValidationError


def require_columns(frame: pl.DataFrame, columns: set[str]) -> pl.DataFrame:
    """Validate required columns."""

    missing = columns.difference(frame.columns)
    if missing:
        raise ContractValidationError(f"Missing columns: {sorted(missing)}")
    return frame
