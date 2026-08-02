"""Framework validation."""

from __future__ import annotations

from typing import Protocol

import polars as pl

from bagelquant_data.core.dataset import DatasetSpec, incremental_key
from bagelquant_data.core.exceptions import ValidationError


class Validator(Protocol):
    """Validate canonical records."""

    def validate(self, frame: pl.LazyFrame, spec: DatasetSpec) -> None:
        """Raise on invalid data."""


class FrameworkValidator:
    """Basic schema validation shared by all datasets."""

    def validate(self, frame: pl.LazyFrame, spec: DatasetSpec) -> None:
        names = set(frame.collect_schema().names())
        required = set(incremental_key(spec) or ())
        missing = sorted(required - names)
        if missing:
            raise ValidationError(f"{spec.source}/{spec.name} missing fields: {missing}")
        if required and frame.select(
            pl.any_horizontal(
                pl.col(column).is_null() for column in sorted(required)
            ).any()
        ).collect().item():
            raise ValidationError(
                f"{spec.source}/{spec.name} contains null primary key values"
            )
