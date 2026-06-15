"""Framework validation."""

from __future__ import annotations

from typing import Protocol

import polars as pl

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.exceptions import ValidationError


class Validator(Protocol):
    """Validate canonical records."""

    def validate(self, frame: pl.LazyFrame, spec: DatasetSpec) -> None:
        """Raise on invalid data."""


class FrameworkValidator:
    """Basic schema validation shared by all datasets."""

    def validate(self, frame: pl.LazyFrame, spec: DatasetSpec) -> None:
        names = set(frame.collect_schema().names())
        required = set(spec.required_columns)
        if not spec.reference:
            required.update({"asset_id", "time"})
        missing = sorted(required - names)
        if missing:
            raise ValidationError(f"{spec.source}/{spec.name} missing columns: {missing}")
        if spec.point_in_time and "period" not in names:
            raise ValidationError(f"{spec.source}/{spec.name} PIT dataset requires period")
