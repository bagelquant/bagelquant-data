"""Pure descriptive exploration of neutral daily DataItem long tables."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from math import isfinite
from typing import Any

import polars as pl


def _validate(frame: pl.DataFrame) -> None:
    missing = {"time", "asset_id", "value"} - set(frame.columns)
    if missing:
        raise ValueError(f"Panel is missing columns: {sorted(missing)}")
    if frame.select("time", "asset_id").is_duplicated().any():
        raise ValueError("Panel has duplicate time-asset keys")


def _valid(frame: pl.DataFrame) -> pl.Expr:
    expression = pl.col("value").is_not_null()
    return expression & pl.col("value").is_finite().fill_null(False) if frame.schema["value"].is_float() else expression


def _eligible(frame: pl.DataFrame, coordinates: pl.DataFrame) -> pl.DataFrame:
    _validate(frame)
    if not {"time", "asset_id"}.issubset(coordinates.columns):
        raise ValueError("Coverage requires explicit time-asset coordinates")
    grid = coordinates.select("time", "asset_id")
    if grid.is_duplicated().any() or grid.null_count().row(0) != (0, 0):
        raise ValueError("Coordinates must have unique non-null time-asset keys")
    return grid.join(frame, on=["time", "asset_id"], how="left", validate="1:1").sort("time", "asset_id")


def overview(frame: pl.DataFrame, *, coordinates: pl.DataFrame | None = None) -> dict[str, Any]:
    """Count valid, null, NaN and infinite values independently."""
    _validate(frame)
    selected = _eligible(frame, coordinates) if coordinates is not None else frame
    valid = selected.filter(_valid(selected))
    numeric = selected.schema["value"].is_numeric()
    floats = selected.schema["value"].is_float()
    null_count = selected["value"].null_count()
    nan_count = int(selected["value"].is_nan().sum()) if floats else 0
    infinite_count = int(selected["value"].is_infinite().sum()) if floats else 0
    expected = selected.height if coordinates is not None else None
    return {
        "source_row_count": frame.height,
        "row_count": selected.height,
        "expected_count": expected,
        "valid_count": valid.height,
        "null_count": null_count,
        "nan_count": nan_count,
        "infinite_count": infinite_count,
        "non_finite_count": nan_count + infinite_count,
        "missing_count": selected.height - valid.height,
        "coverage_rate": valid.height / expected if expected else None,
        "missing_rate": (expected - valid.height) / expected if expected else None,
        "asset_count": selected["asset_id"].n_unique(),
        "date_count": selected["time"].n_unique(),
        "value_dtype": str(frame.schema["value"]),
        "numeric": numeric,
        "cardinality": valid["value"].n_unique(),
        "first_valid_date": valid["time"].min(),
        "last_valid_date": valid["time"].max(),
    }


def coverage(frame: pl.DataFrame, coordinates: pl.DataFrame, *, by: str = "time") -> pl.DataFrame:
    """Coverage denominators come solely from the explicit coordinate grid."""
    if by not in {"time", "asset_id"}:
        raise ValueError("Coverage grouping must be time or asset_id")
    eligible = _eligible(frame, coordinates).with_columns(_valid(frame).alias("_valid"))
    return eligible.group_by(by).agg(
        pl.len().alias("expected_count"), pl.col("_valid").sum().alias("valid_count"),
        pl.col("value").null_count().alias("null_count"),
    ).with_columns(
        (pl.col("expected_count") - pl.col("valid_count")).alias("missing_count"),
        (pl.col("valid_count") / pl.col("expected_count")).alias("coverage_rate"),
    ).sort(by)


def quantiles(frame: pl.DataFrame, probabilities: Sequence[float] = (.01, .05, .25, .5, .75, .95, .99)) -> pl.DataFrame:
    """Finite-value quantiles using linear interpolation."""
    _validate(frame)
    if not frame.schema["value"].is_numeric():
        raise ValueError("Quantiles require a numerical value dtype")
    if any(not 0 <= value <= 1 for value in probabilities):
        raise ValueError("Quantile probabilities must be between zero and one")
    values = frame.filter(_valid(frame))["value"]
    return pl.DataFrame({"probability": list(probabilities), "value": [values.quantile(value, interpolation="linear") for value in probabilities]}, schema={"probability": pl.Float64, "value": pl.Float64})


def distribution(frame: pl.DataFrame) -> pl.DataFrame:
    """Numerical summary statistics or categorical value frequencies."""
    _validate(frame)
    valid = frame.filter(_valid(frame))
    if not frame.schema["value"].is_numeric():
        return valid.group_by("value").len(name="count").sort("value")
    values = valid["value"]
    statistics: dict[str, Any] = {"count": values.len(), "mean": values.mean(), "std": values.std(), "min": values.min(), "max": values.max()}
    for probability, value in quantiles(valid).iter_rows():
        statistics[f"p{int(probability * 100):02d}"] = value
    return pl.DataFrame({"statistic": list(statistics), "value": [float(value) if value is not None else None for value in statistics.values()]}, schema={"statistic": pl.String, "value": pl.Float64})


def outliers(frame: pl.DataFrame, *, iqr_multiplier: float = 1.5, limit: int | None = None) -> pl.DataFrame:
    """Finite IQR outliers ordered by their time-asset coordinate."""
    _validate(frame)
    if not isfinite(iqr_multiplier) or iqr_multiplier < 0 or limit is not None and limit < 0:
        raise ValueError("Outlier multiplier and limit must be non-negative")
    if not frame.schema["value"].is_numeric():
        return frame.head(0)
    valid = frame.filter(_valid(frame))
    if valid.is_empty():
        return valid
    lower, upper = quantiles(valid, (.25, .75))["value"].to_list()
    distance = (upper - lower) * iqr_multiplier
    selected = valid.filter((pl.col("value") < lower - distance) | (pl.col("value") > upper + distance)).sort("time", "asset_id")
    return selected if limit is None else selected.head(limit)


def time_series(frame: pl.DataFrame, asset_id: str) -> pl.DataFrame:
    _validate(frame)
    return frame.filter(pl.col("asset_id") == asset_id).sort("time")


def cross_section(frame: pl.DataFrame, time: date | str) -> pl.DataFrame:
    _validate(frame)
    value = date.fromisoformat(time) if isinstance(time, str) else time
    return frame.filter(pl.col("time") == value).sort("asset_id")


def profile_item(
    source: pl.DataFrame, coordinates: pl.DataFrame, *,
    outlier_iqr_multiplier: float = 1.5, outlier_limit: int = 500,
) -> dict[str, Any]:
    """Return one inspectable profile without charts, report IO or app metadata."""
    eligible = _eligible(source, coordinates)
    valid = eligible.filter(_valid(eligible))
    return {
        "summary": overview(source, coordinates=coordinates),
        "daily_coverage": coverage(source, coordinates, by="time"),
        "asset_coverage": coverage(source, coordinates, by="asset_id"),
        "distribution": distribution(eligible),
        "outliers": outliers(eligible, iqr_multiplier=outlier_iqr_multiplier, limit=outlier_limit),
        "values": valid.select("time", "asset_id", "value").sort("time", "asset_id"),
    }


__all__ = ["overview", "coverage", "quantiles", "distribution", "outliers", "time_series", "cross_section", "profile_item"]
