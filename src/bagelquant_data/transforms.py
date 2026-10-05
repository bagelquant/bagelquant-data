"""Serializable, provider-neutral frame transformations.

Transformations deliberately contain no formula language or numerical graph.
Their input frames and explicit coordinates are supplied by the caller.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from typing import Any, Literal, TypeAlias

import polars as pl


_DTYPES: dict[str, pl.DataType | type[pl.DataType]] = {
    "float64": pl.Float64, "float32": pl.Float32,
    "int64": pl.Int64, "int32": pl.Int32, "int16": pl.Int16, "int8": pl.Int8,
    "uint64": pl.UInt64, "uint32": pl.UInt32, "uint16": pl.UInt16, "uint8": pl.UInt8,
    "string": pl.String, "bool": pl.Boolean, "boolean": pl.Boolean,
    "date": pl.Date, "datetime": pl.Datetime("us"), "categorical": pl.Categorical,
}


def scalar_dtype(value: str | pl.DataType | type[pl.DataType]) -> pl.DataType | type[pl.DataType]:
    """Resolve a supported scalar dtype without importing a numerical package."""
    key = str(value).lower()
    if key in {"str", "utf8"}:
        key = "string"
    if key not in _DTYPES:
        raise ValueError(f"Unsupported DataItem scalar dtype: {value!r}")
    return _DTYPES[key]


def dtype_name(value: str | pl.DataType | type[pl.DataType]) -> str:
    dtype = scalar_dtype(value)
    for name, candidate in _DTYPES.items():
        if dtype == candidate:
            return name
    raise ValueError(f"Unsupported DataItem scalar dtype: {value!r}")


@dataclass(frozen=True, slots=True)
class Select:
    """Select explicit columns; rename is applied after projection."""
    columns: tuple[str, ...]
    rename: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Filter:
    """A typed comparison, membership, or null predicate."""
    column: str
    operator: str
    value: Any = None


@dataclass(frozen=True, slots=True)
class MapValues:
    """Map categorical/scalar values, retaining unmapped values by default."""
    column: str
    mapping: Mapping[Any, Any]
    output_column: str | None = None


@dataclass(frozen=True, slots=True)
class Cast:
    columns: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class Join:
    """Join another named input using explicit keys and join cardinality."""
    input: str
    on: tuple[str, ...]
    how: Literal["left", "inner", "full", "semi", "anti"] = "left"
    validate: Literal["1:1", "1:m", "m:1", "m:m"] = "1:1"
    suffix: str = "_right"


@dataclass(frozen=True, slots=True)
class Align:
    """Align to supplied (time, asset_id) coordinates.

    Forward filling counts the caller's distinct coordinate dates, never an
    implicit exchange calendar. A null event supersedes the preceding value.
    """
    input: str
    forward_fill_sessions: int | None = None
    available_column: str = "available_date"

    def __post_init__(self) -> None:
        if self.forward_fill_sessions is not None and (isinstance(self.forward_fill_sessions, bool) or not isinstance(self.forward_fill_sessions, int) or self.forward_fill_sessions <= 0):
            raise ValueError("forward_fill_sessions must be a positive finite integer")


Transform: TypeAlias = Select | Filter | MapValues | Cast | Join | Align
_TRANSFORMS = {value.__name__: value for value in (Select, Filter, MapValues, Cast, Join, Align)}


def transform_payload(value: Transform) -> dict[str, Any]:
    result = {"kind": type(value).__name__, **asdict(value)}
    if isinstance(value, Filter):
        result["value"] = _encode_scalar(value.value)
    # JSON object keys would destroy integer/bool mapping keys.
    if isinstance(value, MapValues):
        result["mapping"] = [[_encode_scalar(key), _encode_scalar(mapped)] for key, mapped in value.mapping.items()]
    return result


def _encode_scalar(value: Any) -> Any:
    if isinstance(value, datetime):
        return {"scalar_type": "datetime", "value": value.isoformat()}
    if isinstance(value, date):
        return {"scalar_type": "date", "value": value.isoformat()}
    if isinstance(value, (list, tuple)):
        return [_encode_scalar(item) for item in value]
    return value


def _decode_scalar(value: Any) -> Any:
    if isinstance(value, dict) and value.get("scalar_type") in {"date", "datetime"}:
        return date.fromisoformat(value["value"]) if value["scalar_type"] == "date" else datetime.fromisoformat(value["value"])
    if isinstance(value, list):
        return [_decode_scalar(item) for item in value]
    return value


def transform_from_payload(value: Mapping[str, Any]) -> Transform:
    payload = dict(value)
    kind = str(payload.pop("kind"))
    if kind not in _TRANSFORMS:
        raise ValueError(f"Unknown transform: {kind}")
    if kind == "Select":
        return Select(tuple(payload["columns"]), dict(payload.get("rename", {})))
    if kind == "Filter":
        payload["value"] = _decode_scalar(payload.get("value"))
        return Filter(**payload)
    if kind == "MapValues":
        return MapValues(payload["column"], {_decode_scalar(key): _decode_scalar(mapped) for key, mapped in payload["mapping"]}, payload.get("output_column"))
    if kind == "Cast":
        return Cast(dict(payload["columns"]))
    if kind == "Join":
        payload["on"] = tuple(payload["on"])
        return Join(**payload)
    return Align(**payload)


def apply_transforms(
    frame: pl.DataFrame,
    transforms: tuple[Transform, ...],
    inputs: Mapping[str, pl.DataFrame],
) -> pl.DataFrame:
    """Apply a deterministic transformation sequence to an input frame."""
    for operation in transforms:
        if isinstance(operation, Select):
            frame = frame.select(operation.columns).rename(operation.rename)
        elif isinstance(operation, Filter):
            column = pl.col(operation.column)
            comparison = operation.value
            expressions = {
                "eq": lambda: column == comparison,
                "ne": lambda: column != comparison,
                "lt": lambda: column < comparison,
                "le": lambda: column <= comparison,
                "gt": lambda: column > comparison,
                "ge": lambda: column >= comparison,
                "in": lambda: column.is_in(comparison),
                "is_null": column.is_null,
                "is_not_null": column.is_not_null,
            }
            if operation.operator not in expressions:
                raise ValueError(f"Unsupported filter operator: {operation.operator}")
            frame = frame.filter(expressions[operation.operator]())
        elif isinstance(operation, MapValues):
            frame = frame.with_columns(
                pl.col(operation.column).replace(dict(operation.mapping))
                .alias(operation.output_column or operation.column)
            )
        elif isinstance(operation, Cast):
            frame = frame.with_columns(
                pl.col(name).cast(scalar_dtype(dtype), strict=True)
                for name, dtype in operation.columns.items()
            )
        elif isinstance(operation, Join):
            if not operation.on:
                raise ValueError("Join requires explicit keys")
            if operation.how not in {"left", "inner", "full", "semi", "anti"}:
                raise ValueError(f"Unsupported join mode: {operation.how}")
            frame = frame.join(
                inputs[operation.input], on=list(operation.on), how=operation.how,
                validate=operation.validate, suffix=operation.suffix, coalesce=True,
            )
        else:
            frame = align_panel(
                frame, inputs[operation.input],
                forward_fill_sessions=operation.forward_fill_sessions,
                available_column=operation.available_column,
            )
    return frame


def align_panel(
    observations: pl.DataFrame,
    coordinates: pl.DataFrame,
    *,
    forward_fill_sessions: int | None = None,
    available_column: str = "available_date",
) -> pl.DataFrame:
    """Materialize only explicit membership coordinates using causal events."""
    keys = ["time", "asset_id"]
    for name, frame in (("observations", observations), ("coordinates", coordinates)):
        missing = set(keys) - set(frame.columns)
        if missing:
            raise ValueError(f"{name} is missing columns: {sorted(missing)}")
        if frame.select(keys).is_duplicated().any():
            raise ValueError(f"{name} contains duplicate time-asset keys")
        if any(frame[key].null_count() for key in keys):
            raise ValueError(f"{name} has null time-asset keys")
    grid = coordinates.select(keys).with_columns(pl.col("time").cast(pl.Date), pl.col("asset_id").cast(pl.String))
    events = observations.with_columns(pl.col("time").cast(pl.Date), pl.col("asset_id").cast(pl.String))
    if available_column not in events.columns:
        events = events.with_columns(pl.col("time").alias(available_column))
    else:
        events = events.with_columns(pl.col(available_column).cast(pl.Date))
    if events[available_column].null_count():
        raise ValueError("availability dates must not be null")
    # Observations cannot be exposed before either their observation date or
    # their declared availability date.
    events = events.with_columns(pl.max_horizontal("time", available_column).alias("_effective_date"))
    if forward_fill_sessions is None:
        events = events.filter(pl.col("_effective_date") == pl.col("time")).drop("_effective_date")
        return grid.join(events, on=keys, how="left", validate="1:1").sort(keys)
    if isinstance(forward_fill_sessions, bool) or not isinstance(forward_fill_sessions, int) or forward_fill_sessions <= 0:
        raise ValueError("forward_fill_sessions must be a positive finite integer")
    sessions = grid.select("time").unique().sort("time").with_row_index("_coordinate_index")
    # Map off-calendar availability onto its next explicit coordinate date.
    events = events.sort("_effective_date").join_asof(
        sessions.rename({"time": "_session_date", "_coordinate_index": "_event_index"}),
        left_on="_effective_date", right_on="_session_date", strategy="forward",
    ).drop_nulls("_event_index")
    events = events.rename({"time": "observation_time"}).sort("asset_id", "_effective_date", "observation_time")
    events = events.unique(["asset_id", "_effective_date"], keep="last", maintain_order=True)
    result = grid.join(sessions, on="time", how="left").sort("asset_id", "time").join_asof(
        events, left_on="time", right_on="_effective_date", by="asset_id",
        strategy="backward", check_sortedness=False,
    )
    valid = pl.col("_event_index").is_not_null() & (
        (pl.col("_coordinate_index") - pl.col("_event_index")) < forward_fill_sessions
    )
    # Keep the requested coordinates after expiration rather than erase missingness.
    payload = [name for name in observations.columns if name not in keys]
    for name in payload:
        result = result.with_columns(pl.when(valid).then(pl.col(name)).otherwise(None).alias(name))
    if "observation_date" not in result.columns:
        result = result.with_columns(pl.col("observation_time").alias("observation_date"))
    return result.drop("_coordinate_index", "_event_index", "_effective_date", "_session_date").sort(keys)


__all__ = ["Select", "Filter", "MapValues", "Cast", "Join", "Align", "Transform", "align_panel", "apply_transforms"]
