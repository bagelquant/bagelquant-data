"""Deterministic schema reconciliation for canonical lake data."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import cast

import polars as pl
from polars.exceptions import InvalidOperationError

from bagelquant_data.core.exceptions import ValidationError


def normalize_all_null_columns(frame: pl.DataFrame) -> pl.DataFrame:
    """Represent entirely null columns as Null so inference cannot invent String."""

    if frame.is_empty():
        return frame
    null_counts = frame.null_count().row(0, named=True)
    expressions = [
        pl.lit(None).alias(name)
        for name in frame.columns
        if null_counts[name] == frame.height
    ]
    return frame.with_columns(expressions) if expressions else frame


def compatible_schema(
    schemas: Iterable[Mapping[str, pl.DataType]],
) -> pl.Schema:
    """Resolve compatible primitive schemas in stable first-seen column order."""

    resolved: dict[str, pl.DataType] = {}
    for schema in schemas:
        for name, dtype in schema.items():
            current = resolved.get(name)
            resolved[name] = (
                dtype if current is None else _common_dtype(name, current, dtype)
            )
    return pl.Schema(resolved)


def align_frame(frame: pl.DataFrame, schema: Mapping[str, pl.DataType]) -> pl.DataFrame:
    """Add missing fields and cast present fields to a resolved canonical schema."""

    available = frame.schema
    if list(available.items()) == list(schema.items()):
        return frame
    expressions: list[pl.Expr] = []
    for name, dtype in schema.items():
        if name not in available:
            expressions.append(pl.lit(None, dtype=dtype).alias(name))
        elif available[name] != dtype:
            expressions.append(pl.col(name).cast(dtype, strict=True).alias(name))
    try:
        aligned = frame.with_columns(expressions) if expressions else frame
    except InvalidOperationError as error:
        raise ValidationError(
            f"Values cannot be parsed as canonical schema types: {error}"
        ) from error
    return aligned.select(list(schema))


def align_lazy_frame(
    frame: pl.LazyFrame,
    schema: Mapping[str, pl.DataType],
    fields: Iterable[str],
) -> pl.LazyFrame:
    """Project and cast a lazy scan to selected fields from a canonical schema."""

    selected = list(fields)
    available = frame.collect_schema()
    expressions: list[pl.Expr] = []
    for name in selected:
        dtype = schema[name]
        if name not in available:
            expressions.append(pl.lit(None, dtype=dtype).alias(name))
        elif available[name] == dtype:
            expressions.append(pl.col(name))
        else:
            expressions.append(pl.col(name).cast(dtype, strict=True).alias(name))
    return frame.select(expressions)


def concat_compatible_frames(frames: Iterable[pl.DataFrame]) -> pl.DataFrame:
    """Normalize, align, and concatenate frames without String type pollution."""

    normalized = [normalize_all_null_columns(frame) for frame in frames]
    if not normalized:
        return pl.DataFrame()
    if len(normalized) == 1:
        return normalized[0]
    schema = compatible_schema(frame.schema for frame in normalized)
    return pl.concat(
        [align_frame(frame, schema) for frame in normalized],
        how="vertical",
        rechunk=False,
    )


def _common_dtype(field: str, left: pl.DataType, right: pl.DataType) -> pl.DataType:
    if left == right:
        return left
    if left == pl.Null:
        return right
    if right == pl.Null:
        return left
    if left.is_integer() and right.is_integer():
        return _integer_supertype(left, right)
    if left.is_numeric() and right.is_numeric():
        return cast(pl.DataType, pl.Float64)
    if left == pl.String and right.is_numeric():
        return right
    if right == pl.String and left.is_numeric():
        return left
    raise ValidationError(
        f"Incompatible canonical types for {field}: {left} and {right}"
    )


def _integer_supertype(left: pl.DataType, right: pl.DataType) -> pl.DataType:
    signed = {pl.Int8, pl.Int16, pl.Int32, pl.Int64}
    unsigned = {pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64}
    if left in signed and right in signed:
        return max((left, right), key=_integer_width)
    if left in unsigned and right in unsigned:
        return max((left, right), key=_integer_width)
    return cast(pl.DataType, pl.Int64)


def _integer_width(dtype: pl.DataType) -> int:
    return {
        pl.Int8: 8,
        pl.UInt8: 8,
        pl.Int16: 16,
        pl.UInt16: 16,
        pl.Int32: 32,
        pl.UInt32: 32,
        pl.Int64: 64,
        pl.UInt64: 64,
    }[dtype]
