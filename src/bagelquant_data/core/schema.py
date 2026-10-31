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


def compatible_key_schema(
    schemas: Iterable[Mapping[str, pl.DataType]], keys: Iterable[str],
) -> pl.Schema:
    """Reconcile consumed schemas only when every key cast is lossless by type.

    Key equality must not depend on sampled values. In particular, Int64 to
    Float64 can collapse distinct keys even when the incoming float is exact.
    This validation uses schema metadata and never reads historical payloads.
    """
    originals = list(schemas)
    resolved = compatible_schema(originals)
    for key in keys:
        if key not in resolved:
            continue
        for schema in originals:
            source = schema.get(key, pl.Null)
            target = resolved[key]
            safe = source == target or source == pl.Null
            if source.is_integer() and target.is_integer():
                signed = {pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.Int128}
                source_bits, target_bits = _integer_width(source), _integer_width(target)
                safe = (source in signed) == (target in signed) and source_bits <= target_bits
                safe |= source not in signed and target in signed and source_bits < target_bits
            elif source.is_integer() and target.is_float():
                precision = 24 if target == pl.Float32 else 53
                safe = _integer_width(source) <= precision
            elif source == pl.Float32 and target == pl.Float64:
                safe = True
            if not safe:
                raise ValidationError(
                    f"Lossless key comparison requires compatible types for {key}: {source} and {target}"
                )
    return resolved


def lossless_cast(series: pl.Series, dtype: pl.DataType) -> pl.Series:
    """Cast selected scalar values only when their exact values survive."""
    if series.dtype == dtype:
        return series
    try:
        converted = series.cast(dtype, strict=True)
        restored = converted.cast(series.dtype, strict=True)
        equal = series.equals(restored, null_equal=True)
        if series.dtype.is_float():
            equal &= series.cast(pl.String).equals(restored.cast(pl.String), null_equal=True)
        if not equal:
            raise ValueError("values changed")
    except (InvalidOperationError, ValueError, TypeError) as error:
        raise ValueError(f"Lossless value conversion from {series.dtype} to {dtype} is required") from error
    return converted


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
    signed = {pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.Int128}
    unsigned = {pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64}
    if left in signed and right in signed:
        return max((left, right), key=_integer_width)
    if left in unsigned and right in unsigned:
        return max((left, right), key=_integer_width)
    return cast(pl.DataType, pl.Int128 if pl.UInt64 in (left, right) or pl.Int128 in (left, right) else pl.Int64)


def _integer_width(dtype: pl.DataType) -> int:
    return {
        pl.Int8: 8,
        pl.UInt8: 8,
        pl.Int16: 16,
        pl.UInt16: 16,
        pl.Int32: 32,
        pl.UInt32: 32,
        pl.Int64: 64,
        pl.Int128: 128,
        pl.UInt64: 64,
    }[dtype]
