"""Canonical normalization."""

from __future__ import annotations

from dataclasses import dataclass
import polars as pl

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.exceptions import ValidationError


@dataclass(frozen=True, slots=True)
class NormalizeContext:
    """Normalization context."""

    source: str
    dataset: str
    run_id: str | None = None


@dataclass(frozen=True, slots=True)
class NormalizeResult:
    """Accepted and rejected normalized records."""

    accepted: pl.LazyFrame
    rejected: pl.LazyFrame


class StandardNormalizer:
    """Apply explicit dataset field mappings and canonical type coercions."""

    def normalize(
        self, frame: pl.LazyFrame, spec: DatasetSpec, context: NormalizeContext
    ) -> NormalizeResult:
        if not frame.collect_schema():
            frame = pl.DataFrame(schema={"source": pl.String}).lazy()
        names = set(frame.collect_schema().names())
        mappings = spec.field_mappings
        missing_sources = sorted(set(mappings) - names)
        if missing_sources:
            raise ValidationError(
                f"{spec.source}/{spec.name} missing mapped source fields: {missing_sources}"
            )
        unmapped_collisions = sorted(
            target
            for source, target in mappings.items()
            if source != target and target in names and target not in mappings
        )
        if unmapped_collisions:
            raise ValidationError(
                f"{spec.source}/{spec.name} mapped destinations collide with input fields: {unmapped_collisions}"
            )
        rename_map = {
            source: target for source, target in mappings.items() if source != target
        }
        lf = (
            frame.with_columns(
                pl.col(source).alias(target) for source, target in rename_map.items()
            )
            if rename_map
            else frame
        )
        renamed_names = set(lf.collect_schema().names())
        schema = lf.collect_schema()
        expressions: list[pl.Expr] = [pl.lit(context.source).alias("source")]
        if "asset_id" in renamed_names:
            expressions.append(pl.col("asset_id").cast(pl.String).alias("asset_id"))
        if "time" in renamed_names:
            expressions.append(_date_expr("time", schema["time"]).alias("time"))
        if "period" in renamed_names:
            expressions.append(_date_expr("period", schema["period"]).alias("period"))
        accepted = lf.with_columns(expressions)
        if spec.update_type == "by_date":
            source_dates = spec.source_time_fields or ("time",)
            accepted_schema = accepted.collect_schema()
            accepted = accepted.with_columns(
                pl.coalesce([_date_expr(field, accepted_schema[field]) for field in source_dates]).alias(
                    "source_time"
                )
            ).with_columns(pl.col("source_time").alias("time"))

        rejected = accepted.filter(pl.lit(False))
        return NormalizeResult(accepted=accepted, rejected=rejected)


def _date_cast_expr(field: str, dtype: pl.DataType, *, strict: bool = False) -> pl.Expr:
    """Parse ISO strings explicitly; retain typed Date/Datetime/epoch casts."""
    if dtype == pl.String:
        return pl.col(field).str.strip_chars().str.strptime(pl.Date, "%Y-%m-%d", strict=strict)
    return pl.col(field).cast(pl.Date, strict=strict)


def _date_expr(field: str, dtype: pl.DataType) -> pl.Expr:
    """Retain compact provider dates and ordinary typed/ISO date semantics."""
    return (
        pl.when(pl.col(field).cast(pl.String).str.len_chars() == 8)
        .then(
            pl.col(field).cast(pl.String).str.strptime(pl.Date, "%Y%m%d", strict=False)
        )
        .otherwise(_date_cast_expr(field, dtype))
    )
