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
        names = set(frame.collect_schema().names())
        mappings = spec.field_mappings
        missing_sources = sorted(set(mappings) - names)
        if missing_sources:
            raise ValidationError(f"{spec.source}/{spec.name} missing mapped source fields: {missing_sources}")
        unmapped_collisions = sorted(
            target for source, target in mappings.items() if source != target and target in names and target not in mappings
        )
        if unmapped_collisions:
            raise ValidationError(
                f"{spec.source}/{spec.name} mapped destinations collide with input fields: {unmapped_collisions}"
            )
        rename_map = {source: target for source, target in mappings.items() if source != target}
        lf = frame.rename(rename_map) if rename_map else frame
        renamed_names = set(lf.collect_schema().names())
        expressions: list[pl.Expr] = [pl.lit(context.source).alias("source")]
        if "asset_id" in renamed_names:
            expressions.append(pl.col("asset_id").cast(pl.String).alias("asset_id"))
        if "time" in renamed_names:
            expressions.append(_date_expr("time").alias("time"))
        if "period" in renamed_names:
            expressions.append(_date_expr("period").alias("period"))
        accepted = lf.with_columns(expressions)
        rejected = accepted.filter(pl.lit(False))
        return NormalizeResult(accepted=accepted, rejected=rejected)


def _date_expr(field: str) -> pl.Expr:
    return (
        pl.when(pl.col(field).cast(pl.String).str.len_chars() == 8)
        .then(pl.col(field).cast(pl.String).str.strptime(pl.Date, "%Y%m%d", strict=False))
        .otherwise(pl.col(field).cast(pl.Date, strict=False))
    )
