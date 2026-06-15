"""Single-value long panel queries."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Literal

import polars as pl

from bagelquant_data.core.exceptions import DuplicateResolutionError
from bagelquant_data.core.types import DateLike
from bagelquant_data.query.raw import RawQueryService

ResolveRule = Literal["latest_period", "latest_revision", "first", "last", "error_on_multiple"]


class FieldQueryService:
    """Extract one or more fields as long panels."""

    def __init__(self, raw_service: RawQueryService) -> None:
        self.raw_service = raw_service

    def field(
        self,
        dataset: str,
        field: str,
        *,
        source: str,
        start: DateLike | None = None,
        end: DateLike | None = None,
        assets: Sequence[str] | None = None,
        resolve: ResolveRule | None = None,
        value_name: str | None = None,
        collect: bool = False,
    ) -> pl.LazyFrame | pl.DataFrame:
        needed = ["time", "asset_id", field]
        if resolve in {"latest_period", "latest_revision"}:
            needed.extend(["period"])
        lf = self.raw_service.raw(
            dataset,
            source=source,
            start=start,
            end=end,
            assets=assets,
            columns=tuple(dict.fromkeys(needed)),
        )
        lf = _resolve_duplicates(lf, field, resolve or "error_on_multiple")
        output = value_name or field
        lf = lf.select("time", "asset_id", pl.col(field).alias(output)).sort("time", "asset_id")
        return lf.collect() if collect else lf

    def fields(
        self,
        dataset: str,
        fields: Sequence[str],
        *,
        source: str,
        start: DateLike | None = None,
        end: DateLike | None = None,
        assets: Sequence[str] | None = None,
        resolve: ResolveRule | None = None,
        collect: bool = False,
    ) -> dict[str, pl.LazyFrame | pl.DataFrame]:
        return {
            field: self.field(
                dataset,
                field,
                source=source,
                start=start,
                end=end,
                assets=assets,
                resolve=resolve,
                collect=collect,
            )
            for field in fields
        }


def _resolve_duplicates(lf: pl.LazyFrame, field: str, resolve: ResolveRule) -> pl.LazyFrame:
    counts = lf.group_by("time", "asset_id").len().filter(pl.col("len") > 1)
    if resolve == "error_on_multiple":
        duplicate_count = counts.select(pl.len()).collect().item()
        if duplicate_count:
            raise DuplicateResolutionError(
                "Multiple records exist for at least one (time, asset_id); pass resolve=..."
            )
        return lf
    if resolve == "latest_period" and "period" in lf.collect_schema().names():
        return lf.sort("time", "asset_id", "period").unique(
            subset=["time", "asset_id"], keep="last", maintain_order=True
        )
    if resolve in {"latest_revision", "last"}:
        return lf.unique(subset=["time", "asset_id"], keep="last", maintain_order=True)
    if resolve == "first":
        return lf.unique(subset=["time", "asset_id"], keep="first", maintain_order=True)
    return lf.select("time", "asset_id", field)
