"""Canonical raw record queries."""

from __future__ import annotations

from collections.abc import Sequence

import polars as pl

from bagelquant_data.core.types import DateLike
from bagelquant_data.query.scanner import manifest_paths
from bagelquant_data.storage.metadata import MetadataStore
from bagelquant_data.storage.parquet import ParquetStore


class RawQueryService:
    """Read row-oriented canonical records."""

    def __init__(self, parquet: ParquetStore, metadata: MetadataStore) -> None:
        self.parquet = parquet
        self.metadata = metadata

    def query_general(
        self,
        dataset: str,
        *,
        source: str,
        fields: Sequence[str] | None = None,
    ) -> pl.LazyFrame:
        return self._scan(dataset, source=source, fields=fields)

    def query(
        self,
        dataset: str,
        *,
        source: str,
        start: DateLike | None = None,
        end: DateLike | None = None,
        assets: Sequence[str] | None = None,
        fields: Sequence[str] | None = None,
    ) -> pl.LazyFrame:
        lf = self._scan(dataset, source=source, fields=None)
        if start is not None:
            lf = lf.filter(pl.col("time") >= _date_literal(start))
        if end is not None:
            lf = lf.filter(pl.col("time") <= _date_literal(end))
        if assets is not None:
            lf = lf.filter(pl.col("asset_id").is_in(list(assets)))
        return _select_fields(lf, fields)

    def _scan(self, dataset: str, *, source: str, fields: Sequence[str] | None) -> pl.LazyFrame:
        paths = manifest_paths(self.metadata, source, dataset)
        lf = self.parquet.scan_dataset(source, dataset, paths or None)
        return _select_fields(lf, fields)


def _date_literal(value: DateLike) -> pl.Expr:
    return pl.lit(value).cast(pl.Date, strict=False)


def _select_fields(frame: pl.LazyFrame, fields: Sequence[str] | None) -> pl.LazyFrame:
    if fields is None:
        return frame
    available = set(frame.collect_schema().names())
    return frame.select([field for field in fields if field in available])
