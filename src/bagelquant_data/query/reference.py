"""Reference dataset queries."""

from __future__ import annotations

import polars as pl

from bagelquant_data.query.raw import RawQueryService


class ReferenceQueryService:
    """Read reference data in row-oriented form."""

    def __init__(self, raw_service: RawQueryService) -> None:
        self.raw_service = raw_service

    def reference(self, dataset: str, *, source: str, collect: bool = False) -> pl.LazyFrame | pl.DataFrame:
        lf = self.raw_service.raw(dataset, source=source)
        return lf.collect() if collect else lf
