"""Record inspection queries."""

from __future__ import annotations

import polars as pl

from bagelquant_data.query.raw import RawQueryService


class RecordsQueryService:
    """Inspect canonical rows."""

    def __init__(self, raw_service: RawQueryService) -> None:
        self.raw_service = raw_service

    def records(self, dataset: str, *, source: str, limit: int = 100) -> pl.DataFrame:
        return self.raw_service.raw(dataset, source=source).limit(limit).collect()
