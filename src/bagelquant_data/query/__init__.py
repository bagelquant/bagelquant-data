"""Minimal lazy query facade."""

from __future__ import annotations

from collections.abc import Sequence

import polars as pl

from bagelquant_data.core.dataset import incremental_key
from bagelquant_data.core.exceptions import ConfigurationError
from bagelquant_data.core.types import DateLike
from bagelquant_data.management.datasets import DatasetManager
from bagelquant_data.query.raw import RawQueryService


class LakeQuery:
    """Read general and canonical-keyed datasets as Polars LazyFrames."""

    def __init__(self, raw_service: RawQueryService, datasets: DatasetManager) -> None:
        self._raw = raw_service
        self._datasets = datasets

    def query_general(
        self,
        dataset: str,
        *,
        source: str,
        fields: Sequence[str] | None = None,
    ) -> pl.LazyFrame:
        """Read any dataset without `time` or `asset_id` filters."""

        return self._raw.query_general(dataset, source=source, fields=fields)

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
        """Read an incremental dataset filtered by its canonical key."""

        spec = self._datasets.get(dataset, source=source)
        if incremental_key(spec) is None:
            raise ConfigurationError(f"{source}/{dataset} is general; use query_general()")
        return self._raw.query(
            dataset,
            source=source,
            start=start,
            end=end,
            assets=assets,
            fields=fields,
        )
