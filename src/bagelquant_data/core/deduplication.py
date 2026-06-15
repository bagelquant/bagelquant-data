"""Deduplication strategies."""

from __future__ import annotations

from typing import Protocol

import polars as pl

from bagelquant_data.core.dataset import DatasetSpec


class DeduplicationStrategy(Protocol):
    """Deduplicate records for a dataset."""

    def apply(self, frame: pl.LazyFrame, spec: DatasetSpec) -> pl.LazyFrame:
        """Return deduplicated records."""
        ...


class NoDeduplication:
    """Leave records unchanged."""

    def apply(self, frame: pl.LazyFrame, spec: DatasetSpec) -> pl.LazyFrame:
        return frame


class ExactRecordHashDeduplication:
    """Drop exact duplicate rows."""

    def apply(self, frame: pl.LazyFrame, spec: DatasetSpec) -> pl.LazyFrame:
        return frame.unique(maintain_order=True)


class PrimaryKeyLastDeduplication:
    """Keep the last row for each primary key."""

    def apply(self, frame: pl.LazyFrame, spec: DatasetSpec) -> pl.LazyFrame:
        if not spec.primary_key:
            return frame.unique(maintain_order=True)
        return frame.unique(subset=list(spec.primary_key), keep="last", maintain_order=True)
