"""Rejected record storage."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from bagelquant_data.storage.paths import LakePaths


class RejectedStore:
    """Write malformed records outside the canonical lake."""

    def __init__(self, paths: LakePaths) -> None:
        self.paths = paths

    def write(self, source: str, dataset: str, run_id: str, reason: str, frame: pl.DataFrame) -> Path:
        path = self.paths.rejected / source / dataset / run_id / f"{reason}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = frame.with_columns(
            pl.lit(source).alias("source"),
            pl.lit(dataset).alias("dataset"),
            pl.lit(run_id).alias("run_id"),
            pl.lit(reason).alias("rejection_reason"),
            pl.lit(datetime.now(UTC)).alias("rejected_at"),
        )
        payload.write_parquet(path, compression="zstd", compression_level=3, statistics=True)
        return path
