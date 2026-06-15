"""Temporary staging storage."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import polars as pl

from bagelquant_data.storage.paths import LakePaths


class StagingStore:
    """Store temporary source responses."""

    def __init__(self, paths: LakePaths) -> None:
        self.paths = paths

    def write(self, source: str, dataset: str, frame: pl.DataFrame, run_id: str) -> Path:
        path = self.paths.staging / source / dataset / run_id / f"{uuid4().hex}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.write_parquet(path, compression="zstd", compression_level=3, statistics=True)
        return path

    def cleanup(self, source: str, dataset: str, run_id: str) -> None:
        import shutil

        shutil.rmtree(self.paths.staging / source / dataset / run_id, ignore_errors=True)
