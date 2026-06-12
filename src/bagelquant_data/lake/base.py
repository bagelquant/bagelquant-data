"""Data lake interfaces."""

from __future__ import annotations

from typing import Protocol

import polars as pl

from bagelquant_data.lake.snapshot import SnapshotRef


class LakeStore(Protocol):
    """Backend-neutral lake store interface."""

    def read(
        self,
        source: str,
        dataset: str,
        *,
        snapshot: str | None = None,
    ) -> pl.DataFrame:
        """Read a dataset snapshot."""
        raise NotImplementedError

    def write(
        self,
        source: str,
        dataset: str,
        data: pl.DataFrame,
        *,
        mode: str = "append",
    ) -> SnapshotRef:
        """Write a dataset and return a snapshot id."""
        raise NotImplementedError
