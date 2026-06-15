"""Atomic file replacement helpers."""

from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

import polars as pl

from bagelquant_data.core.exceptions import ValidationError


def atomic_write_parquet(frame: pl.DataFrame, path: Path) -> None:
    """Write a parquet file then atomically replace the destination."""

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    frame.write_parquet(
        tmp,
        compression="zstd",
        compression_level=3,
        statistics=True,
    )
    read_back = pl.read_parquet(tmp)
    if read_back.height != frame.height:
        tmp.unlink(missing_ok=True)
        raise ValidationError("Atomic parquet write failed read-back row count check")
    os.replace(tmp, path)
