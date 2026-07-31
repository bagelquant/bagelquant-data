"""Atomic file replacement helpers."""

from __future__ import annotations

import os
import time
from pathlib import Path
from uuid import uuid4

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from bagelquant_data.core.exceptions import ValidationError


def atomic_write_parquet(
    frame: pl.DataFrame,
    path: Path,
    *,
    expected_schema: pa.Schema | None = None,
) -> None:
    """Write a parquet file then atomically replace the destination."""

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        frame.write_parquet(
            tmp,
            compression="zstd",
            compression_level=3,
            statistics=True,
        )
        parquet_file = pq.ParquetFile(tmp)
        try:
            metadata = parquet_file.metadata
            physical_schema = (
                expected_schema
                if expected_schema is not None
                else frame.head(0).to_arrow().schema
            )
            schema_matches = parquet_file.schema_arrow.equals(physical_schema)
        finally:
            parquet_file.close()
        if (
            metadata.num_rows != frame.height
            or metadata.num_columns != frame.width
            or not schema_matches
        ):
            raise ValidationError(
                "Atomic parquet write failed read-back row count check"
            )
        for attempt in range(8):
            try:
                os.replace(tmp, path)
                break
            except PermissionError as error:
                if attempt == 7:
                    raise PermissionError(
                        f"atomic parquet replace failed for {path}: {error}"
                    ) from error
                time.sleep(0.05 * (2**attempt))
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
