"""Parquet scan planning."""

from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import date, datetime
from pathlib import Path

from bagelquant_data.core.types import DateLike
from bagelquant_data.storage.metadata import MetadataStore


def manifest_rows(
    metadata: MetadataStore,
    source: str,
    dataset: str,
    *,
    start: DateLike | None = None,
    end: DateLike | None = None,
    buckets: Iterable[int] | None = None,
) -> list[dict[str, object]]:
    """Return manifest rows overlapping the requested physical partitions."""

    lower = None if start is None else _date_value(start).isoformat()
    upper = None if end is None else _date_value(end).isoformat()
    selected_buckets = None if buckets is None else set(buckets)
    rows = []
    for row in metadata.manifest(source, dataset):
        if (
            lower is not None
            and row.get("max_time") is not None
            and str(row["max_time"]) < lower
        ):
            continue
        if (
            upper is not None
            and row.get("min_time") is not None
            and str(row["min_time"]) > upper
        ):
            continue
        partition_values = row.get("partition_values")
        if isinstance(partition_values, str):
            partition_values = json.loads(partition_values)
        if (
            selected_buckets is not None
            and isinstance(partition_values, dict)
            and "bucket" in partition_values
            and int(partition_values["bucket"]) not in selected_buckets
        ):
            continue
        rows.append(row)
    return rows


def manifest_paths(metadata: MetadataStore, source: str, dataset: str) -> list[Path]:
    """Return all known manifest paths for compatibility."""

    return [
        Path(str(row["partition_path"]))
        for row in manifest_rows(metadata, source, dataset)
    ]


def _date_value(value: DateLike) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).split("T", maxsplit=1)[0]
    if len(text) == 8 and text.isdigit():
        return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
    return date.fromisoformat(text)
