"""Stable hashing helpers."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable

import polars as pl


def stable_bucket(asset_id: str, bucket_count: int) -> int:
    """Return a deterministic asset bucket."""

    digest = hashlib.blake2b(asset_id.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, byteorder="big") % bucket_count


def stable_record_hash(values: dict[str, object]) -> str:
    """Hash a record using stable JSON encoding."""

    payload = json.dumps(values, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.blake2b(payload.encode("utf-8"), digest_size=16).hexdigest()


def frame_content_hash(frame: pl.DataFrame, fields: Iterable[str] | None = None) -> str:
    """Hash a dataframe deterministically after sorting selected fields."""

    selected = list(fields or frame.columns)
    rows = frame.select(selected).sort(selected).to_dicts()
    payload = json.dumps(rows, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.blake2b(payload.encode("utf-8"), digest_size=16).hexdigest()
