"""Stable hashing helpers."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable

import polars as pl
import pyarrow as pa
from pyarrow import ipc

CONTENT_HASH_ALGORITHM = "arrow-ipc-v1"


def stable_bucket(asset_id: str, bucket_count: int) -> int:
    """Return a deterministic asset bucket."""

    digest = hashlib.blake2b(asset_id.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, byteorder="big") % bucket_count


def stable_record_hash(values: dict[str, object]) -> str:
    """Hash a record using stable JSON encoding."""

    payload = json.dumps(values, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.blake2b(payload.encode("utf-8"), digest_size=16).hexdigest()


def frame_content_hash(frame: pl.DataFrame, fields: Iterable[str] | None = None) -> str:
    """Hash logical dataframe content without materializing rows as Python objects."""

    selected = frame.columns if fields is None else list(fields)
    candidate = frame if fields is None else frame.select(selected)
    canonical = candidate.sort(selected).rechunk()
    table = canonical.to_arrow().combine_chunks()
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    digest = hashlib.blake2b(sink.getvalue(), digest_size=16).hexdigest()
    return f"{CONTENT_HASH_ALGORITHM}:{digest}"
