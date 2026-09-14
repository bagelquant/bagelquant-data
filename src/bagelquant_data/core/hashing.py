"""Stable hashing helpers."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable

import polars as pl
import pyarrow as pa
from pyarrow import ipc

CONTENT_HASH_ALGORITHM = "arrow-ipc-v1"


def stable_record_hash(values: dict[str, object]) -> str:
    """Hash a record using stable JSON encoding."""

    payload = json.dumps(values, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.blake2b(payload.encode("utf-8"), digest_size=16).hexdigest()


def frame_content_hash(frame: pl.DataFrame, fields: Iterable[str] | None = None) -> str:
    """Hash logical dataframe content without materializing rows as Python objects."""
    table = canonical_arrow_table(frame if fields is None else frame.select(list(fields)))
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    digest = hashlib.blake2b(sink.getvalue(), digest_size=16).hexdigest()
    return f"{CONTENT_HASH_ALGORITHM}:{digest}"


def canonical_arrow_table(frame: pl.DataFrame) -> pa.Table:
    """Canonical logical rows and bitmap buffers shared by hashing and recovery."""
    fields = None
    selected = frame.columns if fields is None else list(fields)
    candidate = frame if fields is None else frame.select(selected)
    # Foreign Arrow producers (notably pandas via ``pl.from_pandas``) can leave
    # logically identical strings with different offset/validity buffers than a
    # Parquet round-trip.  IPC hashes those physical buffers, so rebuild only
    # variable-width strings in Polars before serialization.  This keeps the
    # fast columnar path while making the hash stable across persistence.
    string_fields = [
        name for name, dtype in candidate.schema.items() if dtype == pl.String
    ]
    if string_fields:
        candidate = candidate.with_columns(
            pl.concat_str([pl.col(name), pl.lit("")]).alias(name)
            for name in string_fields
        )
    canonical = candidate.sort(selected).rechunk()
    table = canonical.to_arrow().combine_chunks()
    table = pa.Table.from_arrays(
        [_canonical_bitmap(column.chunk(0)) for column in table.columns],
        schema=table.schema,
    ) if table.num_rows else table
    return table


def _canonical_bitmap(array: pa.Array) -> pa.Array:
    """Exclude unused validity bits from IPC's physical representation.

    Arrow leaves padding bits unspecified. Parallel Polars sorts can set them
    differently for equal logical arrays; IPC serializes the final byte as-is.
    Rebuild only that bitmap, retaining the columnar value buffers.
    """
    if pa.types.is_dictionary(array.type):
        return pa.DictionaryArray.from_arrays(
            _canonical_bitmap(array.indices), array.dictionary,
            ordered=array.type.ordered,
        )
    children = None
    if pa.types.is_struct(array.type):
        children = [_canonical_bitmap(array.field(i)) for i in range(array.type.num_fields)]
    elif pa.types.is_list(array.type) or pa.types.is_large_list(array.type) or pa.types.is_fixed_size_list(array.type) or pa.types.is_map(array.type):
        children = [_canonical_bitmap(array.values)]
    buffers = list(array.buffers()[:array.type.num_buffers])
    end = array.offset + len(array)
    if buffers and buffers[0] is not None and end % 8:
        validity = bytearray(buffers[0])
        validity[(end - 1) // 8] &= (1 << (end % 8)) - 1
        buffers[0] = pa.py_buffer(validity)
    if pa.types.is_boolean(array.type) and buffers[1] is not None:
        values = bytearray(buffers[1])
        if end % 8:
            values[(end - 1) // 8] &= (1 << (end % 8)) - 1
        if buffers[0] is not None:
            for index, validity_byte in enumerate(bytes(buffers[0])):
                values[index] &= validity_byte
        buffers[1] = pa.py_buffer(values)
    return pa.Array.from_buffers(
        array.type, len(array), buffers, null_count=array.null_count,
        offset=array.offset, children=children,
    )
