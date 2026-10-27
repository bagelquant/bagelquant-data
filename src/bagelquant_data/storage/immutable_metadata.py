"""Content-addressed, bounded metadata blocks and explicit lazy public views.

Only registered immutable metadata is represented here. Reading a root never
expands its descendants; original-byte auditing remains an explicit operation.
"""
from __future__ import annotations

import hashlib
import json
from collections import OrderedDict
from collections.abc import Callable
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from typing import Any


BLOCK_SIZE = 64
BATCH_FIELDS = ("commit_seq", "partition_path", "content_hash", "row_count", "mode", "spec_hash", "pit_date", "input_receipt_id", "baseline", "min_available", "max_available", "min_observation", "max_observation", "generation_path", "field_changes_json", "key_identity")
SCHEMA = """
CREATE TABLE IF NOT EXISTS immutable_metadata(
 digest TEXT PRIMARY KEY,kind TEXT NOT NULL,payload_json TEXT NOT NULL,
 item_count INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS immutable_schemas(
 digest TEXT PRIMARY KEY,schema_ipc BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS input_selection_sets(
 id TEXT PRIMARY KEY,digest TEXT NOT NULL,source_id TEXT NOT NULL,
 source_digest TEXT NOT NULL,root_ref TEXT NOT NULL,item_count INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS input_selection_chunk_entries(
 block_ref TEXT NOT NULL,descriptor_key TEXT NOT NULL,ordinal INTEGER NOT NULL,
 reference_json TEXT NOT NULL,PRIMARY KEY(block_ref,descriptor_key));
"""


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
        default=lambda item: dict(item) if isinstance(item, Mapping) else
        list(item) if isinstance(item, Sequence) and not isinstance(item, (str, bytes)) else str(item))


def save_schema(db, payload: bytes) -> str:
    digest = hashlib.sha256(payload).hexdigest()
    db.execute("INSERT OR IGNORE INTO immutable_schemas VALUES(?,?)", (digest, payload))
    return digest


def schema_bytes(db, digest: str) -> bytes:
    row = db.execute("SELECT schema_ipc FROM immutable_schemas WHERE digest=?", (digest,)).fetchone()
    if row is None:
        raise RuntimeError("Registered immutable schema is missing")
    payload = bytes(row[0])
    if hashlib.sha256(payload).hexdigest() != digest:
        raise RuntimeError("Registered immutable schema checksum mismatch")
    return payload


def _save(db, kind: str, payload: Any, count: int) -> dict[str, Any]:
    serialized = canonical(payload)
    digest = hashlib.sha256((kind + ":" + serialized).encode()).hexdigest()
    db.execute("INSERT OR IGNORE INTO immutable_metadata VALUES(?,?,?,?)",
        (digest, kind, serialized, count))
    reference = {"ref": digest, "kind": kind, "count": count}
    if kind in {"sequence", "branches"} and payload and all(isinstance(item, dict) and item.get("min_observation") is not None and item.get("max_observation") is not None for item in payload):
        reference.update(min_observation=min(item["min_observation"] for item in payload),
            max_observation=max(item["max_observation"] for item in payload),
            min_available=min(item["min_available"] for item in payload) if all(item.get("min_available") is not None for item in payload) else None)
    return reference


def save(db, value: Any) -> Any:
    """Store each immutable dictionary/sequence block once, with scalar leaves."""
    if isinstance(value, Mapping):
        body = {}
        for key, item in sorted(value.items()):
            schema_reference = None
            if key == "schema_ipc" and isinstance(item, str):
                try:
                    payload = bytes.fromhex(item)
                except ValueError:
                    pass  # Arbitrary authored metadata can use the same key.
                else:
                    encoded = payload.hex()
                    if item in (encoded, encoded.upper()):
                        schema_reference = {"schema": save_schema(db, payload), "uppercase": item == item.upper()}
            body[key] = save(db, item) if schema_reference is None else schema_reference
        reference = _save(db, "mapping", body, len(body))
        if set(value) == set(BATCH_FIELDS):
            reference.update(entity="segment", **{key: value[key]
                for key in ("min_observation", "max_observation", "min_available")})
        return reference
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return _save_sequence(db, value)
    if isinstance(value, str) and len(value) > 256:
        return _save(db, "text", value, 1)
    return value


def _save_sequence(db, values, *, run_ends=None):
    """Chunk neutral contiguous runs without shifting later runs' leaf blocks."""
    leaves, start = [], 0
    for end in (run_ends if run_ends is not None else (len(values),)):
        if type(end) is not int or not start < end <= len(values):
            if not values and end == 0:
                continue
            raise ValueError("Immutable sequence run bounds are invalid")
        for first in range(start, end, BLOCK_SIZE):
            chunk = values[first:min(end, first + BLOCK_SIZE)]
            leaves.append(_save(db, "sequence", [save(db, item) for item in chunk], len(chunk)))
        start = end
    if start != len(values):
        raise ValueError("Immutable sequence run bounds are incomplete")
    if not leaves:
        return _save(db, "sequence", [], 0)
    while len(leaves) > 1:
        leaves = [_save(db, "branches", leaves[first:first + BLOCK_SIZE],
            sum(item["count"] for item in leaves[first:first + BLOCK_SIZE]))
            for first in range(0, len(leaves), BLOCK_SIZE)]
    return leaves[0]


def save_selection_membership(db, members: Sequence[Mapping[str, Any]], *, run_ends=None):
    """Share immutable member chunks and their bounded indexed manifest."""
    sequence = _save_sequence(db, members, run_ends=run_ends)
    reader = MetadataReader(database=db)
    blocks = []

    def visit(reference, start):
        if reference["kind"] == "sequence":
            blocks.append({"ref": reference["ref"], "start": start, "count": reference["count"]})
            return
        for child in reader.block(reference):
            visit(child, start)
            start += child["count"]

    visit(sequence, 0)
    return _save(db, "selection_membership", {"schema": "selection.membership.v1",
        "members": sequence, "blocks": save(db, [{"ref": block["ref"], "count": block["count"]}
            for block in blocks])}, len(members)), blocks


class MetadataMemo:
    """Authenticated decoded blocks retained only by one admitted read view."""

    def __init__(self, maximum_bytes: int):
        self.maximum_bytes = maximum_bytes
        self.entries: OrderedDict[tuple, tuple[Any, int]] = OrderedDict()
        self.resident_bytes = self.peak_bytes = self.reads = self.hits = 0
        self.enabled = True

    def get(self, key):
        entry = self.entries.pop(key, None)
        if entry is None:
            return None
        self.entries[key] = entry
        self.hits += 1
        return entry

    def put(self, key, value, charge):
        if charge > self.maximum_bytes:
            return
        while self.resident_bytes + charge > self.maximum_bytes:
            _, (_, previous) = self.entries.popitem(last=False)
            self.resident_bytes -= previous
        self.entries[key] = (value, charge)
        self.resident_bytes += charge
        self.peak_bytes = max(self.peak_bytes, self.resident_bytes)

    def clear(self):
        self.entries.clear()
        self.resident_bytes = 0

    def shrink(self, maximum_bytes):
        self.maximum_bytes = min(self.maximum_bytes, maximum_bytes)
        while self.resident_bytes > self.maximum_bytes:
            _, (_, charge) = self.entries.popitem(last=False)
            self.resident_bytes -= charge


class MetadataReader:
    """Resolve originals through the current store view and its optional memo."""
    def __init__(self, store=None, *, database=None,
                 memo: Callable[[], MetadataMemo | None] | None = None):
        self.store, self.database = store, database
        self.memo = memo

    @contextmanager
    def connection(self):
        if self.database is not None:
            yield self.database
        else:
            if self.store is None:
                raise RuntimeError("Immutable metadata reader requires an owning store or active database")
            with self.store.connect() as db:
                yield db

    def block(self, reference: Mapping[str, Any]) -> Any:
        memo = None if self.memo is None else self.memo()
        key = (reference["ref"], reference["kind"], reference["count"])
        cached = None if memo is None or not memo.enabled else memo.get(key)
        if cached is not None:
            return cached[0]
        with self.connection() as db:
            row = db.execute("SELECT kind,payload_json,item_count FROM immutable_metadata WHERE digest=?",
                (reference["ref"],)).fetchone()
        if row is None:
            raise RuntimeError("Registered immutable metadata block is missing")
        kind, payload, count = row
        if (kind != reference["kind"] or count != reference["count"]
                or hashlib.sha256((kind + ":" + payload).encode()).hexdigest() != reference["ref"]):
            raise RuntimeError("Registered immutable metadata block checksum mismatch")
        value = json.loads(payload)
        if memo is not None and memo.enabled:
            memo.reads += 1
            # Include parsed Python cells, references and the LRU entry, rather
            # than charging only the serialized body. Oversize blocks bypass it.
            memo.put(key, value, 256 + 8 * len(payload.encode()) + 64 * count)
        return value

    def value(self, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "schema" in value:
            memo = None if self.memo is None else self.memo()
            key = (value["schema"], "schema")
            cached = None if memo is None or not memo.enabled else memo.get(key)
            if cached is not None:
                encoded = cached[0]
                return encoded.upper() if value["uppercase"] else encoded
            with self.connection() as db:
                payload = schema_bytes(db, value["schema"])
            encoded = payload.hex()
            if memo is not None and memo.enabled:
                memo.put(key, encoded, 256 + 8 * len(encoded))
            return encoded.upper() if value["uppercase"] else encoded
        if value["kind"] == "mapping":
            return LazyMetadataMapping(self, value)
        if value["kind"] in {"sequence", "branches"}:
            return LazyMetadataSequence(self, value)
        return self.block(value)

    def audit(self, reference: Mapping[str, Any], seen: set[str]) -> None:
        """Audit every registered descendant once within an explicit invocation."""
        if reference["ref"] in seen:
            return
        body = self.block(reference)
        seen.add(reference["ref"])
        if reference["kind"] == "text":
            return
        values = body.values() if isinstance(body, dict) else body
        for value in values:
            if isinstance(value, dict) and "ref" in value:
                self.audit(value, seen)
            elif isinstance(value, dict) and "schema" in value:
                with self.connection() as db:
                    schema_bytes(db, value["schema"])


class FrozenEvidenceMapping(Mapping[str, Any]):
    """Immutable alias references with lazy views and explicit materialized copying."""
    def __init__(self, reader: MetadataReader, references: Mapping[str, Any]):
        self.reader, self.references = reader, dict(references)
        self._views: dict[str, Any] = {}

    def __len__(self) -> int:
        return len(self.references)

    def __iter__(self) -> Iterator[str]:
        return iter(self.references)

    def __getitem__(self, key: str) -> Any:
        if key not in self._views:
            self._views[key] = self.reader.value(self.references[key])
        return self._views[key]

    def __deepcopy__(self, memo):
        from copy import deepcopy
        return {key: deepcopy(self[key], memo) for key in self}


class LazyMetadataMapping(Mapping[str, Any]):
    """Explicit lazy evidence mapping; only the requested block is decoded."""
    def __init__(self, reader: MetadataReader, reference: Mapping[str, Any]):
        self.reader, self.reference = reader, dict(reference)
        self._body = None

    def _contents(self):
        if self.reader.memo is not None and self.reader.memo() is not None:
            # The context owns the bounded decoded body. A retained lazy view
            # must not pin it after eviction, exit or a writer-generation change.
            return self.reader.block(self.reference)
        if self._body is None:
            self._body = self.reader.block(self.reference)
        return self._body

    def __len__(self) -> int:
        return self.reference["count"]

    def __iter__(self) -> Iterator[str]:
        return iter(self._contents())

    def __getitem__(self, key: str) -> Any:
        return self.reader.value(self._contents()[key])

    def __deepcopy__(self, memo):
        from copy import deepcopy
        return {key: deepcopy(self[key], memo) for key in self}


class LazyMetadataSequence(Sequence[Any]):
    """Persistent bounded sequence; length does not load historical blocks."""
    def __init__(self, reader: MetadataReader, reference: Mapping[str, Any]):
        self.reader, self.reference = reader, dict(reference)

    def __len__(self) -> int:
        return self.reference["count"]

    def __iter__(self) -> Iterator[Any]:
        body = self.reader.block(self.reference)
        if self.reference["kind"] == "branches":
            for reference in body:
                yield from LazyMetadataSequence(self.reader, reference)
        else:
            for item in body:
                yield self.reader.value(item)

    def selected(self, lower=None, upper=None, cutoff=None) -> Iterator[Any]:
        """Prune registered reference blocks before dereferencing old headers."""
        def intersects(reference):
            return not (lower is not None and reference.get("max_observation") is not None and reference["max_observation"] < str(lower)
                or upper is not None and reference.get("min_observation") is not None and reference["min_observation"] > str(upper)
                or cutoff is not None and reference.get("min_available") is not None and reference["min_available"] > str(cutoff))
        if not intersects(self.reference):
            return
        for item in self.reader.block(self.reference):
            if self.reference["kind"] == "branches":
                yield from LazyMetadataSequence(self.reader, item).selected(lower, upper, cutoff)
            elif not isinstance(item, dict) or intersects(item):
                yield self.reader.value(item)

    def __getitem__(self, index):
        if isinstance(index, slice):
            return tuple(self)[index]
        if index < 0:
            index += len(self)
        if not 0 <= index < len(self):
            raise IndexError(index)
        body = self.reader.block(self.reference)
        if self.reference["kind"] == "sequence":
            return self.reader.value(body[index])
        for reference in body:
            if index < reference["count"]:
                return LazyMetadataSequence(self.reader, reference)[index]
            index -= reference["count"]
        raise IndexError(index)

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, Sequence) and len(self) == len(other) and all(a == b for a, b in zip(self, other))

    def __deepcopy__(self, memo):
        from copy import deepcopy
        return tuple(deepcopy(item, memo) for item in self)


def encode_requests(db, requests: Sequence[Mapping[str, Any]]) -> str:
    """Share common request fields/templates and immutable per-scope deltas."""
    return canonical({"request_ref": save(db, requests)})


def decode_requests(database, serialized: str) -> str:
    value = json.loads(serialized)
    return canonical(MetadataReader(database=database).value(value["request_ref"]))
