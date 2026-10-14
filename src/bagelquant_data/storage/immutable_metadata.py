"""Content-addressed, bounded metadata blocks and explicit lazy public views.

Only registered immutable metadata is represented here. Reading a root never
expands its descendants; original-byte auditing remains an explicit operation.
"""
from __future__ import annotations

import hashlib
import json
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
CREATE TABLE IF NOT EXISTS input_selection_set_entries(
 set_id TEXT NOT NULL,descriptor_key TEXT NOT NULL,ordinal INTEGER NOT NULL,
 reference_json TEXT NOT NULL,PRIMARY KEY(set_id,descriptor_key));
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
            if key == "schema_ipc" and isinstance(item, str):
                body[key] = {"schema": save_schema(db, bytes.fromhex(item)), "uppercase": item == item.upper()}
            else:
                body[key] = save(db, item)
        reference = _save(db, "mapping", body, len(body))
        if set(value) == set(BATCH_FIELDS):
            reference.update(entity="segment", **{key: value[key]
                for key in ("min_observation", "max_observation", "min_available")})
        return reference
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        leaves = [_save(db, "sequence", [save(db, item) for item in value[first:first + BLOCK_SIZE]],
            len(value[first:first + BLOCK_SIZE])) for first in range(0, len(value), BLOCK_SIZE)]
        if not leaves:
            return _save(db, "sequence", [], 0)
        while len(leaves) > 1:
            leaves = [_save(db, "branches", leaves[first:first + BLOCK_SIZE],
                sum(item["count"] for item in leaves[first:first + BLOCK_SIZE]))
                for first in range(0, len(leaves), BLOCK_SIZE)]
        return leaves[0]
    if isinstance(value, str) and len(value) > 256:
        return _save(db, "text", value, 1)
    return value


class MetadataReader:
    """One store-bound reader; views never retain a connection or validation cache."""
    def __init__(self, store=None, *, database=None):
        self.store, self.database = store, database

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
        with self.connection() as db:
            row = db.execute("SELECT kind,payload_json,item_count FROM immutable_metadata WHERE digest=?",
                (reference["ref"],)).fetchone()
        if row is None:
            raise RuntimeError("Registered immutable metadata block is missing")
        kind, payload, count = row
        if (kind != reference["kind"] or count != reference["count"]
                or hashlib.sha256((kind + ":" + payload).encode()).hexdigest() != reference["ref"]):
            raise RuntimeError("Registered immutable metadata block checksum mismatch")
        return json.loads(payload)

    def value(self, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "schema" in value:
            with self.connection() as db:
                encoded = schema_bytes(db, value["schema"]).hex()
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


def decode_requests(store, serialized: str) -> str:
    value = json.loads(serialized)
    return canonical(MetadataReader(store).value(value["request_ref"]))
