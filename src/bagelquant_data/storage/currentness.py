"""Optional, transactionally maintained currentness admission metadata.

This index never certifies readiness or original-byte integrity. Equal tokens
can only reuse a separately validated publication under its owner fences.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from uuid import uuid4

from bagelquant_data.core.exceptions import ConfigurationError


TABLE = "data_currentness_clock"
TRACKED = (
    "datasets", "dataset_schemas", "item_definitions", "item_dependencies",
    "node_completions", "version_commits", "version_checks", "item_builds",
    "item_completion_proofs", "item_completion_authorities", "item_pending_outputs",
    "item_completion_attestations", "frozen_inputs",
)
_ERROR = "Data currentness index is missing or damaged; run integrity.initialize_currentness_index() explicitly"
_INDEX = "CREATE INDEX data_currentness_build_sources ON item_builds(frozen_receipt_id)"
_CLOCK = (f"CREATE TABLE {TABLE}(singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
    "epoch TEXT NOT NULL,revision INTEGER NOT NULL CHECK(revision>=0),nonce TEXT NOT NULL)")


def _tables(db: sqlite3.Connection) -> set[str]:
    names = (*TRACKED, TABLE)
    return {row[0] for row in db.execute(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name IN ({','.join('?' for _ in names)})", names)}


def _triggers(table: str) -> dict[str, str]:
    result = {}
    for operation in ("INSERT", "UPDATE", "DELETE"):
        name = f"data_currentness_{table}_{operation.lower()}"
        condition = ""
        if table == "frozen_inputs":
            references = []
            for version in (("NEW",) if operation == "INSERT" else ("OLD",)
                    if operation == "DELETE" else ("OLD", "NEW")):
                references.append(f"EXISTS(SELECT 1 FROM item_builds WHERE frozen_receipt_id={version}.receipt_id)")
            condition = " WHEN " + " OR ".join(references)
        result[name] = (f"CREATE TRIGGER {name} AFTER {operation} ON {table}{condition} BEGIN "
            f"UPDATE {TABLE} SET revision=revision+1,nonce=lower(hex(randomblob(16))) WHERE singleton=1; END")
    return result


def _expected(tables: set[str]) -> dict[str, str]:
    tracked = tables.intersection(TRACKED)
    if "item_builds" not in tables:
        tracked.discard("frozen_inputs")
    return {name: sql for table in sorted(tracked) for name, sql in _triggers(table).items()}


def _bump(db: sqlite3.Connection) -> None:
    db.execute(f"UPDATE {TABLE} SET revision=revision+1,nonce=lower(hex(randomblob(16))) WHERE singleton=1")


def create_tracked_table(db: sqlite3.Connection, table: str, sql: str) -> None:
    """Attach tracking only to newly created tables in an indexed authority."""
    if table not in TRACKED:
        raise ValueError("Unknown currentness table")
    before = _tables(db)
    db.execute(sql)
    if table in before or TABLE not in before:
        return
    after = _tables(db)
    for name, statement in _expected(after).items():
        if name not in _expected(before):
            db.execute(statement)
    if table == "item_builds":
        db.execute(_INDEX)
    _bump(db)


def initialize(db: sqlite3.Connection, *, check_canceled=lambda: None) -> str:
    """Explicitly create/reset the optional index inside an admitted transaction."""
    check_canceled()
    db.execute(f"DROP TABLE IF EXISTS {TABLE}")
    db.execute(_CLOCK)
    tables = _tables(db)
    for table in TRACKED:
        for name in _triggers(table):
            db.execute(f"DROP TRIGGER IF EXISTS {name}")
    if "item_builds" in tables:
        db.execute("DROP INDEX IF EXISTS data_currentness_build_sources")
        db.execute(_INDEX)
    for statement in _expected(tables).values():
        check_canceled()
        db.execute(statement)
    db.execute(f"DELETE FROM {TABLE}")
    db.execute(f"INSERT INTO {TABLE} VALUES(1,?,0,?)", (uuid4().hex, uuid4().hex))
    check_canceled()
    return identity(db, check_canceled=check_canceled)


def identity(db: sqlite3.Connection, *, max_buffer_bytes: int = 64 * 1024 * 1024,
             check_canceled=lambda: None) -> str:
    """Read bounded clock/schema metadata in the caller's existing snapshot."""
    check_canceled()
    tables = _tables(db)
    if TABLE not in tables:
        raise ConfigurationError(_ERROR)
    if db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (TABLE,)).fetchone()[0] != _CLOCK:
        raise ConfigurationError(_ERROR)
    expected = _expected(tables)
    if "item_builds" in tables:
        expected["data_currentness_build_sources"] = _INDEX
    rows = {row[0]: row[1] for row in db.execute(
        "SELECT name,sql FROM sqlite_master WHERE name GLOB 'data_currentness_*' AND type IN ('trigger','index')")}
    if rows != expected:
        raise ConfigurationError(_ERROR)
    try:
        row = db.execute(f"SELECT epoch,revision,nonce FROM {TABLE} WHERE singleton=1").fetchone()
        if (row is None or type(row[1]) is not int or row[1] < 0
                or any(not isinstance(value, str) or len(value) != 32
                    or any(char not in "0123456789abcdef" for char in value) for value in (row[0], row[2]))):
            raise ConfigurationError(_ERROR)
    except sqlite3.Error as error:
        raise ConfigurationError(_ERROR) from error
    shapes = [tuple(item) for item in db.execute(
        f"SELECT name,sql FROM sqlite_master WHERE type='table' AND name IN ({','.join('?' for _ in TRACKED)}) ORDER BY name", TRACKED)]
    body = json.dumps(("data.currentness.state.v2", *tuple(row), shapes), separators=(",", ":")).encode()
    if len(body) * 8 > max_buffer_bytes:
        raise MemoryError("Currentness metadata row exceeds max_buffer_bytes")
    check_canceled()
    return hashlib.sha256(body).hexdigest()
