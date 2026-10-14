"""Optional SQLite lookup indexes; they never certify input currentness."""
from __future__ import annotations

import sqlite3
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from bagelquant_data.execution import ExecutionOptions

if TYPE_CHECKING:
    from bagelquant_data.storage.data_meta import DataMetaStore


# Expression spelling deliberately matches the three existing input queries.
_INDEXES = (
    ("item_builds_record_lookup", "item_builds",
     "CREATE INDEX item_builds_record_lookup on item_builds(name,definition_hash,id desc)"),
    ("frozen_inputs_check_lookup", "frozen_inputs",
     "CREATE INDEX frozen_inputs_check_lookup on frozen_inputs(receipt_id,cast(json_extract(payload_json,'$.max_check_id') as integer))"),
    ("version_checks_record_lookup", "version_checks",
     "CREATE INDEX version_checks_record_lookup on version_checks(source,dataset,id)"),
)


def _existing(db: sqlite3.Connection, *, strict: bool) -> set[str]:
    result: set[str] = set()
    for name, table, sql in _INDEXES:
        row = db.execute("select type,tbl_name,sql from sqlite_master where name=?", (name,)).fetchone()
        if row is None:
            continue
        result.add(name)
        if strict and (row["type"] != "index" or row["tbl_name"] != table
                       or (row["sql"] or "").strip().rstrip(";") != sql):
            raise RuntimeError(f"Record lookup index {name} has a conflicting definition")
    return result


def _admit(db: sqlite3.Connection, missing: set[str], budget: int) -> None:
    # octet_length(TEXT) uses SQLite's byte-count header without exporting JSON.
    # Bounds are for admitted cells/parser work; SQLite's allocator is not a
    # process-wide hard RSS limit. Sorts spill to FILE and page cache is capped.
    admitted = budget // 32
    if "frozen_inputs_check_lookup" in missing:
        row = db.execute("select coalesce(max(octet_length(payload_json)),0) as body,"
                         "coalesce(max(octet_length(receipt_id)),0) as key from frozen_inputs").fetchone()
        if row["body"] > admitted or row["key"] > admitted:
            raise MemoryError("Frozen metadata exceeds record-lookup index admission; increase max_buffer_bytes")
    if "item_builds_record_lookup" in missing:
        row = db.execute("select coalesce(max(octet_length(name)+octet_length(definition_hash)),0) as key from item_builds").fetchone()
        if row["key"] > admitted:
            raise MemoryError("Item build key exceeds record-lookup index admission; increase max_buffer_bytes")
    if "version_checks_record_lookup" in missing:
        row = db.execute("select coalesce(max(octet_length(source)+octet_length(dataset)),0) as key from version_checks").fetchone()
        if row["key"] > admitted:
            raise MemoryError("Version check key exceeds record-lookup index admission; increase max_buffer_bytes")


def install_record_lookups(db: sqlite3.Connection, *, config: ExecutionOptions,
                           check_canceled: Callable[[], None], empty_only: bool = False) -> dict[str, Any]:
    """Atomically install owner-named indexes on an unborrowed connection.

    Empty-only initialization never scans or indexes populated legacy tables.
    The explicit operation admits original metadata, supports SQL cancellation,
    and rejects a same-named index with a different definition.
    """
    if db.in_transaction:
        if empty_only:
            return {"version": 1, "indexes": {}}
        raise RuntimeError("Record-lookup maintenance requires its own transaction")
    check_canceled()
    existing = _existing(db, strict=not empty_only)
    pending = [(name, table, sql) for name, table, sql in _INDEXES if name not in existing]
    if empty_only:
        pending = [(name, table, sql) for name, table, sql in pending
                   if db.execute(f"select 1 from {table} limit 1").fetchone() is None]
    if not pending:
        check_canceled()
        return {"version": 1, "indexes": {name: "existing" for name, _, _ in _INDEXES if name in existing}}
    budget = config.max_buffer_bytes
    if budget < 16 * 1024:
        raise MemoryError("Record-lookup maintenance requires at least 16 KiB of admitted buffer")
    saved = {key: db.execute(f"pragma {key}").fetchone()[0]
             for key in ("temp_store", "cache_size", "threads", "cache_spill")}
    saved_length = db.getlimit(sqlite3.SQLITE_LIMIT_LENGTH)
    canceled: BaseException | None = None

    def poll() -> int:
        nonlocal canceled
        try:
            check_canceled()
        except BaseException as error:
            canceled = error
            return 1
        return 0

    try:
        db.execute("pragma temp_store=FILE")
        db.execute(f"pragma cache_size=-{max(1, budget // 8 // 1024)}")
        db.execute("pragma threads=0")
        db.execute("pragma cache_spill=ON")
        db.set_progress_handler(poll, 1000)
        db.execute("begin immediate")
        # A writer may have populated an empty table or installed an index since
        # the initial probe. Recheck under the same global writer fence.
        existing = _existing(db, strict=not empty_only)
        pending = [(name, table, sql) for name, table, sql in pending if name not in existing]
        if empty_only:
            pending = [(name, table, sql) for name, table, sql in pending
                       if db.execute(f"select 1 from {table} limit 1").fetchone() is None]
        missing = {name for name, _, _ in pending}
        if not empty_only:
            _admit(db, missing, budget)
        check_canceled()
        db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, min(saved_length, budget // 16))
        created: set[str] = set()
        for name, _, sql in pending:
            check_canceled()
            db.execute(sql)
            created.add(name)
        check_canceled()
        db.commit()
        return {"version": 1, "indexes": {name: "created" if name in created else "existing"
                for name, _, _ in _INDEXES if name in existing or name in created}}
    except sqlite3.Error:
        if canceled is not None:
            raise canceled
        raise
    finally:
        db.set_progress_handler(None, 0)
        if db.in_transaction:
            db.rollback()
        db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, saved_length)
        for key, value in saved.items():
            db.execute(f"pragma {key}={value}")


def initialize_empty_record_lookups(store: DataMetaStore) -> None:
    """Cheap writable initialization; populated missing indexes stay missing."""
    if store.read_only or getattr(store._thread_state, "writer_connection", None) is not None:
        return
    with store.connect() as db:
        install_record_lookups(db, config=ExecutionOptions(), check_canceled=lambda: None, empty_only=True)
