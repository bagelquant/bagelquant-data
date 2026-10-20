"""Immutable metadata decoding shares the query's transaction and connection."""
import json
import sqlite3
import zlib

import pytest

from bagelquant_data.storage.data_meta import DataMetaStore
from bagelquant_data.storage.immutable_metadata import canonical, encode_requests, save_schema


def publish(db):
    requests = [{"date": str(index), "params": {f"field{key}": [key, None, "中文"]
        for key in range(80)}} for index in range(3)]
    schema = b"registered schema bytes"
    db.execute("INSERT INTO query_test VALUES(?,?,?,?)", (
        encode_requests(db, requests), save_schema(db, schema),
        '{"rows":3}', zlib.compress(b'{"scope":"test"}')))
    return requests, schema


def metadata_store(tmp_path):
    store = DataMetaStore(tmp_path / "data.sqlite")
    with store.connect() as db:
        db.execute("CREATE TABLE query_test(request_json TEXT,schema_ref TEXT,"
            "metrics_json TEXT,request_params BLOB)")
    return store


@pytest.mark.parametrize("read_only", [False, True])
def test_reference_decoding_uses_one_connection_and_preserves_fields(tmp_path, monkeypatch, read_only):
    store = metadata_store(tmp_path)
    with store.connect() as db:
        requests, schema = publish(db)
    if read_only:
        store = DataMetaStore(tmp_path / "data.sqlite", read_only=True)
    opens = []
    original = store._new_connection

    def connect():
        connection = original()
        opens.append(connection)
        return connection

    monkeypatch.setattr(store, "_new_connection", connect)
    rows = store._rows("SELECT * FROM query_test")
    assert len(opens) == 1
    assert rows[0]["request_json"] == canonical(requests)
    assert rows[0]["schema_ipc"] == schema
    assert rows[0]["metrics"] == {"rows": 3}
    assert rows[0]["request_params"] == '{"scope":"test"}'
    with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
        opens[0].execute("SELECT 1")


def test_decoding_sees_uncommitted_blocks_and_rollback_preserves_atomicity(tmp_path, monkeypatch):
    store = metadata_store(tmp_path)
    with pytest.raises(ValueError, match="rollback"):
        with store.atomic_transaction() as db:
            requests, schema = publish(db)
            monkeypatch.setattr(store, "_new_connection",
                lambda: pytest.fail("nested decoding opened a second connection"))
            with store.connect():
                row = store._rows("SELECT * FROM query_test")[0]
                assert json.loads(row["request_json"]) == requests
                assert row["schema_ipc"] == schema
            raise ValueError("rollback")
    monkeypatch.undo()
    assert store._rows("SELECT * FROM query_test") == []


def test_existing_read_view_reuses_its_connection(tmp_path, monkeypatch):
    store = metadata_store(tmp_path)
    with store.connect() as db:
        requests, _ = publish(db)
    with store.read_view():
        monkeypatch.setattr(store, "_new_connection",
            lambda: pytest.fail("decoding escaped the read view"))
        assert json.loads(store._rows("SELECT request_json FROM query_test")[0]["request_json"]) == requests


@pytest.mark.parametrize("table,column", [
    ("immutable_metadata", "payload_json"), ("immutable_schemas", "schema_ipc")])
@pytest.mark.parametrize("missing", [False, True])
def test_reference_corruption_is_still_rejected(tmp_path, table, column, missing):
    store = metadata_store(tmp_path)
    with store.connect() as db:
        publish(db)
        if missing:
            db.execute(f"DELETE FROM {table}")
        else:
            db.execute(f"UPDATE {table} SET {column}=?", (b"corrupt" if column == "schema_ipc" else "{}",))
    with pytest.raises(RuntimeError, match="missing|checksum"):
        store._rows("SELECT * FROM query_test")
