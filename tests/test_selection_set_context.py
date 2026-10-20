"""Selection inventories reuse only the current finite metadata transaction."""
from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
import json
import sqlite3

import pytest

from bagelquant_data import ExecutionOptions
from bagelquant_data.storage import immutable_metadata
from test_selection_descriptors import DAY, forbid_values, setup


@pytest.fixture
def inventory(tmp_path):
    lake, _, source, scope, _ = setup(tmp_path)
    scopes = [replace(scope, coordinates=None, fields=fields, as_of=DAY + timedelta(days=offset))
        for fields in (("value",), ("value", "available_date"), ("time", "asset_id", "value"))
        for offset in range(2, 9)]
    descriptors = tuple(value for first in range(0, len(scopes), 16)
        for value in lake.inputs.prepare_range_selections(source, scopes[first:first + 16]))
    reference = lake.inputs.register_selection_set(source, descriptors)
    return lake, source, descriptors, reference


@pytest.mark.parametrize("selected", [False, True])
def test_selected_inventory_has_one_connection_and_plain_results(inventory, monkeypatch, selected):
    lake, _, descriptors, reference = inventory
    wanted = descriptors[::3] if selected else descriptors
    keys = [lake.inputs.selection_descriptor_identity(value) for value in wanted] if selected else None
    opened = []
    connect = sqlite3.connect

    def observed(*args, **kwargs):
        db = connect(*args, **kwargs)
        opened.append(db)
        return db

    forbid_values(monkeypatch)
    monkeypatch.setattr(sqlite3, "connect", observed)
    result = lake.inputs.read_selection_set(reference, keys=keys)
    assert result == wanted
    assert json.loads(json.dumps(result)) == list(wanted)
    assert len(opened) == 1
    assert lake.inputs._read_context() is None
    with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
        opened[0].execute("SELECT 1")


def test_registration_shares_validation_context_and_closes_before_write(inventory, monkeypatch):
    lake, source, descriptors, reference = inventory
    resolve = lake.inputs.resolve_selection_descriptors
    connect = lake._data_meta.connect
    views, publication_views = [], []

    def observed(*args, **kwargs):
        context = lake.inputs._read_context()
        assert context is not None
        views.append(context)
        return resolve(*args, **kwargs)

    @contextmanager
    def connection():
        with connect() as db:
            # The publication transaction is distinct from its validation.
            def trace(statement):
                if statement.lower().startswith("begin immediate"):
                    publication_views.append(lake.inputs._read_context())
            db.set_trace_callback(trace)
            try:
                yield db
            finally:
                db.set_trace_callback(None)

    forbid_values(monkeypatch)
    monkeypatch.setattr(lake.inputs, "resolve_selection_descriptors", observed)
    monkeypatch.setattr(lake._data_meta, "connect", connection)
    assert lake.inputs.register_selection_set(source, descriptors) == reference
    assert len(views) == 2 and views[0] is views[1]
    assert views[0].closed and lake.inputs._read_context() is None
    assert publication_views == [None]


def test_inventory_reuses_existing_reader_and_publication_guard(inventory, monkeypatch):
    lake, source, descriptors, reference = inventory
    for context in (lake.inputs.read_context, lake.inputs.publication_guard):
        with context(source):
            active = lake.inputs._read_context()
            with monkeypatch.context() as guard:
                guard.setattr(sqlite3, "connect", lambda *_a, **_k: pytest.fail("escaped current reader"))
                assert lake.inputs.read_selection_set(reference) == descriptors
            assert lake.inputs._read_context() is active
        assert lake.inputs._read_context() is None


def test_inventory_reads_uncommitted_writer_rows_and_preserves_rollback(inventory, monkeypatch):
    lake, _, descriptors, reference = inventory
    tables = ("input_selection_sets", "input_selection_chunk_entries")
    with lake._data_meta.connect() as db:
        retained = {table: db.execute(f"SELECT * FROM {table}").fetchall() for table in tables}
        for table in tables:
            db.execute(f"DELETE FROM {table}")
    with pytest.raises(ValueError, match="rollback"):
        with lake._data_meta.atomic_transaction() as db:
            for table, rows in retained.items():
                for row in rows:
                    db.execute(f"INSERT INTO {table} VALUES ({','.join('?' for _ in row)})", tuple(row))
            with monkeypatch.context() as guard:
                guard.setattr(sqlite3, "connect", lambda *_a, **_k: pytest.fail("escaped uncommitted writer"))
                assert lake.inputs.read_selection_set(reference) == descriptors
            raise ValueError("rollback")
    with pytest.raises(KeyError, match="Unknown registered selection set"):
        lake.inputs.read_selection_set(reference)


@pytest.mark.parametrize("unknown", [False, True])
def test_inventory_validates_header_before_loading_claimed_source(inventory, monkeypatch, unknown):
    lake, _, _, reference = inventory
    invalid = {**reference, "source": {"id": "not-a-source", "digest": "not-a-digest"}}
    if unknown:
        invalid["id"] = "not-a-set"
    monkeypatch.setattr(lake.inputs, "read_context",
        lambda *_a, **_k: pytest.fail("unvalidated header entered an Inputs context"))
    with pytest.raises(KeyError if unknown else RuntimeError,
            match="Unknown registered selection set" if unknown else "identity or original Source changed"):
        lake.inputs.read_selection_set(invalid)
    assert lake.inputs._read_context() is None


@pytest.mark.parametrize("completed_groups", [1, 2])
def test_registration_cancellation_preserves_inventory_and_releases_context(
        inventory, monkeypatch, completed_groups):
    lake, source, descriptors, reference = inventory
    with lake._data_meta.connect() as db:
        db.execute("DELETE FROM input_selection_sets WHERE id=?", (reference["id"],))
    resolve = lake.inputs.resolve_selection_descriptors
    completed = 0

    def observed(*args, **kwargs):
        nonlocal completed
        result = resolve(*args, **kwargs)
        completed += 1
        return result

    def canceled():
        if completed >= completed_groups:
            raise RuntimeError("cancel registration")

    monkeypatch.setattr(lake.inputs, "resolve_selection_descriptors", observed)
    with pytest.raises(RuntimeError, match="cancel registration"):
        lake.inputs.register_selection_set(source, descriptors, check_canceled=canceled)
    assert lake.inputs._read_context() is None
    with pytest.raises(KeyError, match="Unknown registered selection set"):
        lake.inputs.describe_selection_set(reference)
    assert lake.inputs.register_selection_set(source, descriptors) == reference


def test_inventory_releases_reader_after_cancellation_and_memory_failure(inventory, monkeypatch):
    lake, _, descriptors, reference = inventory
    block = immutable_metadata.MetadataReader.block
    visited = 0

    def observed(*args, **kwargs):
        nonlocal visited
        visited += 1
        return block(*args, **kwargs)

    class Canceled(Exception):
        pass

    def canceled():
        if visited >= 10:
            raise Canceled("inventory canceled")

    monkeypatch.setattr(immutable_metadata.MetadataReader, "block", observed)
    with pytest.raises(Canceled, match="inventory canceled"):
        lake.inputs.read_selection_set(reference, check_canceled=canceled)
    assert lake.inputs._read_context() is None
    with pytest.raises(MemoryError, match="max_buffer_bytes"):
        lake.inputs.read_selection_set(reference, config=ExecutionOptions(max_buffer_bytes=1024))
    assert lake.inputs._read_context() is None
    assert lake.inputs.read_selection_set(reference) == descriptors


def test_inventory_has_no_cross_call_proof_cache(inventory):
    lake, _, descriptors, reference = inventory
    assert lake.inputs.read_selection_set(reference) == descriptors
    with lake._data_meta.connect() as db:
        row = db.execute("SELECT reference_json FROM input_selection_chunk_entries LIMIT 1").fetchone()
        digest = json.loads(row[0])["ref"]
        db.execute("UPDATE immutable_metadata SET payload_json='{}' WHERE digest=?", (digest,))
    with pytest.raises(RuntimeError, match="checksum"):
        lake.inputs.read_selection_set(reference)
