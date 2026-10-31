"""Currentness token work is independent of retained historical row counts."""
from contextlib import closing
import sqlite3

import pytest

from bagelquant_data import ConfigurationError, DataLake
from bagelquant_data.storage import currentness


def lake_at(root):
    return DataLake.open(data_meta_path=root / "meta.sqlite", lake_path=root / "lake")


def remove_index(db):
    for table in currentness.TRACKED:
        for name in currentness._triggers(table):
            db.execute(f"DROP TRIGGER IF EXISTS {name}")
    db.execute("DROP INDEX IF EXISTS data_currentness_build_sources")
    db.execute(f"DROP TABLE {currentness.TABLE}")


@pytest.mark.parametrize("table", [name for name in currentness.TRACKED if name != "frozen_inputs"])
def test_all_watched_table_insert_update_delete_and_rollback_are_tracked(table):
    with closing(sqlite3.connect(":memory:")) as db:
        for name in currentness.TRACKED:
            columns = "id TEXT PRIMARY KEY,value TEXT"
            if name == "item_builds":
                columns += ",frozen_receipt_id TEXT"
            if name == "frozen_inputs":
                columns = "receipt_id TEXT PRIMARY KEY,digest TEXT"
            db.execute(f"CREATE TABLE {name}({columns})")
        currentness.initialize(db)
        previous = currentness.identity(db)
        for sql in (f"INSERT INTO {table}(id,value) VALUES('row','one')",
                    f"UPDATE {table} SET value='two' WHERE id='row'",
                    f"DELETE FROM {table} WHERE id='row'"):
            db.execute("SAVEPOINT changed")
            db.execute(sql)
            assert currentness.identity(db) != previous
            db.execute("ROLLBACK TO changed")
            assert currentness.identity(db) == previous
            db.execute("RELEASE changed")
            db.execute(sql)
            previous = currentness.identity(db)


def test_referenced_source_changes_track_but_unrelated_sources_do_not(tmp_path):
    lake = lake_at(tmp_path)
    with lake._data_meta.atomic_transaction() as db:
        db.execute("INSERT INTO frozen_inputs VALUES('source','{}','digest','now')")
        db.execute("INSERT INTO item_builds VALUES(1,'item','definition','dependency','2020-01-01',"
            "'2020-01-01',0,NULL,'success','now','source')")
        before = currentness.identity(db)
        db.execute("INSERT INTO frozen_inputs VALUES('unrelated','{}','other','now')")
        assert currentness.identity(db) == before
        for sql in ("UPDATE frozen_inputs SET digest='changed' WHERE receipt_id='source'",
                    "DELETE FROM frozen_inputs WHERE receipt_id='source'"):
            db.execute("SAVEPOINT changed")
            db.execute(sql)
            assert currentness.identity(db) != before
            db.execute("ROLLBACK TO changed")
        db.execute("DELETE FROM frozen_inputs WHERE receipt_id='source'")
        missing = currentness.identity(db)
        db.execute("INSERT INTO frozen_inputs VALUES('source','{}','digest','now')")
        assert currentness.identity(db) != missing


def test_new_completion_tables_attach_tracking_but_dropped_tables_change_identity(tmp_path):
    lake = lake_at(tmp_path)
    before = lake.inputs.currentness_identity()
    with lake._data_meta.connect() as db:
        currentness.create_tracked_table(db, "item_pending_outputs",
            "CREATE TABLE item_pending_outputs(build_id INTEGER PRIMARY KEY,payload_json TEXT,digest TEXT)")
        created = currentness.identity(db)
        assert created != before
        db.execute("INSERT INTO item_pending_outputs VALUES(1,'{}','digest')")
        assert currentness.identity(db) != created
    before_drop = lake.inputs.currentness_identity()
    with lake._data_meta.connect() as db:
        db.execute("DROP TABLE item_pending_outputs")
    assert lake.inputs.currentness_identity() != before_drop


@pytest.mark.parametrize("damage", ["missing", "trigger", "trigger_body", "clock"])
def test_existing_lakes_need_explicit_index_maintenance(tmp_path, damage):
    lake = lake_at(tmp_path)
    with lake._data_meta.connect() as db:
        if damage == "missing":
            remove_index(db)
        elif damage in {"trigger", "trigger_body"}:
            db.execute("DROP TRIGGER data_currentness_datasets_update")
            if damage == "trigger_body":
                db.execute("CREATE TRIGGER data_currentness_datasets_update AFTER UPDATE ON datasets BEGIN SELECT 1; END")
        else:
            db.execute(f"DELETE FROM {currentness.TABLE}")
    reopened = lake_at(tmp_path)
    with pytest.raises(ConfigurationError, match="initialize_currentness_index"):
        reopened.inputs.currentness_identity()
    result = reopened.integrity.initialize_currentness_index()
    assert result["identity"] == reopened.inputs.currentness_identity()
    with DataLake.open(data_meta_path=lake.data_meta_path, lake_path=lake.lake_path, read_only=True) as reader:
        with pytest.raises(PermissionError):
            reader.integrity.initialize_currentness_index()


def test_explicit_reinitialization_cancellation_rolls_back_and_preserves_token(tmp_path):
    lake = lake_at(tmp_path)
    before = lake.inputs.currentness_identity()
    calls = 0
    def cancel():
        nonlocal calls
        calls += 1
        if calls == 5:
            raise InterruptedError("cancel maintenance")
    with pytest.raises(InterruptedError, match="cancel maintenance"):
        lake.integrity.initialize_currentness_index(check_canceled=cancel)
    assert lake.inputs.currentness_identity() == before


def test_identity_work_does_not_grow_with_retained_checks(tmp_path):
    lake = lake_at(tmp_path)
    def steps():
        with lake._data_meta.read_view() as db:
            count = [0]
            def instruction():
                count[0] += 1
                return 0
            db.set_progress_handler(instruction, 1)
            try:
                lake.inputs.currentness_identity()
            finally:
                db.set_progress_handler(None, 0)
            return count[0]
    before = steps()
    with lake._data_meta.connect() as db:
        db.executemany("INSERT INTO version_checks(source,dataset,run_id,checked_at,pit_date,baseline,request_json,row_count)"
            " VALUES('custom','input',?,'now','2020-01-01',0,'[]',0)", ((str(index),) for index in range(2000)))
    assert steps() == before


def test_independent_copies_cannot_alias_after_equal_numbers_of_mutations(tmp_path):
    source = lake_at(tmp_path / "source")
    source.integrity.snapshot(data_meta_path=tmp_path / "copy/meta.sqlite", lake_path=tmp_path / "copy/lake")
    copied = lake_at(tmp_path / "copy")
    assert copied.inputs.currentness_identity() == source.inputs.currentness_identity()
    for lake, target in ((source, "2020-01-01"), (copied, "2020-02-01")):
        with lake._data_meta.connect() as db:
            db.execute("INSERT INTO version_checks(source,dataset,run_id,checked_at,pit_date,baseline,request_json,row_count)"
                " VALUES('custom','input','run','now',?,0,'[]',0)", (target,))
    assert copied.inputs.currentness_identity() != source.inputs.currentness_identity()
