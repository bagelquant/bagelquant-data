"""Connection reuse must retain finite transaction and publication fences."""
import sqlite3

import pytest

from bagelquant_data.core import ConfigurationError
from bagelquant_data.storage.data_meta import DataMetaStore


def test_nested_helpers_cannot_commit_enclosing_failed_transaction(tmp_path):
    metadata = DataMetaStore(tmp_path / "data.sqlite")
    with metadata.writer_session():
        with pytest.raises(RuntimeError, match="cancel"):
            with metadata.connect() as db:
                db.execute("begin immediate")
                db.execute("insert into data_meta_state values('probe.outer','before','now')")
                with metadata.connect() as helper:
                    helper.execute("insert into data_meta_state values('probe.inner','during','now')")
                raise RuntimeError("cancel")
        assert not metadata._rows("select 1 from data_meta_state where key like 'probe.%'")


def test_atomic_fences_borrow_idle_session_and_release_each_transaction(tmp_path, monkeypatch):
    metadata = DataMetaStore(tmp_path / "data.sqlite")
    original = metadata._new_connection
    opened = 0
    def counted():
        nonlocal opened
        opened += 1
        return original()
    monkeypatch.setattr(metadata, "_new_connection", counted)
    with metadata.writer_session() as connection:
        with metadata.atomic_transaction() as atomic:
            assert atomic is connection
            atomic.execute("insert into data_meta_state values('probe.commit','one','now')")
            with metadata.connect() as helper:
                assert helper is atomic
        assert not connection.in_transaction
        # A competing writer can commit between fences. The session retains no
        # old snapshot or operation-long write lock.
        with sqlite3.connect(metadata.data_meta_path, timeout=.1) as external:
            assert external.execute("select value from data_meta_state where key='probe.commit'").fetchone()[0] == 'one'
            external.execute("update data_meta_state set value='two' where key='probe.commit'")
        assert metadata._rows("select value from data_meta_state where key='probe.commit'")[0]['value'] == 'two'
        with pytest.raises(RuntimeError, match="cancel"):
            with metadata.atomic_transaction() as atomic:
                atomic.execute("insert into data_meta_state values('probe.rollback','cancel','now')")
                with metadata.connect() as helper:
                    helper.execute("select 1")
                raise RuntimeError("cancel")
        assert not connection.in_transaction
        assert not metadata._rows("select 1 from data_meta_state where key='probe.rollback'")
        with metadata.atomic_transaction() as atomic:
            atomic.execute("update data_meta_state set value='three' where key='probe.commit'")
    assert opened == 1
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        connection.execute("select 1")


def test_active_transactions_and_read_views_cannot_upgrade_atomic_fences(tmp_path):
    metadata = DataMetaStore(tmp_path / "data.sqlite")
    with metadata.writer_session():
        with metadata.connect() as db:
            db.execute("begin")
            with pytest.raises(ConfigurationError, match="nested"):
                with metadata.atomic_transaction():
                    pytest.fail("active transaction upgraded")
        with metadata.atomic_transaction():
            with pytest.raises(ConfigurationError, match="nested"):
                with metadata.atomic_transaction():
                    pytest.fail("atomic transaction nested")
        with metadata.read_view():
            with pytest.raises(ConfigurationError, match="upgrade"):
                with metadata.publication_read_view():
                    pytest.fail("read view upgraded")


@pytest.mark.parametrize("borrow", [False, True])
def test_atomic_write_cannot_upgrade_read_view_and_publication_guard_remains_valid(tmp_path, borrow):
    from contextlib import nullcontext
    metadata = DataMetaStore(tmp_path / "data.sqlite")
    with metadata.writer_session() if borrow else nullcontext():
        with metadata.read_view() as read:
            before = read.execute("select count(*) from data_meta_state").fetchone()[0]
            with pytest.raises(ConfigurationError, match="upgrade"):
                with metadata.atomic_transaction() as writer:
                    writer.execute("insert into data_meta_state values('probe.invalid','write','now')")
            assert read.execute("select count(*) from data_meta_state").fetchone()[0] == before
        assert not metadata._rows("select 1 from data_meta_state where key='probe.invalid'")
        # A publication guard owns its transaction before installing the view;
        # its same-thread helpers still share that valid atomic boundary.
        with metadata.publication_read_view() as writer:
            with metadata.connect() as helper:
                assert helper is writer
                helper.execute("insert into data_meta_state values('probe.valid','guard','now')")
        assert metadata._rows("select value from data_meta_state where key='probe.valid'")[0]['value'] == 'guard'
