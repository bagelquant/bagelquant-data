"""Declaration receipts require an explicit fresh schema-six lake."""
from contextlib import closing
import sqlite3

import polars as pl
import pytest

from bagelquant_data import ConfigurationError, DataLake, DatasetSpec
from bagelquant_data.storage.data_meta import DataMetaStore


def _files(root):
    return {path.relative_to(root): path.read_bytes() for path in root.rglob("*") if path.is_file()}


@pytest.fixture
def previous_schema_five(tmp_path):
    metadata, root = tmp_path / "data_meta.sqlite", tmp_path / "lake"
    lake = DataLake.open(data_meta_path=metadata, lake_path=root)
    lake.raw.ingest(DatasetSpec("retained", "general", source="fake"),
        pl.DataFrame({"asset_id": ["A"], "value": [3.0]}))
    lake.close()
    # Schema five had the complete Raw/PIT authority but no declaration receipt
    # table. Reconstruct that actual prior shape with retained committed bytes.
    with closing(sqlite3.connect(metadata)) as connection:
        connection.execute("DROP TABLE declaration_batch_receipts")
        connection.execute("UPDATE data_meta_state SET value='5' WHERE key='schema_version'")
        connection.commit()
        assert connection.execute("SELECT value FROM data_meta_state WHERE key='schema_version'").fetchone()[0] == "5"
    return metadata, root


@pytest.mark.parametrize("read_only", [True, False])
def test_schema_five_open_rejects_before_any_storage_changes(tmp_path, previous_schema_five, read_only):
    metadata, root = previous_schema_five
    files = {path.relative_to(tmp_path): path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()}
    entries = {path.relative_to(tmp_path) for path in tmp_path.rglob("*")}
    assert DataLake.inspect(data_meta_path=metadata, lake_path=root) == {
        "status": "incompatible", "reason": "metadata_schema_incompatible", "schema_version": "5",
    }
    with pytest.raises(ConfigurationError, match="Automatic migration is disabled"):
        DataLake.open(data_meta_path=metadata, lake_path=root, read_only=read_only)
    assert {path.relative_to(tmp_path) for path in tmp_path.rglob("*")} == entries
    assert {path.relative_to(tmp_path): path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()} == files


def test_new_lake_has_schema_six_and_read_only_declaration_receipts(tmp_path):
    metadata, root = tmp_path / "data_meta.sqlite", tmp_path / "lake"
    DataLake.open(data_meta_path=metadata, lake_path=root).close()
    with closing(sqlite3.connect(metadata.resolve().as_uri() + "?mode=ro", uri=True)) as connection:
        assert connection.execute("SELECT value FROM data_meta_state WHERE key='schema_version'").fetchone()[0] == "6"
    lake = DataLake.open(data_meta_path=metadata, lake_path=root, read_only=True)
    assert lake.catalog.declaration_batch_receipt("missing-request") is None


def test_fresh_lake_schema_can_be_entirely_in_live_wal(tmp_path):
    metadata, root = tmp_path / "data_meta.sqlite", tmp_path / "lake"
    with closing(sqlite3.connect(metadata)) as keeper:
        keeper.execute("PRAGMA journal_mode=WAL")
        keeper.execute("PRAGMA wal_autocheckpoint=0")
        keeper.execute("BEGIN")
        keeper.execute("SELECT name FROM sqlite_master").fetchall()
        DataLake.open(data_meta_path=metadata, lake_path=root).close()
        with closing(sqlite3.connect(metadata.as_uri() + "?mode=ro&immutable=1", uri=True)) as main_only:
            assert main_only.execute("SELECT name FROM sqlite_master WHERE name='data_meta_state'").fetchone() is None
        keeper.rollback()
        assert keeper.execute("SELECT value FROM data_meta_state WHERE key='schema_version'").fetchone()[0] == "6"
        before = _files(tmp_path)
        DataMetaStore.check_compatibility(metadata)
        assert DataLake.inspect(data_meta_path=metadata, lake_path=root)["status"] == "ready"
        assert _files(tmp_path) == before
        lake = DataLake.open(data_meta_path=metadata, lake_path=root, read_only=True)
        assert lake.catalog.declaration_batch_receipt("absent") is None
        lake.close()


@pytest.mark.parametrize("checkpointed,committed", [("5", "6"), ("6", "5")])
def test_compatibility_probe_reads_live_wal_without_any_source_writes(tmp_path, checkpointed, committed):
    metadata, root = tmp_path / "data_meta.sqlite", tmp_path / "lake"
    DataLake.open(data_meta_path=metadata, lake_path=root).close()
    with closing(sqlite3.connect(metadata)) as writer:
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("UPDATE data_meta_state SET value=? WHERE key='schema_version'", (checkpointed,))
        writer.commit()
        writer.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        writer.execute("UPDATE data_meta_state SET value=? WHERE key='schema_version'", (committed,))
        writer.commit()
        with closing(sqlite3.connect(metadata.as_uri() + "?mode=ro&immutable=1", uri=True)) as main_only:
            assert main_only.execute("SELECT value FROM data_meta_state WHERE key='schema_version'").fetchone()[0] == checkpointed
        before = _files(tmp_path)
        if committed == "6":
            DataMetaStore.check_compatibility(metadata)
        else:
            with pytest.raises(ConfigurationError, match="Automatic migration is disabled"):
                DataMetaStore.check_compatibility(metadata)
            for read_only in (True, False):
                with pytest.raises(ConfigurationError, match="Automatic migration is disabled"):
                    DataLake.open(data_meta_path=metadata, lake_path=root, read_only=read_only)
        inspection = DataLake.inspect(data_meta_path=metadata, lake_path=root)
        assert inspection["schema_version"] == committed
        assert inspection["status"] == ("ready" if committed == "6" else "incompatible")
        assert _files(tmp_path) == before


def test_probe_retries_if_source_changes_during_snapshot(tmp_path, monkeypatch):
    metadata, root = tmp_path / "data_meta.sqlite", tmp_path / "lake"
    DataLake.open(data_meta_path=metadata, lake_path=root).close()
    with closing(sqlite3.connect(metadata)) as writer:
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("UPDATE data_meta_state SET value='5' WHERE key='schema_version'")
        writer.commit()
        writer.execute("UPDATE data_meta_state SET value='6' WHERE key='schema_version'")
        writer.commit()
        import bagelquant_data.storage.data_meta as module
        original = module.shutil.copyfile
        changed = False

        def copy_and_change(source, destination):
            nonlocal changed
            result = original(source, destination)
            if str(source).endswith("-wal") and not changed:
                changed = True
                writer.execute("UPDATE data_meta_state SET value='5' WHERE key='schema_version'")
                writer.commit()
            return result

        monkeypatch.setattr(module.shutil, "copyfile", copy_and_change)
        with pytest.raises(ConfigurationError, match="Automatic migration is disabled"):
            DataMetaStore.check_compatibility(metadata)
        assert changed


def test_public_inspect_never_creates_directories_or_checkpointed_sidecars(tmp_path):
    metadata, root = tmp_path / "missing" / "data_meta.sqlite", tmp_path / "lake"
    assert DataLake.inspect(data_meta_path=metadata, lake_path=root) == {
        "status": "uninitialized", "reason": "metadata_missing", "schema_version": None,
    }
    assert list(tmp_path.iterdir()) == []
    DataLake.open(data_meta_path=metadata, lake_path=root).close()
    with closing(sqlite3.connect(metadata)) as writer:
        writer.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    assert not metadata.with_name(metadata.name + "-wal").exists()
    assert not metadata.with_name(metadata.name + "-shm").exists()
    before = _files(tmp_path)
    entries = {path.relative_to(tmp_path) for path in tmp_path.rglob("*")}
    assert DataLake.inspect(data_meta_path=metadata, lake_path=root)["status"] == "ready"
    assert DataLake.inspect(data_meta_path=metadata, lake_path=tmp_path / "different")["reason"] == "lake_binding_invalid"
    assert _files(tmp_path) == before
    assert {path.relative_to(tmp_path) for path in tmp_path.rglob("*")} == entries


def test_schema_six_missing_receipt_authority_is_not_auto_created(tmp_path):
    metadata, root = tmp_path / "data_meta.sqlite", tmp_path / "lake"
    DataLake.open(data_meta_path=metadata, lake_path=root).close()
    with closing(sqlite3.connect(metadata)) as writer:
        writer.execute("DROP TABLE declaration_batch_receipts")
        writer.commit()
    before = _files(tmp_path)
    assert DataLake.inspect(data_meta_path=metadata, lake_path=root)["reason"] == "metadata_schema_incompatible"
    with pytest.raises(ConfigurationError, match="Automatic migration is disabled"):
        DataLake.open(data_meta_path=metadata, lake_path=root)
    assert _files(tmp_path) == before


def test_probe_refuses_uncommitted_rollback_pages_without_recovery(tmp_path):
    metadata, root = tmp_path / "data_meta.sqlite", tmp_path / "lake"
    DataLake.open(data_meta_path=metadata, lake_path=root).close()
    with closing(sqlite3.connect(metadata)) as writer:
        assert writer.execute("PRAGMA journal_mode=DELETE").fetchone()[0] == "delete"
        writer.execute("CREATE TABLE padding(value BLOB)")
        writer.execute("UPDATE data_meta_state SET value='5' WHERE key='schema_version'")
        writer.commit()
        writer.execute("PRAGMA cache_size=1")
        writer.execute("PRAGMA cache_spill=ON")
        writer.execute("BEGIN IMMEDIATE")
        writer.execute("UPDATE data_meta_state SET value='6' WHERE key='schema_version'")
        writer.executemany("INSERT INTO padding(value) VALUES(?)", [(b"x" * 65536,) for _ in range(40)])
        journal = metadata.with_name(metadata.name + "-journal")
        assert journal.stat().st_size > 0 and any(journal.read_bytes()[:8])
        # SQLite has spilled an uncommitted schema-six page into the main file.
        # A main-only immutable read would incorrectly declare this lake ready.
        with closing(sqlite3.connect(metadata.as_uri() + "?mode=ro&immutable=1", uri=True)) as unsafe:
            assert unsafe.execute("SELECT value FROM data_meta_state WHERE key='schema_version'").fetchone()[0] == "6"
        before = _files(tmp_path)
        assert DataLake.inspect(data_meta_path=metadata, lake_path=root) == {
            "status": "incompatible", "reason": "metadata_unreadable", "schema_version": None,
        }
        for read_only in (True, False):
            with pytest.raises(sqlite3.OperationalError, match="active rollback journal"):
                DataLake.open(data_meta_path=metadata, lake_path=root, read_only=read_only)
        assert _files(tmp_path) == before
        writer.rollback()
        assert DataLake.inspect(data_meta_path=metadata, lake_path=root)["schema_version"] == "5"


def test_invalidated_persist_journal_allows_inspection_without_changes(tmp_path):
    metadata, root = tmp_path / "data_meta.sqlite", tmp_path / "lake"
    DataLake.open(data_meta_path=metadata, lake_path=root).close()
    with closing(sqlite3.connect(metadata)) as writer:
        assert writer.execute("PRAGMA journal_mode=PERSIST").fetchone()[0] == "persist"
        writer.execute("UPDATE data_meta_state SET updated_at='persist-test' WHERE key='schema_version'")
        writer.commit()
    journal = metadata.with_name(metadata.name + "-journal")
    assert journal.stat().st_size > 0 and journal.read_bytes()[:8] == bytes(8)
    before = _files(tmp_path)
    assert DataLake.inspect(data_meta_path=metadata, lake_path=root)["status"] == "ready"
    assert _files(tmp_path) == before
