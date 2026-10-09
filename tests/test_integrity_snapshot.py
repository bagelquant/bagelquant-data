from contextlib import closing
from datetime import date
import hashlib
import shutil
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec, RawInput
from bagelquant_data.management import backup
from bagelquant_data.storage import snapshot


def prepared(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "source" / "meta.sqlite", lake_path=tmp_path / "source" / "lake")
    spec = DatasetSpec("raw", "by_date", date_kind="calendar", field_mappings={"time": "time", "asset_id": "asset_id"})
    lake.raw.ingest(spec, pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": [1.]}), mode="initialize")
    frozen = lake.inputs.freeze({"raw": RawInput("custom", "raw", view="versions")})
    return lake, frozen


def evidence(root):
    return {path.relative_to(root): (path.stat().st_ino, path.stat().st_size, path.stat().st_mtime_ns,
                                    hashlib.sha256(path.read_bytes()).hexdigest())
            for path in root.rglob("*") if path.is_file()}


@pytest.mark.parametrize("clone", [True, False])
def test_snapshot_retains_committed_wal_and_original_identity_without_source_changes(tmp_path, monkeypatch, clone):
    lake, frozen = prepared(tmp_path)
    copies = []
    def clone_file(source, destination):
        copies.append((source, destination))
        if clone:
            shutil.copy2(source, destination)
        return clone
    monkeypatch.setattr(snapshot, "_clone_file", clone_file)
    monkeypatch.setattr(backup, "verify", lambda *args: pytest.fail("unverified snapshot scanned history"))
    with closing(sqlite3.connect(lake.data_meta_path)) as writer:
        writer.execute("pragma wal_autocheckpoint=0")
        writer.execute("pragma wal_checkpoint(truncate)")
        writer.execute("update data_meta_state set updated_at='committed-wal' where key='schema_version'")
        writer.commit()
        source_id = writer.execute("select value from data_meta_state where key='lake_id'").fetchone()[0]
        before = evidence(tmp_path / "source")
        report = lake.integrity.snapshot(data_meta_path=tmp_path / "copy" / "meta.sqlite", lake_path=tmp_path / "copy" / "lake")
        assert evidence(tmp_path / "source") == before
        assert report["valid"] is None and report["verification"] == "unverified"
        assert any(str(source).endswith("-wal") for source, _ in copies)
        copied = DataLake.open(data_meta_path=report["data_meta_path"], lake_path=report["lake_path"], read_only=True)
        with copied._data_meta.connect() as db:
            assert db.execute("select updated_at from data_meta_state where key='schema_version'").fetchone()[0] == "committed-wal"
            copied_id = db.execute("select value from data_meta_state where key='lake_id'").fetchone()[0]
        assert copied_id == source_id
        assert copied.inputs.verify(frozen)["valid"]
        for path in (tmp_path / "copy").rglob("*"):
            if path.is_file():
                assert not path.is_symlink()
                source = tmp_path / "source" / path.relative_to(tmp_path / "copy")
                if source.is_file():
                    assert source.stat().st_ino != path.stat().st_ino
        with sqlite3.connect(report["data_meta_path"]) as db:
            db.execute("update version_batches set payload=x'00'")
        copied_path = next((tmp_path / "copy" / "lake").rglob("*.parquet"))
        copied_path.write_bytes(b"independent damage")
        assert evidence(tmp_path / "source") == before
        assert lake.inputs.verify(frozen)["valid"]


def test_snapshot_cleanup_is_confined_to_new_destinations(tmp_path, monkeypatch):
    lake, _ = prepared(tmp_path)
    before = evidence(tmp_path / "source")
    original = snapshot.copy_file
    def fail_generation(source, destination):
        if source.suffix == ".parquet":
            raise OSError("generation copy failed")
        return original(source, destination)
    monkeypatch.setattr(backup, "copy_file", fail_generation)
    with pytest.raises(OSError, match="generation copy failed"):
        lake.integrity.snapshot(data_meta_path=tmp_path / "copy" / "meta.sqlite", lake_path=tmp_path / "copy" / "lake")
    assert evidence(tmp_path / "source") == before
    assert not (tmp_path / "copy" / "meta.sqlite").exists()
    assert not (tmp_path / "copy" / "lake").exists()
    unrelated = tmp_path / "copy" / "keep"
    unrelated.write_text("preserved")
    existing = tmp_path / "existing"
    existing.mkdir()
    with pytest.raises(FileExistsError):
        lake.integrity.snapshot(data_meta_path=tmp_path / "existing.sqlite", lake_path=existing)
    assert unrelated.read_text() == "preserved"


@pytest.mark.parametrize("kind", ["inside_lake", "contains_lake", "contains_metadata", "same_metadata", "future_sidecar"])
def test_snapshot_rejects_source_overlap_before_writes(tmp_path, kind):
    lake, _ = prepared(tmp_path)
    metadata = tmp_path / "copy" / "meta.sqlite"
    target = tmp_path / "copy" / "lake"
    if kind == "inside_lake":
        metadata = lake.lake_path / "nested.sqlite"
    elif kind == "contains_lake":
        target = tmp_path / "source"
    elif kind == "contains_metadata":
        target = lake.data_meta_path.parent
    elif kind == "same_metadata":
        metadata = lake.data_meta_path
    else:
        metadata = lake.data_meta_path.with_name(lake.data_meta_path.name + "-journal")
    with pytest.raises((ValueError, FileExistsError)):
        lake.integrity.snapshot(data_meta_path=metadata, lake_path=target)
    assert not (tmp_path / "copy").exists()


def test_snapshot_rejects_active_rollback_journal_without_cleanup_of_source(tmp_path):
    lake, _ = prepared(tmp_path)
    journal = lake.data_meta_path.with_name(lake.data_meta_path.name + "-journal")
    journal.write_bytes(b"active!!")
    before = evidence(tmp_path / "source")
    with pytest.raises(sqlite3.OperationalError, match="rollback journal"):
        lake.integrity.snapshot(data_meta_path=tmp_path / "copy" / "meta.sqlite", lake_path=tmp_path / "copy" / "lake")
    assert evidence(tmp_path / "source") == before
    assert not (tmp_path / "copy" / "meta.sqlite").exists()
