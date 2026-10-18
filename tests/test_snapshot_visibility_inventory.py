"""Public copies retain every registered original, including visibility events."""
from datetime import UTC, date, datetime
import hashlib
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec, RawInput
from bagelquant_data.management import backup
from test_integrity_snapshot import evidence


def prepared(tmp_path, *, frozen=True):
    lake = DataLake.open(data_meta_path=tmp_path / "source/meta.sqlite", lake_path=tmp_path / "source/lake")
    receipts = []
    for name in ("first", "second"):
        spec = DatasetSpec(name,"by_date",date_kind="calendar",field_mappings={"time":"time","asset_id":"asset_id"})
        frame = pl.DataFrame({"time":[date(2020,1,1)],"asset_id":["A"],"value":[1.]})
        lake.raw.ingest(spec,frame,mode="initialize",ingested_at=datetime(2020,1,1,tzinfo=UTC))
        lake.raw.ingest(spec,frame,ingested_at=datetime(2020,1,2,tzinfo=UTC))
        if frozen:
            receipts.append(lake.inputs.freeze({"raw":RawInput("custom",name,view="snapshot")},information_cutoff="2020-01-03"))
    events = list(lake.lake_path.rglob(".visibility/*.parquet"))
    assert len(events) == 2
    return lake,receipts,events


def copy(lake,tmp_path,operation="snapshot"):
    return getattr(lake.integrity,operation)(data_meta_path=tmp_path / "copy/meta.sqlite",lake_path=tmp_path / "copy/lake")


@pytest.mark.parametrize("operation", ["snapshot", "backup"])
def test_public_copy_preserves_visibility_identity_and_reopened_audit(tmp_path,monkeypatch,operation):
    lake,receipts,events = prepared(tmp_path)
    expected = [lake.inputs.verify(frozen) for frozen in receipts]
    with sqlite3.connect(lake.data_meta_path) as db:
        metadata_before = tuple(db.iterdump())
    before = evidence(tmp_path / "source")
    if operation == "snapshot":
        monkeypatch.setattr(backup,"verify",lambda *args:pytest.fail("snapshot performed deep audit"))
        monkeypatch.setattr(backup,"_file",lambda *args:pytest.fail("snapshot hashed original files"))
    report = copy(lake,tmp_path,operation)
    if operation == "snapshot":
        assert report["valid"] is None and report["verification"] == "unverified"
        assert report["generation_count"] == 2
        assert report["immutable_file_count"] == 4 and report["visibility_count"] == 2
    else:
        assert report["valid"]
        assert len([value for value in report["files"] if value["kind"] == "lake"]) == 4
    copied = DataLake.open(data_meta_path=report["data_meta_path"],lake_path=report["lake_path"],read_only=True)
    for frozen,result in zip(receipts,expected,strict=True):
        assert copied.inputs.get(frozen).digest == frozen.digest
        assert copied.inputs.verify(frozen) == result
    for original in events:
        destination = copied.lake_path / original.relative_to(lake.lake_path)
        assert destination.read_bytes() == original.read_bytes()
        assert destination.stat().st_ino != original.stat().st_ino
        destination.write_bytes(b"independent corruption")
        assert original.read_bytes() != b"independent corruption"
    if operation == "snapshot":
        assert evidence(tmp_path / "source") == before
    else:
        # Verified backup retains ordinary SQLite coordination/checkpoint behavior.
        assert evidence(lake.lake_path) == {path.relative_to("lake"): facts for path,facts in before.items()
                                           if path.parts[0] == "lake"}
        with sqlite3.connect(lake.data_meta_path) as db:
            assert tuple(db.iterdump()) == metadata_before


def test_verified_backup_restore_retains_visibility_and_historical_general_source(tmp_path):
    lake,receipts,_ = prepared(tmp_path)
    spec = DatasetSpec("general","general")
    lake.raw.ingest(spec,pl.DataFrame({"value":[1.]}),mode="initialize",ingested_at=datetime(2020,1,1,tzinfo=UTC))
    historical = lake.inputs.freeze({"general":RawInput("custom","general")},information_cutoff="2020-01-02")
    lake.raw.ingest(spec,pl.DataFrame({"value":[2.]}),mode="refresh",ingested_at=datetime(2020,2,1,tzinfo=UTC))
    # A retained committed original is authority even when absent from the current manifest.
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("delete from partition_manifest where dataset='general' and generation_path in "
                   "(select generation_path from version_segments where commit_seq="
                   "(select min(seq) from version_commits where dataset='general'))")
    report = copy(lake,tmp_path,"backup")
    restored = DataLake.restore(backup_data_meta_path=report["data_meta_path"],backup_lake_path=report["lake_path"],
        data_meta_path=tmp_path / "restored/meta.sqlite",lake_path=tmp_path / "restored/lake")
    assert restored.inputs.read(historical,"general").collect()["value"].to_list() == [1.]
    for frozen in [historical,*receipts]:
        assert restored.inputs.verify(frozen)["valid"]
    snapshot = lake.integrity.snapshot(data_meta_path=tmp_path / "snapshot/meta.sqlite",lake_path=tmp_path / "snapshot/lake")
    reopened = DataLake.open(data_meta_path=snapshot["data_meta_path"],lake_path=snapshot["lake_path"],read_only=True)
    assert reopened.inputs.verify(historical)["valid"]
    assert snapshot["generation_count"] == 3
    assert snapshot["immutable_file_count"] == 6


@pytest.mark.parametrize("operation", ["snapshot", "backup"])
def test_missing_registered_visibility_fails_and_cleans_only_new_copy(tmp_path,operation):
    lake,_,events = prepared(tmp_path)
    events[0].unlink()
    before = evidence(tmp_path / "source")
    with pytest.raises(FileNotFoundError):
        copy(lake,tmp_path,operation)
    assert evidence(tmp_path / "source") == before
    assert not (tmp_path / "copy/meta.sqlite").exists() and not (tmp_path / "copy/lake").exists()


@pytest.mark.parametrize("operation", ["snapshot", "backup", "verify_backup"])
def test_corrupt_unreferenced_visibility_never_becomes_verified(tmp_path,operation):
    lake,_,events = prepared(tmp_path,frozen=False)
    events[0].write_bytes(b"corrupt visibility")
    before = evidence(tmp_path / "source")
    if operation == "snapshot":
        report = copy(lake,tmp_path)
        assert report["valid"] is None
        copied = DataLake.open(data_meta_path=report["data_meta_path"],lake_path=report["lake_path"],read_only=True)
        with pytest.raises(RuntimeError,match="visibility.*checksum"):
            copied.integrity.verify_backup()
    else:
        with pytest.raises(RuntimeError,match="visibility.*checksum"):
            lake.integrity.verify_backup() if operation == "verify_backup" else copy(lake,tmp_path,operation)
    assert evidence(tmp_path / "source") == before


@pytest.mark.parametrize("reference", ["../escape.parquet", "/absolute.parquet", ".visibility/../escape.parquet",
    ".visibility/nohex.parquet", ".visibility\\escape.parquet", ".visibility/C:escape.parquet"])
def test_snapshot_rejects_malformed_visibility_references_without_source_changes(tmp_path,reference):
    lake,_,_ = prepared(tmp_path)
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("update visibility_segments set generation_path=? where check_id=(select min(check_id) from visibility_segments)",(reference,))
    before = evidence(tmp_path / "source")
    with pytest.raises(ValueError,match="visibility file reference"):
        copy(lake,tmp_path)
    assert evidence(tmp_path / "source") == before
    assert not (tmp_path / "copy/lake").exists()


@pytest.mark.parametrize("kind", ["leaf", "ancestor"])
def test_snapshot_rejects_visibility_symbolic_links(tmp_path,kind):
    lake,_,events = prepared(tmp_path)
    event = events[0]
    if kind == "leaf":
        outside = tmp_path / "outside.parquet"
        event.rename(outside)
        event.symlink_to(outside)
    else:
        outside = tmp_path / "outside"
        event.parent.rename(outside)
        event.parent.symlink_to(outside,target_is_directory=True)
    with pytest.raises(ValueError,match="escapes|symbolic"):
        copy(lake,tmp_path)
    assert event.is_file() and not (tmp_path / "copy/lake").exists()


def test_snapshot_uses_copied_metadata_inventory_and_ignores_loose_files(tmp_path,monkeypatch):
    lake,receipts,_ = prepared(tmp_path)
    loose = lake.lake_path / "raw/custom/first/.visibility" / ("f"*32+".parquet")
    loose.write_bytes(b"unregistered")
    original = backup.copy_database
    def captured(source,destination):
        original(source,destination)
        # A new source event published after metadata capture is outside this copy.
        spec = lake.raw.get("first",source="custom")
        lake.raw.ingest(spec,pl.DataFrame({"time":[date(2020,1,2)],"asset_id":["B"],"value":[2.]}),
                        ingested_at=datetime(2020,1,4,tzinfo=UTC))
    monkeypatch.setattr(backup,"copy_database",captured)
    report = copy(lake,tmp_path)
    assert report["immutable_file_count"] == 4
    assert not (tmp_path / "copy/lake" / loose.relative_to(lake.lake_path)).exists()
    copied = DataLake.open(data_meta_path=report["data_meta_path"],lake_path=report["lake_path"],read_only=True)
    assert all(copied.inputs.verify(frozen)["valid"] for frozen in receipts)
    assert hashlib.sha256(loose.read_bytes()).hexdigest() == hashlib.sha256(b"unregistered").hexdigest()


def test_verified_backup_checks_unreferenced_committed_original_outside_manifest(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "source/meta.sqlite",lake_path=tmp_path / "source/lake")
    spec = DatasetSpec("general","general")
    lake.raw.ingest(spec,pl.DataFrame({"value":[1.]}),mode="initialize",ingested_at=datetime(2020,1,1,tzinfo=UTC))
    old = next(lake.lake_path.rglob("*.parquet"))
    lake.raw.ingest(spec,pl.DataFrame({"value":[2.]}),mode="refresh",ingested_at=datetime(2020,2,1,tzinfo=UTC))
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("delete from partition_manifest where generation_path like 'year=2020/month=01/%'")
        assert db.execute("select count(*) from frozen_inputs").fetchone()[0] == 0
    old.write_bytes(b"corrupt unreferenced committed original")
    with pytest.raises(RuntimeError,match="checksum mismatch"):
        lake.integrity.verify_backup()


def test_snapshot_rejects_registered_visibility_without_check_owner(tmp_path):
    lake,_,_ = prepared(tmp_path)
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("update visibility_segments set check_id=check_id+1000")
    with pytest.raises(RuntimeError,match="no check owner"):
        copy(lake,tmp_path)
    assert not (tmp_path / "copy/lake").exists()


@pytest.mark.parametrize("kind", ["malformed", "leaf", "ancestor"])
def test_verified_backup_confines_inventory_before_reading_any_original(tmp_path,monkeypatch,kind):
    lake,_,events = prepared(tmp_path,frozen=False)
    event = events[0]
    if kind == "malformed":
        with sqlite3.connect(lake.data_meta_path) as db:
            db.execute("update visibility_segments set generation_path='../outside.parquet'")
    elif kind == "leaf":
        outside = tmp_path / "outside.parquet"
        event.rename(outside)
        event.symlink_to(outside)
    else:
        outside = tmp_path / "outside"
        event.parent.rename(outside)
        event.parent.symlink_to(outside,target_is_directory=True)
    monkeypatch.setattr(backup,"_file",lambda *args:pytest.fail("read before inventory confinement"))
    monkeypatch.setattr(backup.pl,"read_parquet",lambda *args,**kwargs:pytest.fail("decoded before confinement"))
    with pytest.raises(ValueError,match="reference|symbolic"):
        lake.integrity.verify_backup()
