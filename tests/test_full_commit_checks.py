from datetime import UTC, date, datetime
import json
import sqlite3
from typing import cast

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec, RawInput
from bagelquant_data.storage import full_commit_checks as proofs
from bagelquant_data.storage.data_meta import DataMetaStore


def prepared(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("selected", "by_date", date_kind="calendar",
                       field_mappings={"time": "time", "asset_id": "asset_id"})
    frame = pl.DataFrame({"time": [date(2020, 1, 1), date(2020, 2, 1)],
                          "asset_id": ["A", "B"], "value": [1., 2.]})
    lake.raw.ingest(spec, frame, mode="initialize", ingested_at=datetime(2020, 2, 2, tzinfo=UTC))
    lake.raw.ingest(spec, frame, ingested_at=datetime(2020, 2, 3, tzinfo=UTC))
    return lake


def request(**kwargs):
    return {"raw": RawInput("custom", "selected", view="snapshot", strict=True, **kwargs)}


def test_compact_replay_matches_inline_and_keeps_older_currentness(tmp_path, monkeypatch):
    lake = prepared(tmp_path)
    old = lake.inputs.freeze(request(), information_cutoff="2020-02-03")
    expected = lake.inputs.read(old, "raw").collect()
    monkeypatch.setattr(proofs, "COMPACT_MIN_ROWS", 1)
    compact = lake.inputs.freeze(request(), information_cutoff="2020-02-03")
    assert compact.evidence["raw"]["record_checks"] == []
    assert len(compact.evidence["raw"]["full_commit_checks"]) == 1
    assert lake.inputs.read(compact, "raw").collect().equals(expected)
    assert lake.inputs.is_current(old)
    assert lake.inputs.get(old).digest == old.digest
    assert lake.inputs.is_current(compact)
    assert lake.inputs.verify(compact)["valid"]
    assert lake.inputs.read(compact, "raw", as_of="2020-02-02").collect().is_empty()
    snapshot = DataMetaStore(lake.data_meta_path, read_only=True, runtime=True).dataset_snapshot("custom", "selected")
    assert len(snapshot["full_commit_checks"]) == 1
    assert lake.raw.read("selected", source="custom", as_of="2020-02-03", strict=True).collect()["value"].to_list() == [1., 2.]
    assert lake.raw.read("selected", source="custom", as_of="2020-02-02", strict=True).collect().is_empty()


@pytest.mark.parametrize("mutation", ["hash", "id", "extra", "mixed_date", "missing", "wrong_commit"])
def test_same_count_wrong_or_partial_witnesses_never_seal(tmp_path, monkeypatch, mutation):
    lake = prepared(tmp_path)
    monkeypatch.setattr(proofs, "COMPACT_MIN_ROWS", 1)
    with sqlite3.connect(lake.data_meta_path) as db:
        check = db.execute("select max(id) from version_checks").fetchone()[0]
        record = db.execute("select record_id from version_check_records where check_id=? order by record_id limit 1", (check,)).fetchone()[0]
        if mutation == "hash":
            db.execute("update version_check_records set payload_hash='wrong' where check_id=? and record_id=?", (check, record))
        elif mutation == "id":
            db.execute("update version_check_records set record_id='wrong' where check_id=? and record_id=?", (check, record))
        elif mutation == "extra":
            db.execute("insert into version_check_records select check_id,'extra',payload_hash,version_commit,available_date from version_check_records where check_id=? limit 1", (check,))
        elif mutation == "mixed_date":
            db.execute("update version_check_records set available_date='2020-02-04' where check_id=? and record_id=?", (check, record))
        elif mutation == "missing":
            db.execute("delete from version_check_records where check_id=? and record_id=?", (check, record))
        else:
            db.execute("update version_check_records set version_commit=999 where check_id=? and record_id=?", (check, record))
    frozen = lake.inputs.freeze(request(), information_cutoff="2020-02-04")
    assert "full_commit_checks" not in frozen.evidence["raw"]


def test_read_only_no_seal_writes_and_frozen_self_contained_verification(tmp_path, monkeypatch):
    lake = prepared(tmp_path)
    monkeypatch.setattr(proofs, "COMPACT_MIN_ROWS", 1)
    readonly = DataLake.open(data_meta_path=lake.data_meta_path, lake_path=lake.lake_path, read_only=True, runtime=True)
    readonly.raw.read("selected", source="custom", as_of="2020-02-03", strict=True).collect()
    with sqlite3.connect(lake.data_meta_path) as db:
        assert db.execute("select count(*) from data_meta_state where key like 'full_commit_check%' ").fetchone()[0] == 0
    frozen = lake.inputs.freeze(request(), information_cutoff="2020-02-03")
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("delete from data_meta_state where key like 'full_commit_check%'")
    assert lake.inputs.verify(frozen)["valid"]
    assert lake.inputs.read(frozen, "raw").collect().height == 2
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("update version_batches set payload=x'00'")
    with pytest.raises(RuntimeError):
        lake.inputs.verify(frozen)


def test_cache_corruption_rejected(tmp_path, monkeypatch):
    lake = prepared(tmp_path)
    monkeypatch.setattr(proofs, "COMPACT_MIN_ROWS", 1)
    lake.inputs.freeze(request(), information_cutoff="2020-02-03")
    with sqlite3.connect(lake.data_meta_path) as db:
        key, payload = db.execute("select key,value from data_meta_state where key like 'full_commit_check_blob:%'").fetchone()
        seal = json.loads(payload)
        seal["available_date"] = "1999-01-01"
        db.execute("update data_meta_state set value=? where key=?", (json.dumps(seal), key))
    with pytest.raises(RuntimeError, match="checksum"):
        lake.inputs.freeze(request(), information_cutoff="2020-02-03")


def test_month_window_prunes_batch_reads_without_changing_receipt_evidence(tmp_path, monkeypatch):
    lake = prepared(tmp_path)
    monkeypatch.setattr(proofs, "COMPACT_MIN_ROWS", 1)
    frozen = lake.inputs.freeze(request(start="2020-02-01", end="2020-02-29"), information_cutoff="2020-02-03")
    from bagelquant_data import inputs
    original = inputs.read_batch
    partitions = []
    def tracked(*args):
        partitions.append(args[1])
        return original(*args)
    monkeypatch.setattr(inputs, "read_batch", tracked)
    result = lake.inputs.read(frozen, "raw").collect()
    assert result["value"].to_list() == [2.]
    assert len(partitions) == 1
    assert len(frozen.evidence["raw"]["batches"]) == 2
    partitions.clear()
    original_verify = inputs.verify_batch
    def tracked_verify(*args, **kwargs):
        partitions.append(args[1])
        return original_verify(*args, **kwargs)
    monkeypatch.setattr(inputs, "verify_batch", tracked_verify)
    assert lake.inputs.verify(frozen)["valid"]
    assert len(partitions) == 2


def test_large_general_checks_retain_snapshot_path(tmp_path, monkeypatch):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("general", "general", field_mappings={"asset_id": "asset_id"})
    frame = pl.DataFrame({"asset_id": ["A", "B"], "value": [1., 2.]})
    lake.raw.ingest(spec, frame, mode="initialize", ingested_at=datetime(2020, 2, 2, tzinfo=UTC))
    lake.raw.ingest(spec, frame, ingested_at=datetime(2020, 2, 3, tzinfo=UTC))
    monkeypatch.setattr(proofs, "COMPACT_MIN_ROWS", 1)
    frozen = lake.inputs.freeze({"raw": RawInput("custom", "general", strict=True, view="snapshot")}, information_cutoff="2020-02-03")
    assert "full_commit_checks" not in frozen.evidence["raw"]
    assert lake.inputs.read(frozen, "raw").collect().height == 2
    assert lake.raw.read("general", source="custom", as_of="2020-02-03", strict=True).collect().height == 2


def test_old_receipt_currentness_honors_pre_check_read_boundary(tmp_path, monkeypatch):
    from bagelquant_data.inputs import input_read_boundary, InputsAPI
    lake = prepared(tmp_path)
    with sqlite3.connect(lake.data_meta_path) as db:
        latest = db.execute("select max(id) from version_checks").fetchone()[0]
    with input_read_boundary(lake.data_meta_path, lake.inputs.max_commit(), max_check_id=latest - 1):
        old = InputsAPI(lake).freeze(request(), information_cutoff="2020-02-03")
    monkeypatch.setattr(proofs, "COMPACT_MIN_ROWS", 1)
    assert not lake.inputs.is_current(old)
    with input_read_boundary(lake.data_meta_path, old.max_commit, max_check_id=old.max_check_id):
        assert InputsAPI(lake).is_current(old)


def test_compact_ingestion_and_check_ceiling_match_inline(tmp_path, monkeypatch):
    lake = prepared(tmp_path)
    monkeypatch.setattr(proofs, "COMPACT_MIN_ROWS", 1)
    lake.inputs.freeze(request(), information_cutoff="2020-02-03")
    assert lake.raw.read("selected", source="custom", strict=True, as_of="2020-02-03",
                         ingested_before=datetime(2020, 2, 2, tzinfo=UTC)).collect().is_empty()
    from bagelquant_data.inputs import input_read_boundary, InputsAPI
    with input_read_boundary(lake.data_meta_path, lake.inputs.max_commit(), max_check_id=0):
        frozen = InputsAPI(lake).freeze(request(), information_cutoff="2020-02-03")
        assert frozen.max_check_id == 0
        assert lake.inputs.read(frozen, "raw").collect().is_empty()


def test_multiple_inline_aliases_respect_aggregate_receipt_limit(tmp_path):
    lake = prepared(tmp_path)
    class LimitedReceiptConnection:
        def __init__(self, db):
            self.db = db
        def execute(self, *args):
            return self.db.execute(*args)
        def getlimit(self, category):
            assert category == sqlite3.SQLITE_LIMIT_LENGTH
            return 2_000
    with lake._data_meta.connect() as db:
        with pytest.raises(MemoryError, match="size limit"):
            lake.inputs._capture(cast(sqlite3.Connection, LimitedReceiptConnection(db)),
                                 {"a": request()["raw"], "b": request()["raw"]}, cutoff=date(2020, 2, 3))
