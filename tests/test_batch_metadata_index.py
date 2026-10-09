from contextlib import contextmanager
import sqlite3
from datetime import UTC, date, datetime

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec, RawInput
from bagelquant_data.storage.data_meta import DataMetaStore


def fixture(root):
    paths = {"data_meta_path": root / "meta.sqlite", "lake_path": root / "lake"}
    lake = DataLake.open(**paths)
    spec = DatasetSpec("raw", "by_date", date_kind="calendar",
                       field_mappings={"time": "time", "asset_id": "asset_id"})
    frame = pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": [1.]})
    lake.raw.ingest(spec, frame, ingested_at=datetime(2020, 1, 2, tzinfo=UTC))
    receipt = lake.inputs.freeze({"raw": RawInput("custom", "raw", view="snapshot")}, information_cutoff="2020-01-31")
    lake.close()
    with sqlite3.connect(paths["data_meta_path"]) as db:
        db.execute("drop index version_batches_metadata")
        db.execute("delete from sqlite_stat1 where tbl='version_batches'")
    return paths, receipt, spec, frame


def evidence(path):
    with sqlite3.connect(path) as db:
        return {table: db.execute(f"select * from {table} order by rowid").fetchall()
                for table in ("data_meta_state", "version_commits", "version_batches", "frozen_inputs")}


def test_old_seven_readonly_then_writable_index_preserves_original_evidence(tmp_path):
    paths, receipt, spec, frame = fixture(tmp_path)
    before = evidence(paths["data_meta_path"])
    with DataLake.open(**paths, read_only=True) as lake:
        assert lake.inputs.get(receipt.receipt_id) == receipt
        assert lake.inputs.verify(receipt)["valid"] and lake.inputs.is_current(receipt)
        assert lake.inputs.read(receipt, "raw").collect()["value"].to_list() == [1.]
    with sqlite3.connect(paths["data_meta_path"]) as db:
        assert db.execute("select 1 from sqlite_master where name='version_batches_metadata'").fetchone() is None
    with DataLake.open(**paths) as lake:
        assert lake.inputs.get(receipt.receipt_id) == receipt
        assert lake.inputs.verify(receipt)["valid"] and lake.inputs.is_current(receipt)
        assert evidence(paths["data_meta_path"]) == before
    with sqlite3.connect(paths["data_meta_path"]) as db:
        stats = db.execute("select * from sqlite_stat1 where tbl='version_batches'").fetchall()
        assert stats
    with DataLake.open(**paths) as lake:
        lake.raw.ingest(spec, frame.with_columns(pl.lit(2.).alias("value")), ingested_at=datetime(2020, 1, 3, tzinfo=UTC))
        assert not lake.inputs.is_current(receipt)
        assert lake.inputs.read(receipt, "raw").collect()["value"].to_list() == [1.]
    with sqlite3.connect(paths["data_meta_path"]) as db:
        assert db.execute("select * from sqlite_stat1 where tbl='version_batches'").fetchall() == stats
        query = "select commit_seq,partition_path,content_hash,row_count,min_available,max_available,min_observation,max_observation from version_batches order by commit_seq,partition_path"
        assert any("COVERING INDEX version_batches_metadata" in row[3] for row in db.execute("explain query plan " + query))
        assert db.execute(query).fetchall() == db.execute(query.replace("from version_batches", "from version_batches not indexed")).fetchall()


def test_index_and_statistics_rollback_together_then_retry(tmp_path, monkeypatch):
    paths, receipt, _, _ = fixture(tmp_path)
    before = evidence(paths["data_meta_path"])
    original = DataMetaStore.connect

    class FailAnalyze:
        def __init__(self, db):
            self.db = db
        def __getattr__(self, name):
            return getattr(self.db, name)
        def execute(self, sql, *args):
            if sql.lower() == "analyze version_batches":
                raise sqlite3.OperationalError("injected analyze failure")
            return self.db.execute(sql, *args)

    @contextmanager
    def failing_connect(store):
        with original(store) as db:
            yield FailAnalyze(db)

    with monkeypatch.context() as patch:
        patch.setattr(DataMetaStore, "connect", failing_connect)
        with pytest.raises(sqlite3.OperationalError, match="injected"):
            DataLake.open(**paths)
    assert evidence(paths["data_meta_path"]) == before
    with sqlite3.connect(paths["data_meta_path"]) as db:
        assert db.execute("select 1 from sqlite_master where name='version_batches_metadata'").fetchone() is None
        assert not db.execute("select * from sqlite_stat1 where tbl='version_batches'").fetchall()
    with DataLake.open(**paths) as lake:
        assert lake.inputs.verify(receipt)["valid"] and lake.inputs.is_current(receipt)
