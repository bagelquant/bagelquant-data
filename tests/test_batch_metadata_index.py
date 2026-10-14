"""Schema-eight metadata does not bootstrap historical row proofs."""
from datetime import UTC, date, datetime
import sqlite3
import polars as pl
from bagelquant_data import DataLake, DatasetSpec, RawInput


def fixture(root):
    paths = {"data_meta_path": root / "meta.sqlite", "lake_path": root / "lake"}
    lake = DataLake.open(**paths)
    spec = DatasetSpec("raw", "by_date", date_kind="calendar", field_mappings={"time": "time", "asset_id": "asset_id"})
    frame = pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": [1.]})
    lake.raw.ingest(spec, frame, ingested_at=datetime(2020, 1, 2, tzinfo=UTC))
    receipt = lake.inputs.freeze({"raw": RawInput("custom", "raw", view="snapshot")}, information_cutoff="2020-01-31")
    lake.close()
    return paths, receipt, spec, frame


def evidence(path):
    with sqlite3.connect(path) as db:
        return {table: db.execute(f"select * from {table} order by rowid").fetchall()
                for table in ("data_meta_state", "version_commits", "version_segments", "frozen_inputs")}


def test_repeat_open_preserves_original_metadata_and_never_builds_row_indexes(tmp_path):
    paths, receipt, spec, frame = fixture(tmp_path)
    before = evidence(paths["data_meta_path"])
    for read_only in (True, False, True):
        with DataLake.open(**paths, read_only=read_only) as lake:
            assert lake.inputs.get(receipt.receipt_id) == receipt
            assert lake.inputs.verify(receipt)["valid"] and lake.inputs.is_current(receipt)
            assert lake.inputs.read(receipt, "raw").collect()["value"].to_list() == [1.]
        assert evidence(paths["data_meta_path"]) == before
    with sqlite3.connect(paths["data_meta_path"]) as db:
        names = {row[0] for row in db.execute("select name from sqlite_master")}
        assert not {"version_batches", "input_batch_index", "input_batch_value_index", "version_check_records"} & names
        columns = {row[1] for row in db.execute("pragma table_info(version_segments)")}
        assert "payload" not in columns
    with DataLake.open(**paths) as lake:
        lake.raw.ingest(spec, frame.with_columns(pl.lit(2.).alias("value")), ingested_at=datetime(2020, 1, 3, tzinfo=UTC))
        assert not lake.inputs.is_current(receipt)
        assert lake.inputs.read(receipt, "raw").collect()["value"].to_list() == [1.]
