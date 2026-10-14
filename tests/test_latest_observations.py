from datetime import date

import polars as pl
import pytest

from bagelquant_data import DataLake, DataItemSpec, ItemInput, ExecutionOptions


def test_latest_observation_uses_global_cutoff_and_bounded_monthly_reads(tmp_path, monkeypatch):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("price"))
    first = pl.DataFrame({"time": [date(2020, 1, 1)] * 2, "asset_id": ["A", "S"], "value": [1., 7.]})
    lake.items.ingest("price", first, available_date="2020-01-01")
    lake.items.ingest("price", first.head(1).with_columns(pl.lit(2.).alias("value")), available_date="2020-04-01")
    lake.items.ingest("price", pl.DataFrame({"time": [date(2020, 3, 1)], "asset_id": ["A"], "value": [3.]}),
        available_date="2020-03-01")
    frozen = lake.inputs.freeze({"price": ItemInput("price", start="2020-01-01", end="2020-03-31")},
        information_cutoff="2020-04-02", retain_batch_bounds=True)
    original = lake.inputs.read
    windows = []
    def read(*args, **kwargs):
        windows.append((kwargs["start"], kwargs["end"]))
        return original(*args, **kwargs)
    monkeypatch.setattr(lake.inputs, "read", read)
    result = lake.inputs.latest(frozen, "price", through="2020-02-29")
    assert result.sort("asset_id")["value"].to_list() == [2., 7.]
    assert windows == [(date(2020, 1, 1), date(2020, 1, 31)), (date(2020, 2, 1), date(2020, 2, 29))]
    assert lake.inputs.latest(frozen, "price", through="2020-03-31").sort("asset_id")["value"].to_list() == [3., 7.]
    with pytest.raises(ValueError, match="widen"):
        lake.inputs.latest(frozen, "price", through="2020-04-01")
    with pytest.raises(MemoryError, match="state"):
        lake.inputs.latest(frozen, "price", through="2020-02-29", config=ExecutionOptions(max_buffer_bytes=8))


def test_latest_missing_captured_bounds_blocks_without_byte_reads(tmp_path, monkeypatch):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("price"))
    lake.items.ingest("price", pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": [1.]}),
        available_date="2020-01-01")
    with lake._data_meta.connect() as db:
        db.execute("UPDATE version_segments SET min_observation=NULL,max_observation=NULL,bounds_json=?", ('{"record":false}',))
    frozen = lake.inputs.freeze({"price": ItemInput("price")}, information_cutoff="2020-02-01")
    with lake._data_meta.connect() as db:
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='input_batch_index'").fetchone() is None
    monkeypatch.setattr(lake.inputs, "read", lambda *a, **k: pytest.fail("unbounded original read"))
    with pytest.raises(ValueError, match="batch bounds"):
        lake.inputs.latest(frozen, "price", through="2020-01-31")
