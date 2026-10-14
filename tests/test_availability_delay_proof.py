from datetime import date
import json
import sqlite3

import polars as pl
import pytest
from bagelquant_data import DataLake, DataItemSpec, ItemInput
from bagelquant_data import input_index


def setup(tmp_path, late=False):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("value"))
    times = [date(2023, 12, 31), date(2024, 1, 1), date(2024, 1, 3)]
    lake.items.ingest("value", pl.DataFrame({"time": times, "asset_id": ["A"] * len(times),
        "value": [1.] * len(times)}), available_date=times[0])
    if late:
        lake.items.ingest("value", pl.DataFrame({"time": [date(2024, 1, 1)], "asset_id": ["A"], "value": [9.]}),
            available_date="2024-01-04")
    request = {"value": ItemInput("value", start="2023-12-31", end="2024-01-31")}
    old = lake.inputs.freeze(request, information_cutoff="2024-01-02", retain_batch_bounds=True)
    current = lake.inputs.freeze(request, information_cutoff="2024-01-05", retain_batch_bounds=True)
    return lake, old, current


def test_metadata_delay_proof_trims_future_rows_without_decoding_indexes(tmp_path, monkeypatch):
    lake, old, current = setup(tmp_path)
    monkeypatch.setattr(input_index, "project_original", lambda *a, **k: pytest.fail("decoded row indexes"))
    assert lake.inputs.selection_equivalent(old, current, "value", end="2024-01-01", view="snapshot",
        values_only=True, records_only=True) is True
    # Schema 8 captures segment bounds even when no row index exists.
    with sqlite3.connect(lake.data_meta_path) as db:
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='input_batch_index'").fetchone() is None
    assert lake.inputs.selection_equivalent(old, current, "value", end="2024-01-01", view="snapshot",
        values_only=True, records_only=True) is True


def test_delay_proof_keeps_late_revision_unknown_and_audits_forged_bound(tmp_path):
    lake, old, current = setup(tmp_path, late=True)
    assert lake.inputs.selection_equivalent(old, current, "value", end="2024-01-01", view="snapshot",
        values_only=True, records_only=True) is None
    assert lake.inputs.read(old, "value", end="2024-01-01", view="snapshot").collect()["value"].to_list() != (
        lake.inputs.read(current, "value", end="2024-01-01", view="snapshot").collect()["value"].to_list())
    assert lake.inputs.verify(current)["valid"]
    with lake._data_meta.connect() as db:
        row = db.execute("SELECT * FROM version_segments ORDER BY commit_seq DESC LIMIT 1").fetchone()
        bounds = json.loads(row["bounds_json"])
        bounds["max_availability_delay_days"] = 0
        db.execute("UPDATE version_segments SET bounds_json=? WHERE commit_seq=?", (json.dumps(bounds), row["commit_seq"]))
    with pytest.raises(RuntimeError, match="bounds differ"):
        lake.inputs.verify(current)
