"""Original receipt windows reuse checked index headers without index row reads."""
from concurrent.futures import CancelledError
from datetime import UTC, date, datetime
import sqlite3

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, ItemInput, RawInput
from bagelquant_data import input_index, inputs


DAYS = [date(2024, month, 2) for month in (1, 2, 3)]


def fixture(root, *, raw=False, assets=1):
    lake = DataLake.open(data_meta_path=root / "data.sqlite", lake_path=root / "lake")
    frame = pl.DataFrame({"time": DAYS, "asset_id": ["A"] * 3, "value": [1., 2., 3.],
        "available_date": DAYS, "observation_date": DAYS, "base_available_date": DAYS})
    if assets > 1:
        frame = frame.drop("asset_id").join(pl.DataFrame({"asset_id": [f"A_{index}" for index in range(assets)]}), how="cross")
    if raw:
        spec = DatasetSpec("x", "by_date", source="custom", date_kind="calendar",
            field_mappings={"time": "time", "asset_id": "asset_id"})
        for index, day in enumerate(DAYS):
            lake.raw.ingest(spec, frame.slice(index, 1), ingested_at=datetime.combine(day, datetime.min.time(), tzinfo=UTC))
        request = RawInput("custom", "x", view="snapshot")
    else:
        lake.items.register(DataItemSpec("x"))
        lake.items.ingest("x", frame)
        request = ItemInput("x", view="snapshot")
    frozen = lake.inputs.freeze({"x": request}, information_cutoff="2024-04-30")
    with sqlite3.connect(lake.data_meta_path) as db:
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='input_batch_index'").fetchone() is None
    return lake, frozen



@pytest.mark.parametrize("raw", [False, True])
def test_registered_segment_headers_support_bounded_original_reads(tmp_path, monkeypatch, raw):
    lake, frozen = fixture(tmp_path, raw=raw)
    expected = lake.inputs.read(frozen, "x", start="2024-02-01", end="2024-02-29").collect()
    before = lake.inputs.get(frozen)
    calls = []
    original = inputs.read_batch
    def observed(*args, **kwargs):
        calls.append(args[1])
        return original(*args, **kwargs)
    monkeypatch.setattr(inputs, "read_batch", observed)
    monkeypatch.setattr(input_index, "project_original", lambda *a, **k: pytest.fail("ordinary window planning audited history"))
    assert lake.inputs.window_read_supported(frozen, "x")
    assert calls == []
    actual = lake.inputs.read(frozen, "x", start="2024-02-01", end="2024-02-29").collect()
    assert_frame_equal(actual, expected)
    assert len(calls) == 1 and "month=02" in calls[0]
    assert lake.inputs.get(frozen) == before


def test_explicit_original_audit_detects_forged_header(tmp_path):
    lake, frozen = fixture(tmp_path)
    with lake._data_meta.connect() as db:
        db.execute("UPDATE version_segments SET content_hash='forged' WHERE commit_seq=1")
    with pytest.raises(RuntimeError, match="checksum"):
        lake.inputs.verify(frozen)


def test_window_read_cancellation_precedes_original_consumption(tmp_path, monkeypatch):
    lake, frozen = fixture(tmp_path)
    def canceled():
        raise CancelledError()
    monkeypatch.setattr(inputs, "read_batch", lambda *a, **k: pytest.fail("canceled operation consumed values"))
    with pytest.raises(CancelledError):
        with lake.inputs.read_context(frozen, check_canceled=canceled):
            lake.inputs.read(frozen, "x", start="2024-02-01", end="2024-02-29")
