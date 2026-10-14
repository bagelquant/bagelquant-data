from datetime import UTC, date, datetime

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec, RawInput


def test_coordinate_identity_uses_index_and_detects_historical_calendar_changes(tmp_path, monkeypatch):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("calendar", "general")
    frame = pl.DataFrame({"time": [date(2024, 1, 1), date(2024, 1, 2)], "is_open": [1, 1]})
    lake.raw.ingest(spec, frame, ingested_at=datetime(2024, 1, 1, tzinfo=UTC))
    requests = {"calendar": RawInput("custom", "calendar", view="versions")}
    old = lake.inputs.freeze(requests, information_cutoff="2024-01-03")
    def proof(receipt):
        return lake.inputs.coordinate_identity(receipt, "calendar", selector=("is_open", 1), end="2024-01-02")
    assert proof(old) is None
    lake.inputs.index_coordinates(old, "calendar", selector=("is_open", 1))
    original = proof(old)
    lake.raw.ingest(spec, pl.concat([frame, pl.DataFrame({"time": [date(2024, 1, 4)], "is_open": [1]})]),
        ingested_at=datetime(2024, 1, 4, tzinfo=UTC))
    new = lake.inputs.freeze(requests, information_cutoff="2024-01-05")
    lake.inputs.index_coordinates(new, "calendar", selector=("is_open", 1))
    def forbidden(*args, **kwargs):
        raise AssertionError("Coordinate prerequisite consumed values or built an index")
    with monkeypatch.context() as patch:
        patch.setattr(lake.inputs, "read", forbidden)
        patch.setattr(lake.inputs, "index_coordinates", forbidden)
        assert proof(new) == original
        assert lake.inputs.coordinate_identity(new, "calendar", selector=("is_open", 1)) != lake.inputs.coordinate_identity(
            old, "calendar", selector=("is_open", 1))
    lake.raw.ingest(spec, frame.with_columns(pl.when(pl.col("time") == date(2024, 1, 2)).then(
        pl.lit(0, dtype=pl.Int64)).otherwise(pl.lit(1, dtype=pl.Int64)).alias("is_open")),
        ingested_at=datetime(2024, 1, 6, tzinfo=UTC))
    changed = lake.inputs.freeze(requests, information_cutoff="2024-01-06")
    lake.inputs.index_coordinates(changed, "calendar", selector=("is_open", 1))
    assert proof(changed) != original
    with pytest.raises(ValueError, match="precedes"):
        lake.inputs.coordinate_identity(old, "calendar", start="2024-01-03", end="2024-01-01")


def test_same_atomic_content_cannot_erase_original_coordinate_windows(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.raw.ingest(DatasetSpec("calendar", "general"), pl.DataFrame({
        "time": [date(2024, 1, 1), date(2024, 1, 2)], "is_open": [1, 1]}),
        ingested_at=datetime(2024, 1, 1, tzinfo=UTC))
    def frozen(day):
        return lake.inputs.freeze({"calendar": RawInput("custom", "calendar", start=day, end=day)},
            information_cutoff="2024-01-03")
    first, second = frozen("2024-01-01"), frozen("2024-01-02")
    for receipt in (first, second):
        lake.inputs.index_coordinates(receipt, "calendar", selector=("is_open", 1))
    assert lake.inputs.coordinate_equivalent(first, second, "calendar", selector=("is_open", 1)) is False
