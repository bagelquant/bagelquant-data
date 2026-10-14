from datetime import UTC, date, datetime

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec, RawInput


@pytest.mark.parametrize("view", ["snapshot", "history", "versions"])
@pytest.mark.parametrize("strict", [False, True])
def test_future_raw_known_checks_keep_original_cutoff_without_indexes(tmp_path, monkeypatch, view, strict):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("prices", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    frame = pl.DataFrame({"time": [date(2024, 1, 1)], "asset_id": ["A"], "close": [1.]})
    lake.raw.ingest(spec, frame, mode="initialize", ingested_at=datetime(2024, 1, 1, tzinfo=UTC))
    request = {"prices": RawInput("custom", "prices", view=view, strict=strict,
        start="2024-01-01", end="2024-01-03")}
    frozen = lake.inputs.freeze(request, information_cutoff="2024-01-03")
    before = lake.inputs.read(frozen, "prices").collect()
    lake.raw.ingest(spec, frame, ingested_at=datetime(2024, 1, 4, tzinfo=UTC))
    current = lake.inputs.freeze(request, information_cutoff="2024-01-03")
    assert before.equals(lake.inputs.read(current, "prices").collect())
    def forbidden(*args, **kwargs):
        raise AssertionError("Raw currentness decoded bytes or optional row indexes")
    from bagelquant_data import inputs
    monkeypatch.setattr(inputs, "read_batch", forbidden)
    monkeypatch.setattr(lake.inputs, "_indexed_selection", forbidden)
    events = []
    assert lake.inputs.is_current(frozen, progress=events.append)
    assert any(event.get("raw_metadata_equal") for event in events)


@pytest.mark.parametrize("view", ["snapshot", "history", "versions"])
def test_raw_known_witness_inside_original_cutoff_invalidates(tmp_path, view):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("prices", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    frame = pl.DataFrame({"time": [date(2024, 1, 1)], "asset_id": ["A"], "close": [1.]})
    lake.raw.ingest(spec, frame, mode="initialize", ingested_at=datetime(2024, 1, 1, tzinfo=UTC))
    request = {"prices": RawInput("custom", "prices", view=view)}
    frozen = lake.inputs.freeze(request, information_cutoff="2024-01-03")
    before = lake.inputs.read(frozen, "prices").collect()
    lake.raw.ingest(spec, frame, ingested_at=datetime(2024, 1, 2, tzinfo=UTC))
    current = lake.inputs.freeze(request, information_cutoff="2024-01-03")
    equal = before.equals(lake.inputs.read(current, "prices").collect())
    assert equal == (view == "history")
    assert lake.inputs.is_current(frozen) == equal


def test_backdated_changed_content_is_not_equivalent_raw_metadata(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("prices", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    frame = pl.DataFrame({"time": [date(2024, 1, 1)], "asset_id": ["A"], "close": [1.]})
    lake.raw.ingest(spec, frame, mode="initialize", ingested_at=datetime(2024, 1, 1, tzinfo=UTC))
    frozen = lake.inputs.freeze({"prices": RawInput("custom", "prices", view="snapshot")}, information_cutoff="2024-01-03")
    lake.raw.ingest(spec, frame.with_columns(pl.lit(2.).alias("close")),
        ingested_at=datetime(2024, 1, 2, tzinfo=UTC))
    assert not lake.inputs.is_current(frozen)
