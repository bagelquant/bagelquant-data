from datetime import UTC, date, datetime

import polars as pl

from bagelquant_data import DataLake, DataItemSpec, DatasetSpec, ItemInput, RawInput


def setup(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("daily", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    frame = pl.DataFrame({"time": [date(2024, 1, 1)], "asset_id": ["A"], "value": [1.]})
    lake.raw.ingest(spec, frame, mode="initialize", ingested_at=datetime(2024, 1, 1, tzinfo=UTC))
    lake.items.register(DataItemSpec("derived", (RawInput("custom", "daily"),), time_column="source_time"))
    lake.items.update("derived", start="2024-01-01", end="2024-01-02")
    frozen = lake.inputs.freeze({"derived": ItemInput("derived", start="2024-01-01", end="2024-01-02",
        view="versions")}, information_cutoff="2024-01-02")
    return lake, frozen


def test_new_complete_range_reuses_original_nonempty_versions_without_row_indexes(tmp_path, monkeypatch):
    lake, frozen = setup(tmp_path)
    before = lake.inputs.read(frozen, "derived").collect()
    original = lake.items.builds("derived")
    lake.items.update("derived", start="2024-01-01", end="2024-01-03")
    current = lake.inputs.freeze(frozen.requests, information_cutoff=frozen.information_cutoff)
    assert before.equals(lake.inputs.read(current, "derived").collect())
    assert all(row in lake.items.builds("derived") for row in original)
    def forbidden(*args, **kwargs):
        raise AssertionError("Nonempty Item versions currentness decoded bytes or row indexes")
    from bagelquant_data import inputs
    monkeypatch.setattr(inputs, "read_batch", forbidden)
    monkeypatch.setattr(lake.inputs, "_indexed_selection", forbidden)
    events = []
    assert lake.inputs.is_current(frozen, progress=events.append)
    assert any(event.get("item_versions_metadata_equal") for event in events)


def test_changed_item_versions_content_rejects_old_completed_descriptor(tmp_path):
    lake, frozen = setup(tmp_path)
    lake.items.ingest("derived", pl.DataFrame({"time": [date(2024, 1, 1)], "asset_id": ["A"], "value": [2.]}),
        available_date="2024-01-02", input_receipt=lake.inputs.freeze(
            {"custom/daily": RawInput("custom", "daily")}, information_cutoff="2024-01-02"))
    assert not lake.inputs.is_current(frozen)
