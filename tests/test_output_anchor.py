from datetime import UTC, date, datetime
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataLake, DataItemSpec, DatasetSpec, RawInput, ItemInput, ProducerUpdatePolicy


def setup(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    raw = DatasetSpec("daily", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    general = DatasetSpec("settings", "general")
    lake.raw.ingest(raw, pl.DataFrame({"time": [date(2024, 1, 4)], "asset_id": ["A"], "value": [10.]}),
        ingested_at=datetime(2024, 1, 4, tzinfo=UTC))
    lake.raw.ingest(general, pl.DataFrame({"asset_id": ["A"], "scale": [1.]}),
        ingested_at=datetime(2024, 1, 1, tzinfo=UTC))
    calls = []
    def producer(context):
        calls.append((context.start, context.end))
        prices = context.frames["daily"]
        if prices.is_empty():
            return pl.DataFrame(schema={"time": pl.Date, "asset_id": pl.String, "value": pl.Float64})
        return prices.join(context.frames["settings"], on="asset_id").select(
            pl.col("source_time").alias("time"), "asset_id", (pl.col("value") * pl.col("scale")).alias("value"))
    policy = ProducerUpdatePolicy("same_date", output_anchors=("daily",))
    lake.items.register_producer("p", "1", producer, update_policy=policy)
    inputs = (RawInput("custom", "daily", alias="daily"), RawInput("custom", "settings", alias="settings"))
    lake.items.register(DataItemSpec("derived", inputs, producer_key="p", producer_revision="1"))
    lake.items.update("derived", start="2024-01-01", end="2024-01-03")
    calls.clear()
    lake.raw.ingest(general, pl.DataFrame({"asset_id": ["A"], "scale": [2.]}),
        ingested_at=datetime(2024, 1, 4, tzinfo=UTC))
    return lake, raw, inputs, calls


def test_changed_general_with_no_anchor_coordinates_preserves_closed_empty_prefix(tmp_path):
    lake, _, inputs, calls = setup(tmp_path)
    original = lake.items.builds("derived")
    assert lake.items.plan_update("derived", start="2024-01-01", end="2024-01-05") == [
        {"mode": "incremental", "start": "2024-01-04", "end": "2024-01-05"}]
    lake.items.update("derived", start="2024-01-01", end="2024-01-05")
    assert calls and all(first >= date(2024, 1, 4) for first, _ in calls)
    assert all(row in lake.items.builds("derived") for row in original)
    lake.items.register(DataItemSpec("reference", inputs, producer_key="p", producer_revision="1"))
    lake.items.update("reference", start="2024-01-01", end="2024-01-05")
    columns = ["time", "asset_id", "value", "available_date", "observation_date", "_baseline"]
    assert lake.items.read("derived", view="snapshot", as_of="2024-01-05").collect().select(columns).equals(
        lake.items.read("reference", view="snapshot", as_of="2024-01-05").collect().select(columns))


@pytest.mark.parametrize("unknown_bounds", [False, True])
def test_general_change_remains_blocked_for_late_or_unknown_anchor_coordinates(tmp_path, unknown_bounds):
    lake, raw, _, calls = setup(tmp_path)
    if unknown_bounds:
        lake.raw.ingest(raw, pl.DataFrame({"time": [date(2024, 1, 4)], "asset_id": ["A"], "value": [11.]}),
            ingested_at=datetime(2024, 1, 5, tzinfo=UTC))
        with sqlite3.connect(lake.data_meta_path) as db:
            db.execute("update version_segments set min_observation=null where commit_seq="
                "(select max(seq) from version_commits where dataset='daily')")
    else:
        lake.raw.ingest(raw, pl.DataFrame({"time": [date(2024, 1, 2)], "asset_id": ["A"], "value": [5.]}),
            ingested_at=datetime(2024, 1, 4, tzinfo=UTC))
    plan = lake.items.plan_update("derived", start="2024-01-01", end="2024-01-05")
    reason = "observation bounds" if unknown_bounds else "full snapshot"
    assert plan[0]["mode"] == "blocked" and reason in plan[0]["reason"]
    with pytest.raises(RuntimeError, match=reason):
        lake.items.update("derived", start="2024-01-01", end="2024-01-05")
    assert not calls


def test_anchor_contract_is_enforced_before_any_output_publication(tmp_path):
    lake, _, inputs, _ = setup(tmp_path)
    lake.items.register_producer("bad", "1", lambda context: pl.DataFrame({
        "time": [date(2024, 1, 2)], "asset_id": ["A"], "value": [1.]}),
        update_policy=ProducerUpdatePolicy("same_date", output_anchors=("daily",)))
    lake.items.register(DataItemSpec("bad", inputs, producer_key="bad", producer_revision="1"))
    with pytest.raises(ValueError, match="outside its declared output anchor"):
        lake.items.update("bad", start="2024-01-01", end="2024-01-05")
    assert not lake.items.builds("bad") and not lake.items.manifest("bad")


def test_live_header_cannot_erase_original_retained_anchor_coordinates(tmp_path):
    lake, _, _, _ = setup(tmp_path)
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("update version_segments set min_observation='2024-01-02',max_observation='2024-01-02' where commit_seq=1")
    with pytest.raises(RuntimeError, match="original batch binding changed"):
        lake.items.plan_update("derived", start="2024-01-01", end="2024-01-05")


def test_union_anchors_preserve_outputs_from_either_input(tmp_path):
    lake, raw, inputs, _ = setup(tmp_path)
    other = DatasetSpec("suspension", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    lake.raw.ingest(other, pl.DataFrame({"time": [date(2024, 1, 2)], "asset_id": ["S"], "value": [0.]}),
        ingested_at=datetime(2024, 1, 2, tzinfo=UTC))
    def both(context):
        return pl.concat([context.frames[key].select(pl.col("source_time").alias("time"), "asset_id", "value")
            for key in ("daily", "suspension")])
    lake.items.register_producer("both", "1", both,
        update_policy=ProducerUpdatePolicy("same_date", output_anchors=("daily", "suspension")))
    lake.items.register(DataItemSpec("both", (*inputs, RawInput("custom", "suspension", alias="suspension")),
        producer_key="both", producer_revision="1"))
    lake.items.update("both", start="2024-01-01", end="2024-01-03")
    assert lake.items.read("both", view="snapshot", as_of="2024-01-03").collect()["asset_id"].to_list() == ["S"]
    # A nonempty second anchor prevents treating the completed prefix as empty.
    lake.raw.ingest(DatasetSpec("settings", "general"), pl.DataFrame({"asset_id": ["A"], "scale": [3.]}),
        ingested_at=datetime(2024, 1, 5, tzinfo=UTC))
    assert lake.items.plan_update("both", start="2024-01-01", end="2024-01-05")[0]["mode"] == "blocked"


def test_registered_empty_raw_anchor_needs_no_canonical_row_schema(tmp_path):
    lake, _, inputs, _ = setup(tmp_path)
    lake.raw.register(DatasetSpec("empty", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"}))
    lake.items.register_producer("empty_union", "1", lambda context: context.frames["daily"].select(
        pl.col("source_time").alias("time"), "asset_id", "value"),
        update_policy=ProducerUpdatePolicy("same_date", output_anchors=("daily", "empty")))
    lake.items.register(DataItemSpec("empty_union", (*inputs, RawInput("custom", "empty", alias="empty")),
        producer_key="empty_union", producer_revision="1"))
    assert lake.items.update("empty_union", start="2024-01-01", end="2024-01-05").rows_committed == 1


def test_invalid_item_anchor_is_rejected_before_initial_publication(tmp_path):
    lake, _, _, calls = setup(tmp_path)
    lake.items.register_producer("invalid", "1", lambda context: context.frames["derived"],
        update_policy=ProducerUpdatePolicy("same_date", output_anchors=("derived",)))
    lake.items.register(DataItemSpec("invalid", (ItemInput("derived"),), producer_key="invalid", producer_revision="1"))
    original = lake.items.builds("derived")
    for method in (lake.items.plan_update, lake.items.initialize, lake.items.update):
        with pytest.raises(ValueError, match="declared by-date Raw"):
            method("invalid", start="2024-01-01", end="2024-01-05")
    assert not lake.items.builds("invalid") and not lake.items.manifest("invalid")
    assert lake.items.builds("derived") == original and not calls
