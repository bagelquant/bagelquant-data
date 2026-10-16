"""Native completion cancellation retains rows without certifying unfinished work."""
from datetime import UTC, date, datetime

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, ProducerUpdatePolicy, RawInput
from bagelquant_data.items import api


@pytest.mark.parametrize("stage", ["initialize_before", "initialize_saved", "update_unit", "update_whole"])
def test_late_cancellation_rolls_back_completion_and_retry_preserves_history(tmp_path, monkeypatch, stage):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    source = DatasetSpec("daily", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    lake.raw.ingest(source, pl.DataFrame({"time": [date(2020, 1, 2)],
        "asset_id": ["A"], "value": [2.]}), mode="initialize",
        ingested_at=datetime(2020, 1, 2, tzinfo=UTC))
    calls = []

    def producer(context):
        calls.append((context.start, context.end))
        return context.frames["daily"].select(pl.col("source_time").alias("time"),
            "asset_id", (pl.col("value") * 3).alias("value"))

    lake.items.register_producer("triple", "1", producer,
        update_policy=ProducerUpdatePolicy("same_date"))
    lake.items.register(DataItemSpec("item", (RawInput("custom", "daily", alias="daily"),),
        producer_key="triple", producer_revision="1"))
    updating = stage.startswith("update")
    if updating:
        lake.items.initialize("item", start="2020-01-01", end="2020-01-03")
        lake.raw.ingest(source, pl.DataFrame({"time": [date(2020, 1, 4)],
            "asset_id": ["A"], "value": [4.]}), ingested_at=datetime(2020, 1, 4, tzinfo=UTC))
    previous = lake.items.completed_builds("item")
    originals = {path: path.read_bytes() for path in lake.lake_path.rglob("*.parquet")}
    operation = lake.items.update if updating else lake.items.initialize
    end = "2020-01-04" if updating else "2020-01-03"
    canceled = False
    saved = 0
    save = api._save_completion
    compare = api._same_ordered_rows

    def after_save(*args, **kwargs):
        nonlocal canceled, saved
        result = save(*args, **kwargs)
        saved += 1
        if stage != "initialize_before" and saved == (2 if stage == "update_whole" else 1):
            canceled = True
        return result

    def before_completion(*args, **kwargs):
        nonlocal canceled
        result = compare(*args, **kwargs)
        if stage == "initialize_before":
            canceled = True
        return result

    with monkeypatch.context() as patch:
        patch.setattr(api, "_save_completion", after_save)
        patch.setattr(api, "_same_ordered_rows", before_completion)
        report = operation("item", start="2020-01-01", end=end, cancelled=lambda: canceled)
    assert canceled and report.status == "cancelled"
    completed = lake.items.completed_builds("item")
    assert completed[:len(previous)] == previous
    # A fully committed append unit may survive cancellation of the enclosing
    # range, but cancellation within its transaction must not certify it.
    assert len(completed) == len(previous) + (stage == "update_whole")
    assert not any(row["start_date"] == "2020-01-01" and row["end_date"] == end
        for row in completed)
    assert all(path.read_bytes() == body for path, body in originals.items())
    assert lake.items.read("item", view="snapshot", as_of=end).collect()["value"].to_list() == (
        [6., 12.] if updating else [6.])
    calls.clear()
    assert operation("item", start="2020-01-01", end=end).status in {"success", "unchanged"}
    if updating:
        assert all(start == date(2020, 1, 4) for start, _ in calls)
    assert lake.items.plan_update("item", start="2020-01-01", end=end) == []
