"""Public planning is read-only and preserves durable initialization evidence."""
from datetime import date

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, RawInput
from test_ledger_updates import LedgerSource, _daily_lake


def test_raw_plan_orders_prerequisites_and_does_not_publish(tmp_path):
    lake = _daily_lake(tmp_path, LedgerSource())
    before = lake.catalog.export_declarations()
    plan = lake.raw.plan_updates(["daily", "trade_cal"], source="custom",
                                start="2025-01-02", end="2025-01-03")
    assert [(a["dataset"], a["mode"]) for a in plan] == [
        ("trade_cal", "incremental"), ("daily", "initialize")]
    assert lake.catalog.export_declarations() == before
    assert lake.integrity.update_scopes(dataset="daily", source="custom") == []


def test_cancelled_raw_initialization_resumes_frozen_end_before_extension(tmp_path):
    lake = _daily_lake(tmp_path, LedgerSource())
    report = lake.raw.initialize("daily", source="custom", start="2025-01-02",
                                 end="2025-01-02", cancel_requested=lambda: True)
    assert report.status == "cancelled"
    plan = lake.raw.plan_updates(["daily"], source="custom", start="2025-01-02", end="2025-01-03")
    assert [(a["mode"], a["end"]) for a in plan] == [
        ("initialize", "2025-01-02"), ("incremental", "2025-01-03")]
    for action in plan:
        lake.raw.update("daily", source="custom", mode=action["mode"],
                        start=action["start"], end=action["end"])
    assert lake.raw.plan_updates(["daily"], source="custom", start="2025-01-02",
                                 end="2025-01-03")[0]["mode"] == "incremental"
    assert lake.raw.status("daily", source="custom")["row_count"] == 2


def test_completed_empty_raw_initialization_uses_incremental(tmp_path):
    lake = _daily_lake(tmp_path, LedgerSource(empty=True))
    lake.raw.initialize("daily", source="custom", start="2025-01-02", end="2025-01-03")
    assert lake.raw.status("daily", source="custom")["row_count"] == 0
    assert lake.raw.plan_updates(["daily"], source="custom", start="2025-01-02",
                                 end="2025-01-03")[0]["mode"] == "incremental"


def test_unfinished_raw_initialization_rejects_changed_bounds_and_definition(tmp_path):
    lake = _daily_lake(tmp_path, LedgerSource())
    lake.raw.initialize("daily", source="custom", start="2025-01-02", end="2025-01-03",
                        cancel_requested=lambda: True)
    with pytest.raises(ValueError, match="original"):
        lake.raw.plan_updates(["daily"], source="custom", start="2025-01-01", end="2025-01-03")
    with pytest.raises(ValueError, match="frozen"):
        lake.raw.plan_updates(["daily"], source="custom", start="2025-01-02", end="2025-01-02")
    from dataclasses import replace
    lake.raw.register(replace(lake.raw.get("daily", source="custom"), description="changed"))
    with pytest.raises(ValueError, match="original"):
        lake.raw.plan_updates(["daily"], source="custom", start="2025-01-02", end="2025-01-03")


@pytest.mark.parametrize("empty", [False, True])
def test_item_plan_resumes_initialization_and_handles_empty_builds(tmp_path, empty):
    lake = _daily_lake(tmp_path, LedgerSource(empty=empty))
    lake.raw.initialize("daily", source="custom", start="2025-01-02", end="2025-01-03")
    lake.items.register(DataItemSpec("close", (RawInput("custom", "daily"),),
        producer_key="fixture", producer_revision="1"))
    def producer(context):
        frame = pl.DataFrame({"time": [date(2025, 1, 2)], "asset_id": ["A"], "value": [10.0]})
        return frame.head(0) if empty else frame
    lake.items.register_producer("fixture", "1", producer)
    assert lake.items.plan_update("close", start="2025-01-02", end="2025-01-02")[0]["mode"] == "initialize"
    lake.items.initialize("close", start="2025-01-02", end="2025-01-02", cancelled=lambda: True)
    plan = lake.items.plan_update("close", start="2025-01-02", end="2025-01-03")
    assert [(a["mode"], a["end"]) for a in plan] == [
        ("initialize", "2025-01-02"), ("incremental", "2025-01-03")]
    for action in plan:
        api = lake.items.initialize if action["mode"] == "initialize" else lake.items.update
        api("close", start=action["start"], end=action["end"])
    assert lake.items.plan_update("close", start="2025-01-02", end="2025-01-03")[0]["mode"] == "incremental"
