"""Resume legacy financial publications without as-of join helper collisions."""
from datetime import UTC, date, datetime
import math

import polars as pl
import pytest

from bagelquant_data import (
    AvailabilityAlignment, AvailabilityPolicy, DataItemSpec, DataLake,
    DatasetSpec, ItemInput, ProducerUpdatePolicy, RawInput, materialize_daily_pit,
)


def lake_at(root):
    return DataLake.open(data_meta_path=root / "meta.sqlite", lake_path=root / "lake")


def day(number):
    return date(2020, 1, number)


def daily_spec(name, **kwargs):
    return DatasetSpec(name, "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"}, **kwargs)


def test_late_visibility_with_business_suffixes_preserves_typed_natural_keys(tmp_path):
    lake = lake_at(tmp_path)
    spec = daily_spec("quotes", primary_key_extra=("variant",),
                      nullable_primary_key_extra=("variant",))
    frame = pl.DataFrame({
        "time": [day(1)] * 3, "asset_id": ["A"] * 3,
        "variant": [None, -0.0, 0.0], "value": [1.0, 2.0, 3.0],
        "time_right": [date(2019, 12, 31)] * 3,
        "asset_id_right": ["caller"] * 3, "value_right": [4.0] * 3,
    })
    lake.raw.ingest(spec, frame, mode="initialize",
                    ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    original = next(lake.lake_path.rglob("*.parquet"))
    original_bytes = original.read_bytes()
    before = lake.inputs.freeze({"raw": RawInput("custom", "quotes", view="snapshot")},
                               information_cutoff="2020-01-05")
    report = lake.raw.ingest(spec, frame.head(2),
                            ingested_at=datetime(2020, 1, 3, tzinfo=UTC))
    assert report.rows_committed == 0
    assert lake.raw.read("quotes", source="custom", view="latest", as_of=day(2),
                         strict=True).collect().is_empty()
    visible = lake.raw.read("quotes", source="custom", view="latest", as_of=day(3),
                            strict=True).collect().sort("value")
    assert visible["value"].to_list() == [1.0, 2.0]
    assert visible["variant"][0] is None
    assert math.copysign(1.0, visible["variant"][1]) == -1.0
    assert visible["time_right"].to_list() == [date(2019, 12, 31)] * 2
    assert visible["asset_id_right"].to_list() == ["caller"] * 2
    assert visible["value_right"].to_list() == [4.0] * 2
    assert lake.raw.ingest(spec, frame.head(2),
        ingested_at=datetime(2020, 1, 4, tzinfo=UTC)).rows_committed == 0
    events = list(lake.lake_path.rglob(".visibility/*.parquet"))
    assert len(events) == 1
    event = pl.read_parquet(events[0])
    assert set(event.columns) == {"source_time", "asset_id", "variant",
                                  "version_commit", "available_date"}
    assert event["available_date"].to_list() == [day(3)] * 2
    assert original.read_bytes() == original_bytes
    assert lake.inputs.read(before, "raw", strict=True).collect().is_empty()
    after = lake.inputs.freeze({"raw": RawInput("custom", "quotes", view="snapshot")},
                              information_cutoff="2020-01-05")
    assert lake.inputs.verify(after)["valid"]


@pytest.mark.parametrize("with_source_time", [False, True])
@pytest.mark.parametrize("caller_time_right", [False, True])
def test_forward_fill_retains_explicit_dates_and_caller_auxiliary_channels(
    with_source_time, caller_time_right,
):
    observations = pl.DataFrame({
        "asset_id": ["A", "A"], "value": [-0.0, None],
        "event_date": [date(2019, 12, 31), day(2)], "known": [day(1), day(3)],
    })
    if with_source_time:
        observations = observations.with_columns(pl.col("known").alias("time"))
    if caller_time_right:
        observations = observations.with_columns(
            pl.Series("time_right", [date(2019, 12, 20), date(2019, 12, 21)]))
    calendar = pl.DataFrame({"time": [day(value) for value in range(1, 6)],
                             "is_open": [1] * 5})
    policy = AvailabilityPolicy("event_date", "known", 1,
        AvailabilityAlignment.FORWARD_FILL, 2, "custom/calendar")
    panel = materialize_daily_pit(observations, calendar, policy, start=day(1), end=day(5))
    assert panel["time"].to_list() == [day(2), day(3), day(4), day(5)]
    assert panel["value"].to_list() == [-0.0, -0.0, None, None]
    assert all(math.copysign(1.0, value) == -1.0 for value in panel["value"].head(2))
    assert panel["observation_date"].to_list() == [date(2019, 12, 31)] * 2 + [day(2)] * 2
    assert panel["base_available_date"].to_list() == [day(1)] * 2 + [day(3)] * 2
    assert panel["available_date"].to_list() == [day(2)] * 2 + [day(4)] * 2
    assert not {"_time_index", "_available_index", "time_right_right"}.intersection(panel.columns)
    if caller_time_right:
        assert panel["time_right"].to_list() == [date(2019, 12, 20)] * 2 + [date(2019, 12, 21)] * 2
    else:
        assert "time_right" not in panel.columns


def test_resume_old_partial_native_body_then_publish_first_visibility(tmp_path, monkeypatch):
    lake = lake_at(tmp_path)
    financial_spec = daily_spec("income")
    financial = pl.DataFrame({"time": [day(2)], "asset_id": ["A"], "value": [1.0],
                              "period_end": [date(2019, 12, 31)], "report_type": ["1"]})
    lake.raw.ingest(financial_spec, financial, mode="initialize",
                    ingested_at=datetime(2020, 1, 5, tzinfo=UTC))
    calendar = pl.DataFrame({"time": [day(value) for value in range(1, 9)],
                             "is_open": [1] * 8})
    lake.raw.ingest(DatasetSpec("calendar", "general", field_mappings={"time": "time"}),
                    calendar, ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    inputs = (RawInput("custom", "income", alias="financial", view="versions",
                       include_historical_baseline=True),
              RawInput("custom", "calendar", alias="calendar", fields=("time", "is_open")))
    lake.items.register(DataItemSpec("financial_daily", inputs,
        producer_key="financial", producer_revision="1"))
    policy = AvailabilityPolicy("period_end", "publication_date", 0,
        AvailabilityAlignment.FORWARD_FILL, 3, "custom/calendar")
    calls = []

    def canonical_producer(context):
        calls.append((context.start, context.end))
        observations = context.frames["financial"].select(
            pl.col("source_time").alias("time"), "asset_id", "value", "period_end",
            pl.col("source_time").alias("publication_date"))
        return materialize_daily_pit(observations, context.frames["calendar"], policy,
                                     start=context.start, end=context.end)

    def legacy_producer(context):
        # Exactly the old Data forward-fill helper, already present in the
        # immutable first body when the live operation failed on its next unit.
        return canonical_producer(context).with_columns(
            pl.col("publication_date").alias("time_right"))

    update_policy = ProducerUpdatePolicy("forward",
        general_fields=(("calendar", ("time", "is_open")),))
    lake.items.register_producer("financial", "1", legacy_producer, update_policy=update_policy)
    stopped = False

    def progress(report):
        nonlocal stopped
        if report.rows_committed:
            stopped = True

    partial = lake.items.initialize("financial_daily", start=day(1), end=day(5),
        cancelled=lambda: stopped, progress=progress)
    assert partial.status == "cancelled" and partial.rows_committed == 3
    assert not lake.items.completion_statuses(names=["financial_daily"])[0]["publication_current"]
    with lake._data_meta.connect() as db:
        assert db.execute("select status from item_initializations where name='financial_daily'").fetchone()[0] == "running"
        assert db.execute("select update_incomplete from datasets where source='items' and name='financial_daily'").fetchone()[0] == 1
    originals = {path: path.read_bytes() for path in (lake.lake_path / "items").rglob("*.parquet")}
    assert len(originals) == 1
    assert "time_right" in pl.read_parquet(next(iter(originals)), hive_partitioning=False).columns
    original_schema = lake._parquet.canonical_schema("items", "financial_daily")
    frozen = lake.inputs.freeze({"item": ItemInput("financial_daily", view="snapshot")},
                               information_cutoff="2020-01-05")
    old_values = lake.inputs.read(frozen, "item").collect()

    lake.items.register_producer("financial", "1", canonical_producer, update_policy=update_policy)
    completed = lake.items.initialize("financial_daily", start=day(1), end=day(5))
    assert completed.status == "success" and completed.rows_committed == 0
    flag = lake.items.completion_statuses(names=["financial_daily"])[0]
    assert flag["definition_current"] and flag["publication_current"]
    assert flag["checked_target"] == "2020-01-05"
    assert flag["completed_start"] == "2020-01-01" and flag["completed_end"] == "2020-01-05"
    assert {path: path.read_bytes() for path in (lake.lake_path / "items").rglob("*.parquet")} == originals
    assert lake._parquet.canonical_schema("items", "financial_daily") == original_schema
    assert lake.inputs.read(frozen, "item").collect().equals(old_values)
    assert lake.items.read("financial_daily", view="snapshot", as_of=day(5), strict=True).collect().is_empty()
    called = len(calls)
    with monkeypatch.context() as patch:
        from bagelquant_data import input_index, inputs as input_api
        patch.setattr(input_api, "read_batch", lambda *a, **k: pytest.fail("completed initialization read original"))
        patch.setattr(input_index, "project_original", lambda *a, **k: pytest.fail("completed initialization projected original"))
        assert lake.items.initialize("financial_daily", start=day(1), end=day(5)).status == "unchanged"
    assert len(calls) == called

    lake.raw.ingest(financial_spec, financial, ingested_at=datetime(2020, 1, 6, tzinfo=UTC))
    source = lake.inputs.freeze({value.key: value for value in inputs}, information_cutoff=day(6))
    fixed_panel = materialize_daily_pit(financial.select("time", "asset_id", "value", "period_end").with_columns(
        pl.col("time").alias("publication_date")), calendar, policy, start=day(1), end=day(5))
    assert "time_right" not in fixed_panel.columns
    visible = lake.items.replace_range("financial_daily", fixed_panel, start=day(1), end=day(5),
                                      available_date=day(6), input_receipt=source)
    assert visible.rows_committed == 0
    assert lake.items.read("financial_daily", view="snapshot", as_of=day(5), strict=True).collect().is_empty()
    assert lake.items.read("financial_daily", view="snapshot", as_of=day(6), strict=True).collect()["value"].to_list() == [1.0] * 3
    assert lake.inputs.read(frozen, "item").collect().equals(old_values)
    assert all(path.read_bytes() == content for path, content in originals.items())
    value_files = [path for path in (lake.lake_path / "items").rglob("*.parquet") if ".visibility" not in path.parts]
    assert len(value_files) == 1
    assert lake._parquet.canonical_schema("items", "financial_daily") == original_schema
    for event_path in (lake.lake_path / "items").rglob(".visibility/*.parquet"):
        assert not {"time_right", "_time_index", "_available_index"}.intersection(pl.read_parquet(event_path).columns)
    current = lake.inputs.freeze({"item": ItemInput("financial_daily", view="snapshot")}, information_cutoff=day(6))
    assert lake.inputs.verify(current)["valid"]
