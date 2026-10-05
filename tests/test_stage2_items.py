from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pytest

from bagelquant_data import (Align, Cast, DataItemSpec, DataLake, DatasetSpec, ExecutionOptions,
                            Filter, ItemInput, Join, MapValues, RawInput, Select, align_panel)
from bagelquant_data.transforms import apply_transforms
from bagelquant_data import AvailabilityAlignment, AvailabilityPolicy, materialize_daily_pit


def lake_at(tmp_path: Path) -> DataLake:
    return DataLake.open(data_meta_path=tmp_path / "meta" / "data_meta.sqlite", lake_path=tmp_path / "lake")


def raw_prices(lake: DataLake) -> DatasetSpec:
    spec = DatasetSpec("prices", "by_date", date_kind="calendar", field_mappings={"time":"time", "asset_id":"asset_id"})
    lake.raw.ingest(spec, pl.DataFrame({"time": [date(2020, 1, 1), date(2020, 1, 2)], "asset_id": ["A", "A"], "close": [1.0, 2.0]}), ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    return spec


def test_typed_items_reopen_and_dependency_identity(tmp_path):
    lake = lake_at(tmp_path)
    raw_prices(lake)
    spec = DataItemSpec("close", (RawInput("custom", "prices"),), time_column="source_time", value_column="close")
    lake.items.register(spec)
    first = lake.items.initialize("close", start="2020-01-01", end="2020-01-02")
    assert first.rows_committed == 2
    assert lake.items.read("close").collect().select("time", "asset_id", "value").rows() == [(date(2020, 1, 1), "A", 1.0), (date(2020, 1, 2), "A", 2.0)]
    assert lake.items.update("close", end="2020-01-02").status == "unchanged"
    reopened = lake_at(tmp_path)
    assert reopened.items.get("close") == spec
    assert reopened.items.status("close")["minimum_time"] == "2020-01-01"
    assert reopened.items.manifest("close")[0]["commit_seq"] == first.commit_seq


def test_item_history_snapshot_revision_null_and_strict(tmp_path):
    lake = lake_at(tmp_path)
    lake.items.register(DataItemSpec("category", value_dtype="string"))
    first = pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": ["old"]})
    lake.items.ingest("category", first, historical_baseline=True)
    lake.items.ingest("category", first.with_columns(pl.lit("new").alias("value")), available_date="2020-01-03")
    lake.items.ingest("category", first.with_columns(pl.lit(None, dtype=pl.String).alias("value")), available_date="2020-01-04")
    assert lake.items.read("category").collect()["value"].to_list() == ["old"]
    assert lake.items.read("category", view="snapshot", as_of="2020-01-03").collect()["value"].to_list() == ["new"]
    assert lake.items.read("category", view="snapshot", as_of="2020-01-04").collect()["value"].to_list() == [None]
    assert lake.items.read("category", strict=True).collect().is_empty()
    assert lake.items.read("category", view="versions").collect().height == 3
    with pytest.raises(ValueError, match="as_of"):
        lake.items.read("category", view="snapshot")


def test_items_reject_duplicates_cycles_and_dependencies(tmp_path):
    lake = lake_at(tmp_path)
    lake.items.register(DataItemSpec("first"))
    lake.items.register(DataItemSpec("second", (ItemInput("first"),)))
    with pytest.raises(ValueError, match="cycle"):
        lake.items.register(DataItemSpec("first", (ItemInput("second"),)))
    with pytest.raises(ValueError, match="dependencies"):
        lake.items.remove("first")
    duplicate = pl.DataFrame({"time": [date(2020, 1, 1)] * 2, "asset_id": ["A"] * 2, "value": [1.0, 2.0]})
    with pytest.raises(ValueError, match="duplicate"):
        lake.items.ingest("first", duplicate)
    lake.items.remove("second")
    lake.items.remove("first")
    assert lake.items.list() == []


def test_external_producer_revision_and_conservative_rebuild(tmp_path):
    lake = lake_at(tmp_path)
    raw_prices(lake)
    calls = []

    def producer(context):
        calls.append((context.start, context.end, context.max_commit))
        return context.frames["prices"].select(pl.col("source_time").alias("time"), "asset_id", pl.col("close").alias("value"))

    spec = DataItemSpec("external", (RawInput("custom", "prices", alias="prices"),), producer_key="test", producer_revision="one")
    lake.items.register(spec)
    with pytest.raises(ValueError, match="not registered"):
        lake.items.update("external", start="2020-01-01", end="2020-01-02")
    lake.items.register_producer("test", "one", producer)
    lake.items.update("external", start="2020-01-01", end="2020-01-02")
    count = len(calls)
    assert lake.items.update("external", end="2020-01-02").status == "unchanged"
    assert len(calls) == count
    lake.items.register(DataItemSpec("external", spec.inputs, producer_key="test", producer_revision="two"))
    lake.items.register_producer("test", "two", producer)
    lake.items.update("external", end="2020-01-02")
    assert len(calls) > count


def test_generic_transforms_and_bounded_null_event_fill():
    dates = [date(2020, 1, day) for day in range(1, 7)]
    coordinates = pl.DataFrame({"time": dates, "asset_id": ["A"] * 6})
    events = pl.DataFrame({"time": [dates[0], dates[2]], "asset_id": ["A", "A"], "value": [1.0, None], "available_date": [dates[1], dates[3]]})
    result = align_panel(events, coordinates, forward_fill_sessions=2)
    assert result["value"].to_list() == [None, 1.0, 1.0, None, None, None]
    assert align_panel(events, coordinates)["value"].null_count() == 6
    frame = pl.DataFrame({"k": [1, 2], "tag": ["x", "y"]})
    transformed = apply_transforms(frame, (Filter("k", "ge", 2), MapValues("tag", {"y": "z"}), Cast({"k": "float64"}), Select(("k", "tag"))), {})
    assert transformed.rows() == [(2.0, "z")]
    with pytest.raises(ValueError, match="explicit keys"):
        apply_transforms(frame, (Join("right", ()),), {"right": frame})
    with pytest.raises(ValueError, match="positive"):
        Align("coordinates", forward_fill_sessions=0)


def test_parallel_build_and_cancel_preserve_committed_work(tmp_path):
    lake = lake_at(tmp_path)
    raw = raw_prices(lake)
    lake.raw.ingest(raw, pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "close": [3.0]}), ingested_at=datetime(2020, 1, 3, tzinfo=UTC))
    def producer(context):
        return context.frames["custom/prices"].select(pl.col("source_time").alias("time"), "asset_id", pl.col("close").alias("value"))
    for name in ("serial", "parallel", "cancelled"):
        lake.items.register(DataItemSpec(name, (RawInput("custom", "prices"),), producer_key="test", producer_revision="1"))
    lake.items.register_producer("test", "1", producer)
    lake.items.update("serial", start="2020-01-01", end="2020-01-03")
    lake.items.update("parallel", start="2020-01-01", end="2020-01-03", config=ExecutionOptions(workers=2, max_in_flight=2))
    fields = ["time", "asset_id", "value", "version_available_date"]
    assert lake.items.read("serial", view="versions").collect().select(fields).equals(lake.items.read("parallel", view="versions").collect().select(fields))
    flag = [False]
    report = lake.items.update("cancelled", start="2020-01-01", end="2020-01-03", progress=lambda _: flag.__setitem__(0, True), cancelled=lambda: flag[0])
    assert report.status == "cancelled"
    assert report.rows_committed == 2
    assert lake.items.read("cancelled").collect().height == 2
    assert lake.items.update("cancelled", start="2020-01-01", end="2020-01-03").status == "success"


def test_item_definition_revision_preserves_evidence_and_tombstones(tmp_path):
    lake = lake_at(tmp_path)
    raw_prices(lake)
    inputs = (RawInput("custom", "prices"),)
    def full(context):
        return context.frames["custom/prices"].select(pl.col("source_time").alias("time"), "asset_id", pl.col("close").alias("value"))
    def partial(context):
        return full(context).filter(pl.col("time") == date(2020, 1, 1))
    lake.items.register(DataItemSpec("derived", inputs, producer_key="p", producer_revision="1"))
    lake.items.register_producer("p", "1", full)
    lake.items.update("derived", start="2020-01-01", end="2020-01-02")
    frozen = lake.inputs.freeze({"item": ItemInput("derived")})
    lake.items.register(DataItemSpec("derived", inputs, producer_key="p", producer_revision="2"))
    lake.items.register_producer("p", "2", partial)
    lake.items.update("derived", end="2020-01-02")
    result = lake.items.read("derived", view="snapshot", as_of="2020-01-02").collect()
    assert result["value"].to_list() == [1.0, None]
    assert result["_producer_revision"].to_list() == ["2", "2"]
    assert lake.inputs.read(frozen, "item").collect()["value"].to_list() == [1.0, 2.0]
    with pytest.raises(ValueError, match="scalar type is immutable"):
        lake.items.register(DataItemSpec("derived", value_dtype="string"))


def test_reopen_typed_transforms_and_read_only_mutations(tmp_path):
    lake = lake_at(tmp_path)
    raw_prices(lake)
    spec = DataItemSpec("after", (RawInput("custom", "prices"),), time_column="source_time", value_column="close", transforms=(Filter("source_time", "ge", date(2020, 1, 2)),))
    lake.items.register(spec)
    reopened = lake_at(tmp_path)
    assert reopened.items.get("after") == spec
    assert reopened.items.update("after", start="2020-01-01", end="2020-01-02").rows_committed == 1
    readonly = DataLake.open(data_meta_path=tmp_path / "meta" / "data_meta.sqlite", lake_path=tmp_path / "lake", read_only=True)
    assert readonly.items.read("after").collect()["value"].to_list() == [2.0]
    with pytest.raises(PermissionError):
        readonly.items.register(DataItemSpec("no"))
    with pytest.raises(PermissionError):
        readonly.inputs.freeze({})


def test_finite_session_fill_preserves_sparse_calendar_and_null_events():
    dates = [date(2020, 1, day) for day in (1, 3, 6, 8, 10)]
    calendar = pl.DataFrame({"time": dates})
    events = pl.DataFrame({"asset_id": ["A", "A"], "event_date": [dates[0], dates[2]], "known": [dates[0], dates[2]], "value": ["x", None]})
    policy = AvailabilityPolicy("event_date", "known", 0, AvailabilityAlignment.FORWARD_FILL, 2, "custom/calendar")
    result = materialize_daily_pit(events, calendar, policy, start=dates[0], end=dates[-1])
    assert result["time"].to_list() == dates[:4]
    assert result["value"].to_list() == ["x", "x", None, None]


@pytest.mark.parametrize("dtype,values", [("int64", [1, None]), ("boolean", [True, None]), ("date", [date(2020, 1, 1), None]), ("categorical", ["sector", None])])
def test_scalar_values_survive_storage_and_freeze(tmp_path, dtype, values):
    lake = lake_at(tmp_path)
    lake.items.register(DataItemSpec("typed", value_dtype=dtype))
    lake.items.ingest("typed", pl.DataFrame({"time": [date(2020, 1, 1), date(2020, 1, 2)], "asset_id": ["A", "A"], "value": values}), ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    receipt = lake.inputs.freeze({"typed": ItemInput("typed")})
    assert lake.items.read("typed").collect()["value"].to_list() == values
    assert lake.inputs.read(receipt, "typed").collect()["value"].to_list() == values
    assert lake.inputs.verify(receipt)["valid"]


def test_initialization_pins_definition_and_range_and_resumes(tmp_path):
    lake = lake_at(tmp_path)
    raw_prices(lake)
    spec = DataItemSpec("initial", (RawInput("custom", "prices"),), time_column="source_time", value_column="close")
    lake.items.register(spec)
    flag = [False]
    cancelled = lake.items.initialize("initial", start="2020-01-01", end="2020-01-02", cancelled=lambda: flag[0], progress=lambda _: flag.__setitem__(0, True))
    assert cancelled.status in {"success", "cancelled"}
    resumed = lake.items.initialize("initial", start="2020-01-01", end="2020-01-02")
    assert resumed.status in {"success", "unchanged"}
    assert resumed.frozen_receipt_id
    assert lake.inputs.verify(resumed.frozen_receipt_id)["valid"]
    with pytest.raises(ValueError, match="range and definition"):
        lake.items.initialize("initial", start="2020-01-01", end="2020-01-03")

