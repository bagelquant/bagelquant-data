"""Native timing follows consumed observations without rereading old originals."""
from datetime import UTC, date, datetime, timedelta
import hashlib

import polars as pl
from polars.testing import assert_frame_equal
import pytest

from bagelquant_data import (
    DataItemSpec, DataLake, DatasetSpec, ItemInput, ProducerUpdatePolicy, RawInput,
)
from bagelquant_data import input_index, inputs


FIRST, LAST, TAIL = date(2024, 1, 1), date(2024, 1, 20), date(2024, 1, 21)


def _lake(root):
    return DataLake.open(data_meta_path=root / "data.sqlite", lake_path=root / "lake")


def _spec(name="quotes", kind="by_date"):
    return DatasetSpec(name, kind, date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})


def _received(day):
    return datetime.combine(day, datetime.min.time(), UTC)


def _panel(days, *, assets=200, value=1.):
    return pl.DataFrame({
        "time": [day for day in days for _ in range(assets)],
        "asset_id": [f"A{index:03}" for _ in days for index in range(assets)],
        "close": [value] * (len(days) * assets),
        "marker": ["caller"] * (len(days) * assets),
        "time_right": [day for day in days for _ in range(assets)],
    })


def _originals(lake):
    return {path: (hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_mtime_ns)
            for path in lake.lake_path.rglob("*.parquet")}


def _watch_timing_projection(monkeypatch):
    projected = []
    original = input_index.project_original

    def record(store, batch, **kwargs):
        frame = original(store, batch, **kwargs)
        projected.append((frame.height, kwargs.get("start"), kwargs.get("end")))
        return frame

    monkeypatch.setattr(input_index, "project_original", record)
    return projected


@pytest.mark.parametrize("view", ["history", "snapshot", "versions"])
@pytest.mark.parametrize("tail_baseline", [False, True])
def test_real_first_visibility_append_uses_only_consumed_tail_timing(
    tmp_path, monkeypatch, view, tail_baseline,
):
    lake = _lake(tmp_path)
    raw = _spec()
    old = _panel([FIRST + timedelta(days=index) for index in range(20)])
    tail = _panel([TAIL], value=2.)
    initial = pl.concat([old, tail]) if tail_baseline else old
    lake.raw.ingest(raw, initial, mode="initialize", ingested_at=_received(LAST))
    request = RawInput("custom", "quotes", alias="raw", view=view)
    lake.items.register(DataItemSpec("close", (request,),
        time_column="source_time", value_column="close"))
    assert lake.items.initialize("close", start=FIRST, end=LAST).status == "success"
    retained = lake.inputs.freeze({"close": ItemInput("close", view="versions")},
        information_cutoff=LAST)
    old_selected = lake.inputs.read(retained, "close").collect()
    preserved = _originals(lake)

    # A genuine same-body verification creates a retained wide first-known
    # event, so the positive-only header proof really is uncertain.
    lake.raw.ingest(raw, old, ingested_at=_received(TAIL))
    if not tail_baseline:
        lake.raw.ingest(raw, tail, ingested_at=_received(TAIL))
    timing = lake.inputs.freeze({"raw": RawInput("custom", "quotes", view="versions")},
        information_cutoff=TAIL, retain_batch_bounds=True)
    assert any(event.get("event") for event in timing.evidence["raw"]["record_checks"])
    assert lake.inputs._proven_baseline_at(timing, "raw", TAIL) is None
    assert lake.items.plan_update("close", start=FIRST, end=TAIL) == [
        {"mode": "incremental", "start": TAIL.isoformat(), "end": TAIL.isoformat()}]

    projected = _watch_timing_projection(monkeypatch)
    reads = []
    original_read = inputs.read_batch

    def read(*args, **kwargs):
        frame = original_read(*args, **kwargs)
        reads.append((frame.height, kwargs.get("observation_start"), kwargs.get("observation_end")))
        return frame

    monkeypatch.setattr(inputs, "read_batch", read)
    report = lake.items.update("close", start=FIRST, end=TAIL)
    assert report.status == "success"
    assert reads == [(200, TAIL, TAIL)]
    assert projected == [], "Native timing must reuse the selected tail, not project 4,200 full-request rows"

    result = lake.items.read("close", start=TAIL, end=TAIL, view="snapshot", as_of=TAIL).collect()
    assert result.height == 200
    assert result["value"].unique().to_list() == [2.]
    assert result["marker"].unique().to_list() == ["caller"]
    assert result["time_right"].unique().to_list() == [TAIL]
    assert result["_baseline"].unique().to_list() == [tail_baseline]
    strict = lake.items.read("close", start=TAIL, end=TAIL, view="snapshot", as_of=TAIL, strict=True).collect()
    assert strict.height == (0 if tail_baseline else 200)
    assert result["available_date"].unique().to_list() == [TAIL]
    assert result["observation_date"].unique().to_list() == [TAIL]
    assert result["version_available_date"].unique().to_list() == [TAIL]
    assert lake.items.read("close", start=TAIL, end=TAIL, view="snapshot", as_of=LAST).collect().is_empty()
    assert_frame_equal(lake.inputs.read(retained, "close").collect(), old_selected)
    assert all(_originals(lake)[path] == value for path, value in preserved.items())

    # A full producer reference with the same admitted observation scope has
    # identical values, caller fields and timing, independent of creator IDs.
    reference_request = RawInput("custom", "quotes", view=view, start=TAIL, end=TAIL)
    lake.items.register(DataItemSpec("reference", (reference_request,),
        time_column="source_time", value_column="close"))
    lake.items.initialize("reference", start=TAIL, end=TAIL)
    reference = lake.items.read("reference", view="snapshot", as_of=TAIL).collect()
    fields = ["time", "asset_id", "value", "marker", "time_right", "observation_date",
              "available_date", "version_available_date", "_baseline"]
    assert_frame_equal(result.select(fields), reference.select(fields))

    # Completed repeats consume no values and never recalculate the prefix.
    def forbidden(*_args, **_kwargs):
        pytest.fail("Current repeat performed numerical/timing input work")

    monkeypatch.setattr(lake.inputs, "read", forbidden)
    monkeypatch.setattr(lake.inputs, "freeze", forbidden)
    monkeypatch.setattr(input_index, "project_original", forbidden)
    assert lake.items.update("close", start=FIRST, end=TAIL).status == "unchanged"


@pytest.mark.parametrize("view", ["history", "snapshot", "versions"])
def test_item_input_logical_snapshot_does_not_reselect_or_inherit_unconsumed_baseline(
    tmp_path, monkeypatch, view,
):
    lake = _lake(tmp_path)
    lake.items.register(DataItemSpec("leaf"))
    old = _panel([FIRST], assets=1).rename({"close": "value"})
    tail = _panel([TAIL], assets=1, value=2.).rename({"close": "value"})
    lake.items.ingest("leaf", old, historical_baseline=True, available_date=FIRST)
    lake.items.ingest("leaf", tail, available_date=TAIL)
    request = ItemInput("leaf", alias="leaf", view=view)
    observed = []

    def producer(context):
        observed.append((context.historical_baseline, context.frames["leaf"]))
        return context.frames["leaf"]

    lake.items.register_producer("identity", "1", producer,
        update_policy=ProducerUpdatePolicy("same_date"))
    lake.items.register(DataItemSpec("consumer", (request,),
        producer_key="identity", producer_revision="1"))
    projected = _watch_timing_projection(monkeypatch)
    assert lake.items.initialize("consumer", start=TAIL, end=TAIL).status == "success"
    assert projected == []
    assert observed and all(not baseline for baseline, _ in observed)
    assert all(frame["time"].to_list() == [TAIL] for _, frame in observed)
    result = lake.items.read("consumer", view="snapshot", as_of=TAIL, strict=True).collect()
    assert result["value"].to_list() == [2.]
    assert result["time_right"].to_list() == [TAIL]
    assert result["_baseline"].to_list() == [False]


def test_consumed_finite_halo_baseline_still_marks_verified_tail(tmp_path, monkeypatch):
    lake = _lake(tmp_path)
    raw = _spec()
    days = [LAST - timedelta(days=1), LAST, TAIL]
    calendar = _panel(days, assets=1).select("time", "asset_id").with_columns(pl.lit(1).alias("is_open"))
    lake.raw.ingest(_spec("calendar", "general"), calendar, ingested_at=_received(TAIL))
    lake.raw.ingest(raw, _panel(days[:2], assets=1), mode="initialize", ingested_at=_received(LAST))
    lake.raw.ingest(raw, _panel([TAIL], assets=1, value=2.), ingested_at=_received(TAIL))
    observed = []

    def producer(context):
        observed.append((context.historical_baseline, context.frames["raw"]["source_time"].to_list()))
        return context.frames["raw"].filter(pl.col("source_time") == TAIL).select(
            pl.col("source_time").alias("time"), "asset_id", pl.col("close").alias("value"))

    lake.items.register_producer("halo", "1", producer, update_policy=ProducerUpdatePolicy(
        "forward", history_observations=1, calendar_input="calendar"))
    lake.items.register(DataItemSpec("halo", (
        RawInput("custom", "quotes", alias="raw"),
        RawInput("custom", "calendar", alias="calendar"),
    ), producer_key="halo", producer_revision="1"))
    projected = _watch_timing_projection(monkeypatch)
    assert lake.items.initialize("halo", start=TAIL, end=TAIL).status == "success"
    assert observed == [(True, days)]
    assert projected == []
    result = lake.items.read("halo", view="snapshot", as_of=TAIL).collect()
    assert result["value"].to_list() == [2.]
    assert result["_baseline"].to_list() == [True]
    assert lake.items.read("halo", view="snapshot", as_of=TAIL, strict=True).collect().is_empty()


def test_late_consumed_withdrawal_keeps_cutoff_baseline_and_caller_trace(tmp_path, monkeypatch):
    lake = _lake(tmp_path)
    known, through = TAIL + timedelta(days=2), TAIL + timedelta(days=3)
    lake.items.register(DataItemSpec("leaf"))
    baseline = _panel([FIRST, TAIL], assets=1, value=2.).rename({"close": "value"})
    lake.items.ingest("leaf", baseline, historical_baseline=True, available_date=TAIL)
    retained = lake.inputs.freeze({"leaf": ItemInput("leaf", view="versions")}, information_cutoff=TAIL)
    retained_frame = lake.inputs.read(retained, "leaf").collect()
    preserved = _originals(lake)
    withdrawal = baseline.filter(pl.col("time") == TAIL).with_columns(
        pl.lit(None, dtype=pl.Float64).alias("value"), pl.lit(known).alias("available_date"))
    lake.items.ingest("leaf", withdrawal, available_date=known)
    observed = []

    def producer(context):
        observed.append((context.information_cutoff, context.historical_baseline,
                         context.frames["leaf"]["value"].to_list()))
        return context.frames["leaf"]

    lake.items.register_producer("late", "1", producer, update_policy=ProducerUpdatePolicy("same_date"))
    lake.items.register(DataItemSpec("late", (ItemInput("leaf", alias="leaf"),),
        producer_key="late", producer_revision="1"))
    projected = _watch_timing_projection(monkeypatch)
    assert lake.items.initialize("late", start=TAIL, end=through).status == "success"
    assert observed == [(known - timedelta(days=1), True, [2.]), (through, False, [None])]
    assert projected == []
    early = lake.items.read("late", view="snapshot", as_of=known - timedelta(days=1)).collect()
    assert early["value"].to_list() == [2.]
    assert early["_baseline"].to_list() == [True]
    assert lake.items.read("late", view="snapshot", as_of=known - timedelta(days=1), strict=True).collect().is_empty()
    latest = lake.items.read("late", view="snapshot", as_of=known, strict=True).collect()
    assert latest["value"].to_list() == [None]
    assert latest["_baseline"].to_list() == [False]
    assert latest["available_date"].to_list() == [known]
    assert latest["version_available_date"].to_list() == [known]
    assert latest["marker"].to_list() == ["caller"]
    assert latest["time_right"].to_list() == [TAIL]
    assert_frame_equal(lake.inputs.read(retained, "leaf").collect(), retained_frame)
    assert all(_originals(lake)[path] == value for path, value in preserved.items())
