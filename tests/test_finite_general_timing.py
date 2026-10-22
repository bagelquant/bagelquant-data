"""Finite causal producers reuse only positively proven native timing scopes."""
from dataclasses import replace
from datetime import UTC, date, datetime

import polars as pl
from polars.testing import assert_frame_equal
import pytest

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, ExecutionOptions, ItemInput, ProducerUpdatePolicy, RawInput
from test_general_consumed_fields import ban_originals


def day(value):
    return date(2025, 1, value)


def received(value):
    return datetime(2025, 1, value, tzinfo=UTC)


def prices(days):
    return pl.DataFrame({"time": [day(value) for value in days], "asset_id": ["A"] * len(days),
        "close": [100. + value for value in days]})


def setup(tmp_path, *, baseline_days=(2, 3, 4), verified_days=(), first=2, unit_days=3):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    calendar = DatasetSpec("calendar", "general")
    axis = pl.DataFrame({"time": [day(value) for value in range(1, 9)], "is_open": [1] * 8})
    lake.raw.ingest(calendar, axis, mode="initialize", ingested_at=received(4))
    quotes = DatasetSpec("quotes", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    if baseline_days:
        lake.raw.ingest(quotes, prices(baseline_days), mode="initialize", ingested_at=received(4))
    if verified_days:
        lake.raw.ingest(quotes, prices(verified_days), ingested_at=received(4))
    if not baseline_days and not verified_days:
        lake.raw.ingest(quotes, prices((2,)).head(0), mode="initialize", ingested_at=received(4))
    policy = ProducerUpdatePolicy("forward", history_observations=1, calendar_input="calendar",
        calendar_selector=("is_open", 1), timing_neutral_inputs=("calendar", "prices"))
    calls = []

    def producer(context):
        frame = context.frames["prices"].sort("asset_id", "source_time")
        calls.append((context.start, context.end, frame["source_time"].to_list()))
        return frame.select(pl.col("source_time").alias("time"), "asset_id",
            (pl.col("close") / pl.col("close").shift(1).over("asset_id") - 1).alias("value"))

    lake.items.register_producer("returns", "1", producer, update_policy=policy)
    lake.items.register(DataItemSpec("returns", (RawInput("custom", "quotes", alias="prices"),
        RawInput("custom", "calendar", alias="calendar")), producer_key="returns", producer_revision="1"))
    options = ExecutionOptions(max_buffer_bytes=8 * 1024**2, observation_window_days=unit_days)
    initial = lake.items.initialize("returns", start=day(first), end=day(4), config=options)
    assert initial.frozen_receipt_id is not None
    old = lake.inputs.get(initial.frozen_receipt_id)
    calls.clear()
    return lake, calendar, axis, quotes, policy, producer, calls, old, options


def forbid_values(patch, lake):
    ban_originals(patch, lake)
    patch.setattr(lake.inputs, "read", lambda *a, **k: pytest.fail("Timing planning consumed input values"))


def test_finite_calendar_promotion_only_executes_tail_and_matches_full_pit_reference(tmp_path, monkeypatch):
    lake, calendar, axis, quotes, policy, producer, calls, old, options = setup(tmp_path)
    original = lake.items.read("returns", view="versions").collect()
    completed = lake.items.completed_builds("returns")
    lake.raw.ingest(calendar, axis, ingested_at=received(5))
    lake.raw.ingest(quotes, prices((5,)), ingested_at=received(5))
    with DataLake.open(data_meta_path=lake.data_meta_path, lake_path=lake.lake_path, read_only=True) as reader:
        with monkeypatch.context() as patch:
            forbid_values(patch, reader)
            with pytest.raises(MemoryError, match="max_buffer_bytes"):
                reader.items.plan_update("returns", start=day(2), end=day(5), update_policy=policy,
                    config=replace(options, max_buffer_bytes=1024))
            assert reader.items.plan_update("returns", start=day(2), end=day(5), update_policy=policy,
                config=options) == [{"mode": "incremental", "start": str(day(5)), "end": str(day(5))}]
    assert not calls and lake.items.completed_builds("returns") == completed
    report = lake.items.update("returns", start=day(2), end=day(5), config=options)
    assert report.status == "success" and report.rows_committed == 1
    assert calls == [(day(5), day(5), [day(3), day(4), day(5)])]
    assert_frame_equal(lake.items.read("returns", end=day(4), view="versions").collect(), original)
    assert all(row in lake.items.completed_builds("returns") for row in completed)
    assert lake.inputs.get(old) == old
    lake.items.register_producer("reference", "1", producer, update_policy=policy)
    lake.items.register(DataItemSpec("reference", lake.items.get("returns").inputs,
        producer_key="reference", producer_revision="1"))
    lake.items.initialize("reference", start=day(2), end=day(5), config=options)
    fields = ["time", "asset_id", "value", "observation_date", "available_date", "version_available_date", "_baseline"]
    for cutoff in (day(4), day(5)):
        for strict in (False, True):
            for view in ("snapshot", "history"):
                assert_frame_equal(lake.items.read("returns", view=view, as_of=cutoff, strict=strict)
                    .collect().select(fields), lake.items.read("reference", view=view, as_of=cutoff, strict=strict)
                    .collect().select(fields))
    calls.clear()
    with monkeypatch.context() as patch:
        forbid_values(patch, lake)
        assert lake.items.update("returns", start=day(2), end=day(5), config=options).status == "unchanged"
    assert not calls


@pytest.mark.parametrize("scope,kwargs", [
    ("halo_only", {"baseline_days": (1,), "verified_days": (2, 3, 4)}),
    ("outside_halo", {"baseline_days": (1,), "verified_days": (2, 3, 4), "first": 4}),
    ("other_unit", {"baseline_days": (2,), "verified_days": (3, 4)}),
    ("appended_date", {"baseline_days": (5,), "verified_days": (2, 3, 4)}),
    ("fully_verified", {"baseline_days": (), "verified_days": (2, 3, 4)}),
    ("empty", {"baseline_days": (), "verified_days": ()}),
])
def test_finite_promotion_cannot_borrow_an_unproven_scope(tmp_path, monkeypatch, scope, kwargs):
    lake, calendar, axis, quotes, _, _, calls, _, options = setup(tmp_path, **kwargs)
    through = 5
    if scope == "other_unit":
        # Native finite initialization is one whole unit. A later append is
        # a separate real unit whose halo no longer includes the day-2 witness.
        lake.raw.ingest(quotes, prices((5,)), ingested_at=received(5))
        lake.items.update("returns", start=day(2), end=day(5), config=options)
        assert (day(5), day(5)) in [(first, last) for first, last, _ in calls]
        calls.clear()
        through = 6
    lake.raw.ingest(calendar, axis, ingested_at=received(through))
    completed = lake.items.completed_builds("returns")
    forbid_values(monkeypatch, lake)
    action, = lake.items.plan_update("returns", start=day(kwargs.get("first", 2)), end=day(through), config=options)
    assert action["mode"] == "blocked" and "timing" in action["reason"]
    with pytest.raises(RuntimeError, match="timing"):
        lake.items.update("returns", start=day(kwargs.get("first", 2)), end=day(through), config=options)
    assert not calls and lake.items.completed_builds("returns") == completed


@pytest.mark.parametrize("damage", ["bounds", "missing_proof"])
def test_finite_promotion_authenticates_original_native_units(tmp_path, monkeypatch, damage):
    lake, calendar, axis, _, _, _, calls, _, options = setup(tmp_path, unit_days=1)
    lake.raw.ingest(calendar, axis, ingested_at=received(5))
    with lake._data_meta.connect() as db:
        unit = db.execute("select id from item_builds where name='returns' and status='unit_success' order by id limit 1").fetchone()[0]
        if damage == "bounds":
            db.execute("update item_builds set end_date='2025-01-03' where id=?", (unit,))
        else:
            db.execute("delete from item_completion_proofs where build_id=?", (unit,))
    forbid_values(monkeypatch, lake)
    with pytest.raises(RuntimeError, match="completion proof source binding"):
        lake.items.plan_update("returns", start=day(2), end=day(5), config=options)
    assert not calls


@pytest.mark.parametrize("change", ["content", "backdated", "unknown_currentness"])
def test_finite_promotion_keeps_content_currentness_and_timing_guards(tmp_path, monkeypatch, change):
    lake, calendar, axis, _, _, _, calls, _, options = setup(tmp_path)
    changed = axis.with_columns(pl.when(pl.col("time") == day(3)).then(0).otherwise(pl.col("is_open")).alias("is_open")) if change == "content" else axis
    lake.raw.ingest(calendar, changed, ingested_at=received(4 if change == "backdated" else 5))
    if change == "unknown_currentness":
        monkeypatch.setattr(lake.inputs, "is_current", lambda *a, **k: None)
    forbid_values(monkeypatch, lake)
    action, = lake.items.plan_update("returns", start=day(2), end=day(5), config=options)
    assert action["mode"] == "blocked" and not calls


def test_finite_promotion_checks_intermediate_baseline_loss_and_restoration(tmp_path, monkeypatch):
    lake, calendar, axis, _, policy, _, calls, _, options = setup(tmp_path)
    lake.items.register(DataItemSpec("leaf"))
    original = prices((2, 3, 4)).rename({"close": "value"})
    lake.items.ingest("leaf", original, available_date=day(4), historical_baseline=True)

    def producer(context):
        calls.append((context.start, context.end))
        return context.frames["prices"].select("time", "asset_id", "value")

    lake.items.register_producer("leaf_consumer", "1", producer, update_policy=policy)
    lake.items.register(DataItemSpec("leaf_consumer", (ItemInput("leaf", alias="prices"),
        RawInput("custom", "calendar", alias="calendar")), producer_key="leaf_consumer", producer_revision="1"))
    initial = lake.items.initialize("leaf_consumer", start=day(2), end=day(4), config=options)
    assert initial.frozen_receipt_id is not None
    old = lake.inputs.get(initial.frozen_receipt_id)
    lake.raw.ingest(calendar, axis, ingested_at=received(5))
    lake.items.ingest("leaf", original, available_date=day(5))
    lake.items.ingest("leaf", original.head(1).with_columns(pl.lit("B").alias("asset_id")),
        available_date=day(6), historical_baseline=True)
    lake.raw.ingest(calendar, axis, ingested_at=received(6))
    current = lake.inputs.freeze(old.requests, information_cutoff=day(6), retain_batch_bounds=True)
    assert lake.inputs.is_current(old)
    assert [lake.inputs._baseline_at(current, "prices", day(value)) for value in (4, 5, 6)] == [True, False, True]
    calls.clear()
    forbid_values(monkeypatch, lake)
    action, = lake.items.plan_update("leaf_consumer", start=day(2), end=day(6), config=options)
    assert action["mode"] == "blocked" and "timing" in action["reason"] and not calls
