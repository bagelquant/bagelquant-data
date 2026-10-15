"""Bounded public initialization preserves causal history and historical reuse."""
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from dataclasses import replace
from threading import get_ident

import polars as pl
import pytest

from bagelquant_data import DataLake, DataItemSpec, DatasetSpec, ExecutionOptions, ItemInput, ProducerUpdatePolicy, RawInput


FIRST, LAST = date(2020, 1, 1), date(2020, 1, 8)


def fixture(root, *, revisions=True, producers=True, calls=None, baseline=True):
    lake = DataLake.open(data_meta_path=root / "data.sqlite", lake_path=root / "lake")
    raw = DatasetSpec("prices", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    original = pl.DataFrame({"time": [FIRST + timedelta(days=day) for day in range(8)],
        "asset_id": ["A"] * 8, "close": [1., 2., 3., 4., -0., None, 7., 8.],
        "volume": [10., 20., 30., 40., 50., 60., 70., 80.]})
    lake.raw.ingest(raw, original, mode="initialize" if baseline else "incremental",
        ingested_at=datetime(2020, 1, 8 if baseline else 1, tzinfo=UTC))
    if revisions:
        lake.raw.ingest(raw, original.head(1).with_columns(pl.lit(9.).alias("close")),
            ingested_at=datetime(2020, 1, 5, tzinfo=UTC))
    for field in ("close", "volume"):
        def producer(context, field=field):
            if calls is not None:
                calls.append((field, context.start, context.end, context.information_cutoff))
            return context.frames["prices"].select(pl.col("source_time").alias("time"),
                "asset_id", pl.col(field).alias("value"), pl.col("time").alias("available_date"))
        if producers:
            lake.items.register_producer(field, "1", producer,
                update_policy=ProducerUpdatePolicy("same_date"), native_kind="columnar")
        lake.items.register(DataItemSpec(field, (RawInput("custom", "prices", alias="prices", fields=(field,)),),
            producer_key=field if producers else None, producer_revision="1" if producers else None,
            time_column="time" if producers else "source_time", value_column="value" if producers else field))
    return lake, raw, original


def semantic(lake, name, view, cutoff=None):
    frame = lake.items.read(name, view=view, as_of=cutoff).collect()
    fields = [field for field in ("time", "asset_id", "value", "observation_date", "available_date",
        "version_available_date", "_baseline") if field in frame.columns]
    return frame.select(fields).sort([field for field in fields if field != "value"])


def test_shared_windows_equal_whole_range_at_every_causal_cutoff(tmp_path):
    bounded, _, _ = fixture(tmp_path / "bounded")
    whole, _, _ = fixture(tmp_path / "whole")
    reports = bounded.items.initialize_many(("volume", "close"), start=FIRST, end=LAST,
        config=ExecutionOptions(workers=2, observation_window_days=2))
    for name in ("close", "volume"):
        whole.items.initialize(name, start=FIRST, end=LAST,
            config=ExecutionOptions(observation_window_days=None))
        assert reports[name].work["windows_built"] == 4
        assert reports[name].work["prepared_hits"] >= 4
        assert reports[name].work["key_rows_prepared"] == 9
        assert reports[name].work["prepared_peak_bytes"] <= 64 * 1024**2 // 4
        assert semantic(bounded, name, "versions").equals(semantic(whole, name, "versions"))
        assert semantic(bounded, name, "history").equals(semantic(whole, name, "history"))
        for day in range(8):
            cutoff = FIRST + timedelta(days=day)
            assert semantic(bounded, name, "snapshot", cutoff).equals(semantic(whole, name, "snapshot", cutoff))
        frozen = bounded.inputs.freeze({name: ItemInput(name)}, information_cutoff=LAST)
        assert bounded.inputs.verify(frozen)["valid"]


def test_small_default_range_coalesces_and_bounded_baselines_publish_in_bulk(tmp_path, monkeypatch):
    from bagelquant_data.items import api as item_api
    lake = DataLake.open(data_meta_path=tmp_path / "data.sqlite", lake_path=tmp_path / "lake")
    raw = DatasetSpec("prices", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    frame = pl.DataFrame({"time": [FIRST + timedelta(days=day) for day in range(70)],
        "asset_id": ["A"] * 70, "close": [float(day) for day in range(70)]})
    lake.raw.ingest(raw, frame, mode="initialize", ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    for name in ("small", "bounded"):
        lake.items.register(DataItemSpec(name, (RawInput("custom", "prices", fields=("close",)),),
            time_column="source_time", value_column="close"))
    commit = item_api.commit_versions
    groups = []
    def counted(spec, values, *args, **kwargs):
        groups.append(values.height)
        return commit(spec, values, *args, **kwargs)
    monkeypatch.setattr(item_api, "commit_versions", counted)
    last = FIRST + timedelta(days=69)
    small = lake.items.initialize("small", start=FIRST, end=last)
    assert small.work["windows_built"] == 1 and groups == [70]
    groups.clear()
    bounded = lake.items.initialize("bounded", start=FIRST, end=last,
        config=ExecutionOptions(observation_window_days=7))
    assert bounded.work["windows_built"] > 1
    assert len(groups) == bounded.work["windows_built"]
    assert sum(groups) == 70 and max(groups) <= 7
    for day in (0, 10, 40, 69):
        cutoff = FIRST + timedelta(days=day)
        assert semantic(lake, "small", "snapshot", cutoff).equals(semantic(lake, "bounded", "snapshot", cutoff))
    assert semantic(lake, "small", "versions").equals(semantic(lake, "bounded", "versions"))
    assert lake.items.read("bounded", strict=True).collect().is_empty()


def test_cancel_resume_keeps_source_and_completed_units_across_worker_settings(tmp_path, monkeypatch):
    calls = []
    lake, raw, original = fixture(tmp_path, revisions=False, calls=calls)
    build = lake.items._build
    stopped = False

    def interrupt(*args, **kwargs):
        nonlocal stopped
        report = build(*args, **kwargs)
        if report.status == "success":
            stopped = True
        return report

    monkeypatch.setattr(lake.items, "_build", interrupt)
    first = lake.items.initialize("close", start=FIRST, end=LAST,
        config=ExecutionOptions(observation_window_days=4), cancelled=lambda: stopped)
    assert first.status == "cancelled"
    assert not lake.items.completion_statuses(names=["close"])[0]["publication_current"]
    assert calls == [("close", FIRST, FIRST + timedelta(days=3), LAST)]
    # A later discovery cannot replace the already captured initialization Source.
    lake.raw.ingest(raw, original.head(1).with_columns(pl.lit(999.).alias("close")),
        ingested_at=datetime(2020, 1, 6, tzinfo=UTC))
    monkeypatch.setattr(lake.items, "_build", build)
    calls.clear()
    resumed = lake.items.initialize("close", start=FIRST, end=LAST,
        config=ExecutionOptions(workers=2, observation_window_days=2))
    assert resumed.status == "success"
    assert resumed.frozen_receipt_id == first.frozen_receipt_id
    assert resumed.work["windows_reused"] == 2
    assert all(start >= FIRST + timedelta(days=4) for _, start, _, _ in calls)
    assert semantic(lake, "close", "snapshot", LAST)["value"][0] == 1.
    # Historical completion is retained, but cannot claim current live inputs.
    assert not lake.items.completion_statuses(names=["close"])[0]["publication_current"]


def test_completed_repeat_is_metadata_only(tmp_path, monkeypatch):
    lake, _, _ = fixture(tmp_path)
    lake.items.initialize_many(("close", "volume"), start=FIRST, end=LAST)
    def forbidden(*args, **kwargs):
        raise AssertionError("unchanged initialization must not prepare or execute values")
    monkeypatch.setattr(lake.inputs, "read", forbidden)
    monkeypatch.setattr(lake.inputs, "freeze", forbidden)
    monkeypatch.setattr(lake.items, "_build", forbidden)
    reports = lake.items.initialize_many(("close", "volume"), start=FIRST, end=LAST,
        config=ExecutionOptions(workers=6, observation_window_days=1, native_admission=forbidden))
    assert all(report.status == "unchanged" and report.rows_committed == 0 for report in reports.values())


def test_daily_batch_append_shares_reads_and_does_not_rerun_history(tmp_path):
    calls = []
    lake, raw, original = fixture(tmp_path, revisions=False, calls=calls)
    lake.items.initialize_many(("close", "volume"), start=FIRST, end=LAST)
    calls.clear()
    day = LAST + timedelta(days=1)
    lake.raw.ingest(raw, original.tail(1).with_columns(pl.lit(day).alias("time")),
        ingested_at=datetime(2020, 1, 9, tzinfo=UTC))
    reports = lake.items.update_many(("close", "volume"), start=FIRST, end=day)
    assert all(report.status == "success" for report in reports.values())
    assert all(report.work["prepared_hits"] >= 1 for report in reports.values())
    assert calls and all(start == end == day for _, start, end, _ in calls)


def test_update_group_cancel_retry_reuses_completed_sibling_and_short_sessions(tmp_path, monkeypatch):
    calls = []
    lake, raw, original = fixture(tmp_path, baseline=False, revisions=False, calls=calls)
    lake.items.initialize_many(("close", "volume"), start=FIRST, end=LAST)
    retained = lake.items.completed_builds("close")[-1]
    next_day = LAST + timedelta(days=1)
    lake.raw.ingest(raw, original.head(1).with_columns(pl.lit(next_day).alias("time")),
        ingested_at=datetime(2020, 1, 9, tzinfo=UTC))
    stopped = False
    update = lake.items.update
    def stop_after_sibling(name, **kwargs):
        nonlocal stopped
        report = update(name, **kwargs)
        if name == "close":
            stopped = True
        return report
    monkeypatch.setattr(lake.items, "update", stop_after_sibling)
    reports = lake.items.update_many(("close", "volume"), start=FIRST, end=next_day,
        cancelled=lambda: stopped)
    assert reports["close"].status == "success" and reports["volume"].status == "cancelled"
    assert retained in lake.items.completed_builds("close")
    calls.clear()
    opened = 0
    connection = lake._data_meta._new_connection
    def counted():
        nonlocal opened
        opened += 1
        return connection()
    monkeypatch.setattr(lake._data_meta, "_new_connection", counted)
    reports = lake.items.update_many(("close", "volume"), start=FIRST, end=next_day)
    assert reports["close"].status == "unchanged" and reports["volume"].status == "success"
    assert calls == [("volume", next_day, next_day, next_day)]
    assert opened <= 3


def test_ready_group_rejects_transitive_selected_dependency_before_work(tmp_path, monkeypatch):
    lake, _, _ = fixture(tmp_path)
    for name, parent in (("middle", "close"), ("last", "middle")):
        lake.items.register(DataItemSpec(name, (ItemInput(parent),)))
    def forbidden(*args, **kwargs):
        pytest.fail("interdependent ready group started numerical work")
    monkeypatch.setattr(lake.items, "_build", forbidden)
    for action in (lake.items.initialize_many, lake.items.update_many):
        with pytest.raises(ValueError, match="independent"):
            action(("close", "last"), start=FIRST, end=LAST)


def test_mixed_native_admission_is_execution_only_and_serial(tmp_path):
    lake, _, _ = fixture(tmp_path)
    entries = []
    @contextmanager
    def admission(kind):
        entries.append((kind, get_ident()))
        yield
    before = lake.items.get("close")
    # Unknown producer kind is conservative even with several revision workers.
    lake.items._producer_native_kinds[("close", "1")] = "mixed"
    report = lake.items.initialize("close", start=FIRST, end=LAST,
        config=ExecutionOptions(workers=4, native_admission=admission))
    assert report.status == "success" and entries
    assert {thread for kind, thread in entries if kind == "mixed"} == {get_ident()}
    assert lake.items.get("close") == before
    assert ExecutionOptions(native_admission=admission) == ExecutionOptions()


def test_invalid_batch_does_not_initialize_any_item(tmp_path):
    lake, _, _ = fixture(tmp_path)
    with pytest.raises(ValueError, match="unique"):
        lake.items.initialize_many(("close", "close"), start=FIRST, end=LAST)
    assert not lake.items.builds("close")


def test_six_distinct_field_receipts_share_original_keys_without_widening(tmp_path):
    lake, raw, original = fixture(tmp_path, revisions=False)
    fields = tuple(f"field_{i}" for i in range(6))
    lake.raw.ingest(raw, original.with_columns(pl.lit(float(i)).alias(field) for i, field in enumerate(fields)),
        ingested_at=datetime(2020, 1, 8, tzinfo=UTC))
    for name in fields:
        lake.items.register(DataItemSpec(name, (RawInput("custom", "prices", alias="prices", fields=(name,)),),
            time_column="source_time", value_column=name))
    reports = lake.items.initialize_many(fields, start=FIRST, end=LAST,
        config=ExecutionOptions(observation_window_days=2))
    identities = set()
    for name in fields:
        receipt_id = reports[name].frozen_receipt_id
        assert receipt_id is not None
        receipt = lake.inputs.get(receipt_id)
        request = receipt.requests["prices"]
        assert isinstance(request, RawInput) and request.fields == (name,)
        identities.add(lake.inputs.request_identity(receipt, "prices"))
        assert reports[name].work["prepared_hits"] >= 20
        # Original baseline and schema-expanded revision rows are prepared once.
        assert reports[name].work["key_rows_prepared"] == 16
    assert len(identities) == 6


def test_original_overcaptured_completion_narrows_declared_fields_without_replay(tmp_path, monkeypatch):
    calls = []
    lake, raw, original = fixture(tmp_path, baseline=False, revisions=False, calls=calls)
    modern = lake.items._input_requests
    def overcaptured(*args, **kwargs):
        return {key: replace(value, fields=()) if isinstance(value, RawInput) else value
            for key, value in modern(*args, **kwargs).items()}
    monkeypatch.setattr(lake.items, "_input_requests", overcaptured)
    initialized = lake.items.initialize("close", start=FIRST, end=LAST)
    assert initialized.frozen_receipt_id is not None
    original_receipt = lake.inputs.get(initialized.frozen_receipt_id)
    request = original_receipt.requests["prices"]
    assert isinstance(request, RawInput) and request.fields == ()
    monkeypatch.setattr(lake.items, "_input_requests", modern)
    calls.clear()
    assert lake.items.update("close", start=FIRST, end=LAST).status == "unchanged"
    # Same PIT date, unchanged selected values/timing, only an unrelated column.
    lake.raw.ingest(raw, original.head(1).with_columns(pl.lit(999.).alias("volume")),
        ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    no_op = lake.items.update("close", start=FIRST, end=LAST)
    assert no_op.status == "unchanged" and calls == []
    assert lake.inputs.get(original_receipt).digest == original_receipt.digest
    assert no_op.frozen_receipt_id is not None
    request = lake.inputs.get(no_op.frozen_receipt_id).requests["prices"]
    assert isinstance(request, RawInput) and request.fields == ("close",)
    next_day = LAST + timedelta(days=1)
    lake.raw.ingest(raw, original.tail(1).with_columns(pl.lit(next_day).alias("time")),
        ingested_at=datetime(2020, 1, 9, tzinfo=UTC))
    assert lake.items.update("close", start=FIRST, end=next_day).status == "success"
    assert calls and all(start == end == next_day for _, start, end, _ in calls)


def test_sparse_events_sum_against_same_date_anchor_across_windows(tmp_path):
    def event_lake(root):
        lake, _, _ = fixture(root, revisions=False)
        events = DatasetSpec("events", "by_date", date_kind="calendar",
            field_mappings={"time": "time", "asset_id": "asset_id"}, primary_key_extra=("event",))
        frame = pl.DataFrame({"time": [FIRST + timedelta(days=2)] * 2,
            "asset_id": ["A", "A"], "event": [1, 2], "cash": [.2, .3]})
        lake.raw.ingest(events, frame, mode="initialize", ingested_at=datetime(2020, 1, 8, tzinfo=UTC))
        lake.raw.ingest(events, frame.head(1).with_columns(pl.lit(.4).alias("cash")),
            ingested_at=datetime(2020, 1, 6, tzinfo=UTC))
        def producer(context):
            anchor = context.frames["anchor"].select(pl.col("source_time").alias("time"), "asset_id")
            selected = context.frames["events"].group_by("source_time", "asset_id").agg(pl.col("cash").sum()).rename({"source_time": "time"})
            return anchor.join(selected, on=["time", "asset_id"], how="left").select("time", "asset_id", pl.col("cash").fill_null(0.).alias("value"))
        lake.items.register_producer("cash", "1", producer,
            update_policy=ProducerUpdatePolicy("same_date", output_anchors=("anchor",)), native_kind="columnar")
        lake.items.register(DataItemSpec("cash", (RawInput("custom", "prices", alias="anchor"),
            RawInput("custom", "events", alias="events", fields=("cash",))), producer_key="cash", producer_revision="1"))
        return lake
    bounded, whole = event_lake(tmp_path / "bounded"), event_lake(tmp_path / "whole")
    bounded.items.initialize("cash", start=FIRST, end=LAST, config=ExecutionOptions(observation_window_days=2))
    whole.items.initialize("cash", start=FIRST, end=LAST, config=ExecutionOptions(observation_window_days=None))
    for view in ("history", "versions", "snapshot"):
        assert semantic(bounded, "cash", view, LAST if view == "snapshot" else None).equals(
            semantic(whole, "cash", view, LAST if view == "snapshot" else None))
    assert semantic(bounded, "cash", "snapshot", LAST)["value"].to_list() == [0., 0., .7, 0., 0., 0., 0., 0.]


def test_bounded_causal_withdrawal_and_pending_visibility_match_reference(tmp_path):
    lakes = []
    for label in ("bounded", "whole"):
        lake, _, _ = fixture(tmp_path / label)
        def producer(context):
            selected = context.frames["prices"].filter(pl.col("close") < 5.)
            return selected.select(pl.col("source_time").alias("time"), "asset_id",
                pl.col("close").alias("value"),
                pl.when(pl.col("source_time") == FIRST + timedelta(days=1))
                .then(pl.lit(date(2020, 1, 10))).otherwise(pl.col("source_time")).alias("available_date"))
        lake.items.register_producer("selected", "1", producer,
            update_policy=ProducerUpdatePolicy("same_date"), native_kind="columnar")
        lake.items.register(DataItemSpec("selected", (RawInput("custom", "prices", alias="prices", fields=("close",)),),
            producer_key="selected", producer_revision="1"))
        lake.items.initialize("selected", start=FIRST, end=LAST,
            config=ExecutionOptions(observation_window_days=2 if label == "bounded" else None))
        lakes.append(lake)
    bounded, whole = lakes
    for cutoff in (date(2020, 1, 3), date(2020, 1, 5), LAST):
        assert semantic(bounded, "selected", "snapshot", cutoff).equals(semantic(whole, "selected", "snapshot", cutoff))
    assert semantic(bounded, "selected", "versions").equals(semantic(whole, "selected", "versions"))
    for lake in lakes:
        build = lake.items.completed_builds("selected")[-1]
        from bagelquant_data.items.completion import pending_dates
        with lake.items._store.connect() as db:
            assert pending_dates(db, build["id"]) == [("2020-01-02", "2020-01-10")]
        lake.items.update("selected", start=FIRST, end="2020-01-10")
    assert semantic(bounded, "selected", "versions").equals(semantic(whole, "selected", "versions"))


def test_cancel_during_final_metadata_save_rolls_back_whole_authority(tmp_path, monkeypatch):
    from bagelquant_data.items import initialization
    calls = []
    lake, _, _ = fixture(tmp_path, revisions=False, calls=calls)
    save = initialization.save_completion
    stopped = False
    def cancel_after_save(*args, **kwargs):
        nonlocal stopped
        save(*args, **kwargs)
        stopped = True
    monkeypatch.setattr(initialization, "save_completion", cancel_after_save)
    report = lake.items.initialize("close", start=FIRST, end=LAST,
        config=ExecutionOptions(observation_window_days=2), cancelled=lambda: stopped)
    assert report.status == "cancelled"
    assert lake.items.completed_builds("close") == []
    assert not lake.items.completion_statuses(names=["close"])[0]["publication_current"]
    monkeypatch.setattr(initialization, "save_completion", save)
    calls.clear()
    resumed = lake.items.initialize("close", start=FIRST, end=LAST,
        config=ExecutionOptions(observation_window_days=1))
    assert resumed.status == "success" and calls == []
    assert resumed.work["windows_reused"] == 8


def test_resume_repairs_overridden_unit_and_reuses_disjoint_completed_unit(tmp_path, monkeypatch):
    calls = []
    lake, _, _ = fixture(tmp_path, revisions=False, baseline=False, calls=calls)
    build = lake.items._build
    count = 0
    def interrupt(*args, **kwargs):
        nonlocal count
        report = build(*args, **kwargs)
        count += report.status == "success"
        return report
    monkeypatch.setattr(lake.items, "_build", interrupt)
    partial = lake.items.initialize("close", start=FIRST, end=LAST,
        config=ExecutionOptions(observation_window_days=2), cancelled=lambda: count >= 2)
    assert partial.status == "cancelled" and partial.frozen_receipt_id is not None
    lake.items.ingest("close", pl.DataFrame({"time": [FIRST], "asset_id": ["A"], "value": [999.]}),
        available_date=LAST, input_receipt=partial.frozen_receipt_id)
    overridden = lake.inputs.freeze({"close": ItemInput("close")}, information_cutoff=LAST)
    assert lake.inputs.read(overridden, "close", view="snapshot").collect().sort("time")["value"][0] == 999.
    monkeypatch.setattr(lake.items, "_build", build)
    calls.clear()
    report = lake.items.initialize("close", start=FIRST, end=LAST,
        config=ExecutionOptions(observation_window_days=2))
    assert report.status == "success"
    assert any(start == FIRST for _, start, _, _ in calls)
    assert all(start != FIRST + timedelta(days=2) for _, start, _, _ in calls)
    assert semantic(lake, "close", "snapshot", LAST)["value"][0] == 1.
    assert lake.inputs.read(overridden, "close", view="snapshot").collect().sort("time")["value"][0] == 999.
