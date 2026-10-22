"""Caller admission covers timing-only proofs without replaying saved history."""
from concurrent.futures import CancelledError
from dataclasses import replace
from datetime import date

import pytest
from polars.testing import assert_frame_equal

from bagelquant_data import DataItemSpec, DataLake, ExecutionOptions, ProducerUpdatePolicy
from test_general_consumed_fields import append_daily, ban_originals, item_setup, received


def timing_item(tmp_path):
    fixture = item_setup(tmp_path, baseline=True, baseline_daily=True, consumed_fields=False,
        initialization_config=ExecutionOptions(observation_window_days=1))
    lake, spec, data, *_ = fixture
    lake.raw.ingest(spec, data, ingested_at=received(4))
    return fixture


@pytest.mark.parametrize("entry", ["plan_update", "update", "recertify", "update_many"])
def test_timing_only_proof_uses_caller_budget_before_value_work(tmp_path, monkeypatch, entry):
    lake, _, _, _, _, calls, _ = timing_item(tmp_path)
    options = ExecutionOptions(max_buffer_bytes=1024 * 1024)
    expected = options.max_buffer_bytes * 3 // 4 if entry == "update_many" else options.max_buffer_bytes
    capture, charged = lake.inputs._capture, []

    def admitted(db, requests, **kwargs):
        if set(requests) == {"metadata"}:
            charged.append(kwargs["max_buffer_bytes"])
            assert kwargs["max_buffer_bytes"] == expected
            raise MemoryError("caller metadata admission")
        return capture(db, requests, **kwargs)

    monkeypatch.setattr(lake.inputs, "_capture", admitted)
    ban_originals(monkeypatch, lake)
    before = lake.items.completed_builds("item")
    with pytest.raises(MemoryError, match="caller metadata admission"):
        getattr(lake.items, entry)(["item"] if entry == "update_many" else "item",
            start="2025-01-02", end="2025-01-04", config=options)
    assert charged == [expected] and not calls
    assert lake.items.completed_builds("item") == before


def test_timing_budget_failure_then_append_preserves_history_and_matches_reference(tmp_path, monkeypatch):
    options = ExecutionOptions(max_buffer_bytes=8 * 1024 * 1024, observation_window_days=2)
    lake, metadata, data, daily, producer, calls, old = item_setup(tmp_path,
        baseline=True, baseline_daily=True, consumed_fields=False, initialization_config=options)
    policy = ProducerUpdatePolicy("same_date", timing_neutral_inputs=("metadata",))
    lake.raw.ingest(metadata, data, ingested_at=received(4))
    append_daily(lake, daily)
    before = lake.items.completed_builds("item")
    original = lake.items.read("item", view="versions").collect()
    with DataLake.open(data_meta_path=lake.data_meta_path, lake_path=lake.lake_path, read_only=True) as reader:
        with monkeypatch.context() as patch:
            ban_originals(patch, reader)
            with pytest.raises(MemoryError, match="max_buffer_bytes"):
                reader.items.plan_update("item", start="2025-01-02", end="2025-01-04",
                    update_policy=policy, config=ExecutionOptions(max_buffer_bytes=1024))
            assert reader.items.plan_update("item", start="2025-01-02", end="2025-01-04",
                update_policy=policy, config=options) == [
                    {"mode": "incremental", "start": "2025-01-04", "end": "2025-01-04"}]
    assert not calls and lake.items.completed_builds("item") == before
    result = lake.items.update("item", start="2025-01-02", end="2025-01-04", config=options)
    assert result.status == "success" and result.rows_committed == 1
    assert calls == [(date(2025, 1, 4), date(2025, 1, 4))]
    assert_frame_equal(lake.items.read("item", end="2025-01-03", view="versions").collect(), original)
    assert all(row in lake.items.completed_builds("item") for row in before)
    assert lake.inputs.get(old).digest == old.digest

    lake.items.register_producer("reference", "1", producer)
    lake.items.register(DataItemSpec("reference", lake.items.get("item").inputs,
        producer_key="reference", producer_revision="1"))
    lake.items.initialize("reference", start="2025-01-02", end="2025-01-04", config=options)
    fields = ["time", "asset_id", "value", "observation_date", "available_date", "version_available_date"]
    for cutoff in ("2025-01-03", "2025-01-04"):
        assert_frame_equal(
            lake.items.read("item", view="snapshot", as_of=cutoff).collect().select(fields),
            lake.items.read("reference", view="snapshot", as_of=cutoff).collect().select(fields))
    # Baseline provenance is scoped to each consumed Source, so compare the
    # appended unit against a reference with the same bounded daily input.
    lake.items.register(DataItemSpec("tail_reference", tuple(
        replace(value, start="2025-01-04", end="2025-01-04") if value.key == "daily" else value
        for value in lake.items.get("item").inputs), producer_key="reference", producer_revision="1"))
    lake.items.initialize("tail_reference", start="2025-01-04", end="2025-01-04", config=options)
    for cutoff in ("2025-01-03", "2025-01-04"):
        for strict in (False, True):
            assert_frame_equal(
                lake.items.read("item", start="2025-01-04", view="snapshot", as_of=cutoff, strict=strict)
                    .collect().select(*fields, "_baseline"),
                lake.items.read("tail_reference", view="snapshot", as_of=cutoff, strict=strict)
                    .collect().select(*fields, "_baseline"))
    calls.clear()
    with monkeypatch.context() as patch:
        ban_originals(patch, lake)
        assert lake.items.update("item", start="2025-01-02", end="2025-01-04", config=options).status == "unchanged"
    assert not calls


@pytest.mark.parametrize("explicit", [False, True])
def test_planning_keeps_outer_admission_cancellation_and_progress(tmp_path, monkeypatch, explicit):
    lake, _, _, _, _, calls, old = timing_item(tmp_path)
    outer = ExecutionOptions(max_buffer_bytes=4 * 1024 * 1024)
    inner = ExecutionOptions(max_buffer_bytes=2 * 1024 * 1024) if explicit else None
    capture = lake.inputs._capture
    canceled, events, charged = False, [], []

    def check():
        if canceled:
            raise CancelledError("outer operation canceled")

    def observed(db, requests, **kwargs):
        if set(requests) == {"metadata"}:
            context = lake.inputs._read_context()
            assert context is not None and context.check_canceled is not None and context.progress is not None
            charged.append(kwargs["max_buffer_bytes"])
            context.progress({"stage": "timing metadata"})
            context.check_canceled()
        return capture(db, requests, **kwargs)

    monkeypatch.setattr(lake.inputs, "_capture", observed)
    ban_originals(monkeypatch, lake)
    with lake.inputs.read_context(old, config=outer, check_canceled=check, progress=events.append):
        lake.items.plan_update("item", start="2025-01-02", end="2025-01-04", config=inner)
        canceled = True
        with pytest.raises(CancelledError, match="outer operation canceled"):
            lake.items.plan_update("item", start="2025-01-02", end="2025-01-04", config=inner)
        canceled = False
    assert charged and set(charged) == {(inner or outer).max_buffer_bytes}
    assert events and not calls
