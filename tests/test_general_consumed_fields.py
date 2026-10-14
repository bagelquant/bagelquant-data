from __future__ import annotations

from concurrent.futures import CancelledError
from dataclasses import replace
from datetime import UTC, date, datetime

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from bagelquant_data import DataLake, DatasetSpec, ExecutionOptions, InputSelectionScope
from bagelquant_data import input_index, inputs
from bagelquant_data.items import DataItemSpec, ProducerUpdatePolicy, RawInput

FIELDS = ("asset_id", "market")
DAY = date(2025, 1, 1)


def received(day):
    return datetime(2025, 1, day, tzinfo=UTC)


def frame():
    return pl.DataFrame({"asset_id": ["A", "B"], "market": ["main", "growth"],
        "act_name": ["old A", "old B"]})


def setup(tmp_path, *, baseline=False, data=None, spec=None):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("metadata", "general") if spec is None else spec
    data = frame() if data is None else data
    lake.raw.ingest(spec, data, mode="initialize" if baseline else "incremental", ingested_at=received(2))
    old = lake.inputs.freeze({"metadata": RawInput("custom", "metadata")}, information_cutoff="2025-01-03")
    return lake, spec, data, old


def freeze(lake, old, day=4):
    return lake.inputs.freeze(old.requests, information_cutoff=date(2025, 1, day))


def index(lake, receipt, **kwargs):
    return lake.inputs.index_selection(receipt, "metadata", view="snapshot", fields=FIELDS, **kwargs)


def ban_originals(monkeypatch, lake):
    def forbidden(*_args, **_kwargs):
        pytest.fail("ordinary General consumption proof read original bytes or row indexes")
    monkeypatch.setattr(input_index, "project_original", forbidden)
    monkeypatch.setattr(input_index, "project_original", forbidden)
    monkeypatch.setattr(inputs, "read_batch", forbidden)
    monkeypatch.setattr(pl, "read_ipc", forbidden)
    assert not hasattr(lake.inputs, "build_index")


def remove_rows(lake):
    with lake._data_meta.connect() as db:
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='input_batch_index'").fetchone() is None
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='input_batch_value_index'").fetchone() is None


def test_whole_general_unused_field_change_uses_only_registered_typed_summaries(tmp_path, monkeypatch):
    lake, spec, data, old = setup(tmp_path)
    original = lake.inputs.get(old)
    first = index(lake, old)
    lake.raw.ingest(spec, data.with_columns(pl.lit("new actor").alias("act_name")), ingested_at=received(4))
    new = freeze(lake, old)
    assert lake.inputs.request_identity(old, "metadata") != lake.inputs.request_identity(new, "metadata")
    remove_rows(lake)
    work = []
    tokens = lake.inputs.index_selections(new, [InputSelectionScope("metadata", view="snapshot", fields=FIELDS)] * 2,
        progress=work.append)
    assert first is not None and tokens == (first, first)
    assert work[-1]["batches"] == 1 and work[-1]["original_rows"] == data.height
    assert lake.inputs.verify([old, new])["valid"]
    ban_originals(monkeypatch, lake)
    assert lake.inputs.selection_equivalent(old, new, "metadata", view="snapshot", fields=FIELDS) is True
    assert lake.inputs.selection_identity(new, "metadata", view="snapshot", fields=FIELDS) == lake.inputs.selection_identity(old, "metadata", view="snapshot", fields=FIELDS)
    assert lake.inputs.selection_equivalent(old, new, "metadata", view="snapshot", records_only=True) is None
    assert lake.inputs.get(old) == original
    skipped = []
    assert lake.inputs.index_selections(new, [InputSelectionScope("metadata", view="snapshot", fields=FIELDS)],
        progress=skipped.append) == (first,)
    assert skipped[-1]["skipped"] == 1 and skipped[-1]["batches"] == 0


@pytest.mark.parametrize("change", ["market", "new_asset", "duplicate"])
def test_general_actual_consumption_changes_preserve_row_multiplicity(tmp_path, monkeypatch, change):
    spec = DatasetSpec("metadata", "general", primary_key_extra=("act_name",)) if change == "duplicate" else None
    lake, spec, data, old = setup(tmp_path, spec=spec)
    first = index(lake, old)
    if change == "market":
        changed = data.with_columns(pl.lit("other").alias("market"))
    elif change == "new_asset":
        changed = pl.concat([data, data.head(1).with_columns(pl.lit("C").alias("asset_id"))])
    else:
        # General extra primary keys allow several rows at one asset. The
        # consumer projection must retain their full multiplicity.
        changed = pl.concat([data, data.head(1).with_columns(pl.lit("another").alias("act_name"))])
    lake.raw.ingest(spec, changed, ingested_at=received(4))
    new = freeze(lake, old)
    assert index(lake, new) != first
    ban_originals(monkeypatch, lake)
    assert lake.inputs.selection_equivalent(old, new, "metadata", view="snapshot", fields=FIELDS) is False


def test_missing_general_summary_is_unknown_and_never_decodes_originals(tmp_path, monkeypatch):
    lake, spec, data, old = setup(tmp_path)
    lake.raw.ingest(spec, data.with_columns(pl.lit("new").alias("act_name")), ingested_at=received(4))
    new = freeze(lake, old)
    ban_originals(monkeypatch, lake)
    assert lake.inputs.selection_identity(old, "metadata", view="snapshot", fields=FIELDS) is not None
    assert lake.inputs.selection_equivalent(old, new, "metadata", view="snapshot", fields=FIELDS) is None
    assert lake.inputs.selection_equivalent(old, old, "metadata", view="snapshot", fields=FIELDS,
        records_only=True) is True


@pytest.mark.parametrize("view,kwargs", [("history", {}), ("versions", {"causal_versions": True}),
    ("snapshot", {"start": DAY}), ("snapshot", {"coordinates": pl.DataFrame(schema={"time": pl.Date, "asset_id": pl.String})})])
def test_general_projection_rejects_other_selection_contracts_before_original_reads(tmp_path, monkeypatch, view, kwargs):
    lake, _, _, old = setup(tmp_path)
    ban_originals(monkeypatch, lake)
    with pytest.raises(ValueError, match="whole snapshot"):
        lake.inputs.index_selection(old, "metadata", view=view, fields=FIELDS, **kwargs)
    with pytest.raises(ValueError, match="whole snapshot"):
        lake.inputs.selection_identity(old, "metadata", view=view, fields=FIELDS, **kwargs)


def test_general_original_snapshot_schema_rejects_conflicting_future_consumed_types(tmp_path, monkeypatch):
    data = frame().with_columns(pl.lit(1, dtype=pl.Int64).alias("quality"))
    lake, spec, data, old = setup(tmp_path, data=data)
    fields = (*FIELDS, "quality")
    first = lake.inputs.index_selection(old, "metadata", view="snapshot", fields=fields)
    lake.raw.ingest(spec, data.with_columns(pl.lit(1.0, dtype=pl.Float64).alias("quality"),
        pl.lit(7).alias("future_only")), ingested_at=received(5))
    new = freeze(lake, old, 5)
    ban_originals(monkeypatch, lake)
    for cutoff in ("2025-01-03", "2025-01-05"):
        with pytest.raises(ValueError, match="conflicting captured atomic schema"):
            lake.inputs.index_selection(new, "metadata", view="snapshot", as_of=cutoff, fields=fields)
    assert lake.inputs.index_selection(old, "metadata", view="snapshot", fields=fields) == first
    with pytest.raises(ValueError, match="missing requested fields"):
        lake.inputs.selection_identity(new, "metadata", view="snapshot", as_of="2025-01-03", fields=(*fields, "future_only"))


def test_general_mixed_consumed_schema_never_certifies_a_coerced_original_read(tmp_path, monkeypatch):
    exact = 2**53 + 1
    data = frame().with_columns(pl.lit(exact, dtype=pl.Int64).alias("score"))
    lake, spec, data, old = setup(tmp_path, data=data)
    original = lake.inputs.get(old)
    fields = (*FIELDS, "score")
    token = lake.inputs.index_selection(old, "metadata", view="snapshot", fields=fields)
    lake.raw.ingest(spec, data.with_columns(pl.lit(1.0, dtype=pl.Float64).alias("score")), ingested_at=received(5))
    new = freeze(lake, old, 3)
    before = lake.inputs.read(old, "metadata", view="snapshot", fields=fields).collect()
    after = lake.inputs.read(new, "metadata", view="snapshot", fields=fields).collect()
    assert before.schema == after.schema and before.schema["score"] == pl.Int64
    assert before["score"].to_list() == [exact, exact]
    assert after["score"].to_list() == [exact - 1, exact - 1]
    with lake._data_meta.connect() as db:
        summaries = db.execute("select count(*) from input_projection_index").fetchone()[0]
    ban_originals(monkeypatch, lake)
    assert lake.inputs.index_selection(old, "metadata", view="snapshot", fields=fields) == token
    for operation in (lake.inputs.selection_identity, lake.inputs.index_selection):
        with pytest.raises(ValueError, match="conflicting captured atomic schema"):
            operation(new, "metadata", view="snapshot", fields=fields)
    # The selected event/batch/check binding matches the already registered
    # token. Admission must reject before either cached-summary or exact-header
    # equality can become a positive proof.
    for current in (old, new):
        with pytest.raises(ValueError, match="conflicting captured atomic schema"):
            lake.inputs.selection_equivalent(new, current, "metadata", view="snapshot", fields=fields,
                records_only=True, summaries_only=True)
    with lake._data_meta.connect() as db:
        assert db.execute("select count(*) from input_projection_index").fetchone()[0] == summaries
    assert lake.inputs.get(old) == original


def test_general_unconsumed_schema_variation_keeps_consumed_field_proof(tmp_path, monkeypatch):
    data = frame().with_columns(pl.lit(2**53 + 1, dtype=pl.Int64).alias("score"))
    lake, spec, data, old = setup(tmp_path, data=data)
    first = index(lake, old)
    lake.raw.ingest(spec, data.with_columns(pl.lit(1.0, dtype=pl.Float64).alias("score"),
        pl.lit("new field").alias("future_only")), ingested_at=received(5))
    new = freeze(lake, old, 5)
    assert_frame_equal(lake.inputs.read(old, "metadata", view="snapshot", fields=FIELDS).collect(),
        lake.inputs.read(new, "metadata", view="snapshot", fields=FIELDS).collect())
    assert index(lake, new) == first
    ban_originals(monkeypatch, lake)
    assert lake.inputs.selection_equivalent(old, new, "metadata", view="snapshot", fields=FIELDS) is True


def test_general_unknown_captured_schema_is_unsupported_before_cached_proof(tmp_path, monkeypatch):
    lake, spec, data, old = setup(tmp_path)
    first = index(lake, old)
    lake.raw.ingest(spec, data.with_columns(pl.lit("future actor").alias("act_name")), ingested_at=received(5))
    # Isolated legacy metadata fixture: an original future atomic header was
    # retained without schema authority, rather than fabricated on a receipt.
    with lake._data_meta.connect() as db:
        db.execute("update version_segments set schema_ipc=x'' where commit_seq in "
            "(select seq from version_commits where source='custom' and dataset='metadata' and seq>?)",
            (old.max_commit,))
    current = freeze(lake, old, 3)
    ban_originals(monkeypatch, lake)
    assert lake.inputs.index_selection(old, "metadata", view="snapshot", fields=FIELDS) == first
    for operation in (lake.inputs.selection_identity, lake.inputs.index_selection):
        with pytest.raises(ValueError, match="every captured atomic original schema"):
            operation(current, "metadata", view="snapshot", fields=FIELDS)
    with pytest.raises(ValueError, match="every captured atomic original schema"):
        lake.inputs.selection_equivalent(old, current, "metadata", view="snapshot", fields=FIELDS)


@pytest.mark.parametrize("entry", ["plan", "update", "recertify", "force"])
def test_future_only_mixed_consumed_schema_blocks_producer_before_noop(tmp_path, monkeypatch, entry):
    exact = 2**53 + 1
    data = frame().with_columns(pl.lit(exact, dtype=pl.Int64).alias("score"))
    lake, metadata, data, _ = setup(tmp_path, data=data)
    daily = DatasetSpec("daily", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    lake.raw.ingest(daily, pl.DataFrame({"time": [date(2025, 1, 2), date(2025, 1, 3)],
        "asset_id": ["A", "A"], "value": [1.0, 2.0]}), ingested_at=received(3))
    calls = []
    def producer(context):
        calls.append((context.start, context.end))
        return context.frames["daily"].join(context.frames["metadata"].select("asset_id", "score"),
            on="asset_id").select(pl.col("source_time").alias("time"), "asset_id", pl.col("score").alias("value"))
    fields = ("asset_id", "score")
    policy = ProducerUpdatePolicy("same_date", timing_neutral_inputs=("metadata",), general_fields=(("metadata", fields),))
    lake.items.register_producer("p", "1", producer, update_policy=policy)
    spec = DataItemSpec("item", (RawInput("custom", "daily", alias="daily"),
        RawInput("custom", "metadata", alias="metadata")), producer_key="p", producer_revision="1", value_dtype="int64")
    lake.items.register(spec)
    report = lake.items.initialize("item", start="2025-01-02", end="2025-01-03")
    assert report.frozen_receipt_id is not None
    old = lake.inputs.get(report.frozen_receipt_id)
    assert lake.items.read("item").collect()["value"].unique().to_list() == [exact]
    assert lake.inputs.index_selection(old, "metadata", view="snapshot", fields=fields) is not None
    completions, manifest = lake.items.completed_builds("item"), lake.items.manifest("item")
    lake.raw.ingest(metadata, data.with_columns(pl.lit(1.0, dtype=pl.Float64).alias("score")), ingested_at=received(5))
    calls.clear()
    ban_originals(monkeypatch, lake)
    if entry == "plan":
        plan = lake.items.plan_update("item", start="2025-01-02", end="2025-01-03")
        assert plan[0]["mode"] == "blocked" and "conflicting captured atomic schema" in plan[0]["reason"]
    else:
        with pytest.raises(RuntimeError, match="conflicting captured atomic schema"):
            if entry == "force":
                lake.items.update("item", start="2025-01-02", end="2025-01-03", force=True)
            else:
                getattr(lake.items, entry)("item", start="2025-01-02", end="2025-01-03")
    assert not calls and lake.items.completed_builds("item") == completions
    assert lake.items.manifest("item") == manifest and lake.items.get("item") == spec
    assert lake.inputs.get(old).digest == old.digest


def test_unchanged_historical_no_eligible_general_reuses_original_completion(tmp_path, monkeypatch):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    metadata = DatasetSpec("metadata", "general")
    lake.raw.ingest(metadata, frame(), ingested_at=received(5))
    daily = DatasetSpec("daily", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    lake.raw.ingest(daily, pl.DataFrame({"time": [date(2025, 1, 2), date(2025, 1, 3)],
        "asset_id": ["A", "A"], "value": [1.0, 2.0]}), ingested_at=received(3))
    calls = []
    def producer(context):
        calls.append((context.start, context.end))
        return context.frames["daily"].join(context.frames["metadata"].select(FIELDS), on="asset_id").select(
            pl.col("source_time").alias("time"), "asset_id", "value")
    policy = ProducerUpdatePolicy("same_date", timing_neutral_inputs=("metadata",), general_fields=(("metadata", FIELDS),))
    lake.items.register_producer("p", "1", producer, update_policy=policy)
    lake.items.register(DataItemSpec("item", (RawInput("custom", "daily", alias="daily"),
        RawInput("custom", "metadata", alias="metadata")), producer_key="p", producer_revision="1"))
    report = lake.items.initialize("item", start="2025-01-02", end="2025-01-03")
    assert report.frozen_receipt_id is not None
    old = lake.inputs.get(report.frozen_receipt_id)
    completions = lake.items.completed_builds("item")
    calls.clear()
    ban_originals(monkeypatch, lake)
    assert lake.items.plan_update("item", start="2025-01-02", end="2025-01-03") == []
    assert lake.items.update("item", start="2025-01-02", end="2025-01-03").status == "unchanged"
    assert lake.items.recertify("item", start="2025-01-02", end="2025-01-03").status == "unchanged"
    assert not calls and lake.items.completed_builds("item") == completions
    assert lake.inputs.get(old).digest == old.digest


def test_typed_empty_selected_general_and_no_eligible_snapshot_are_distinct(tmp_path, monkeypatch):
    lake, spec, data, old = setup(tmp_path, baseline=True, data=frame().head(0))
    baseline = index(lake, old)
    assert baseline is not None
    with monkeypatch.context() as patch:
        ban_originals(patch, lake)
        with pytest.raises(ValueError, match="selected atomic snapshot"):
            index(lake, old, strict=True)
        with pytest.raises(ValueError, match="selected atomic snapshot"):
            lake.inputs.selection_identity(old, "metadata", view="snapshot", strict=True, fields=FIELDS)
        with pytest.raises(ValueError, match="selected atomic snapshot"):
            lake.inputs.selection_equivalent(old, old, "metadata", view="snapshot", strict=True, fields=FIELDS)
    lake.raw.ingest(spec, data, ingested_at=received(4))
    new = freeze(lake, old)
    assert index(lake, new, strict=True) is not None
    assert index(lake, new) == baseline
    assert lake.inputs.verify([old, new])["valid"]


def test_general_null_nan_inf_and_typed_empty_fields_remain_distinct(tmp_path):
    lake, spec, data, old = setup(tmp_path, data=frame().with_columns(pl.lit(None, dtype=pl.Float64).alias("quality")))
    fields = (*FIELDS, "quality")
    tokens = []
    for day, value in enumerate((None, float("nan"), float("inf"), -float("inf"), -0.0, 0.0), start=3):
        lake.raw.ingest(spec, data.with_columns(pl.lit(value, dtype=pl.Float64).alias("quality")), ingested_at=received(day))
        root = freeze(lake, old, day)
        tokens.append(lake.inputs.index_selection(root, "metadata", view="snapshot", fields=fields))
    assert all(tokens) and len(set(tokens)) == 6


def item_setup(tmp_path, *, timing_neutral=True, baseline=False, future=False, known_at=None, windowed=False):
    lake, metadata, data, _ = setup(tmp_path, baseline=baseline)
    daily = DatasetSpec("daily", "by_date", date_kind="calendar", field_mappings={"time": "time", "asset_id": "asset_id"})
    rows = pl.DataFrame({"time": [date(2025, 1, day) for day in (2, 3)], "asset_id": ["A", "A"], "value": [2.0, 3.0]})
    lake.raw.ingest(daily, rows, ingested_at=received(3))
    if known_at is not None:
        lake.raw.ingest(metadata, data, ingested_at=received(known_at))
    if future:
        lake.raw.ingest(metadata, data.with_columns(pl.lit("future actor").alias("act_name")), ingested_at=received(4))
    calls = []
    def producer(context):
        calls.append((context.start, context.end))
        market = context.frames["metadata"].select(FIELDS)
        return context.frames["daily"].join(market, on="asset_id", how="inner").select(
            pl.col("source_time").alias("time"), "asset_id",
            pl.when(pl.col("market") == "main").then(pl.col("value")).otherwise(pl.col("value") + 10).alias("value"))
    policy = ProducerUpdatePolicy("same_date", timing_neutral_inputs=("metadata",) if timing_neutral else (),
        general_fields=(("metadata", FIELDS),))
    lake.items.register_producer("p", "1", producer, update_policy=policy)
    requests = (RawInput("custom", "daily", alias="daily"), RawInput("custom", "metadata", alias="metadata"))
    lake.items.register(DataItemSpec("item", requests, producer_key="p", producer_revision="1"))
    report = (lake.items.update("item", start="2025-01-02", end="2025-01-03",
        input_windows={"daily": ("2025-01-02", "2025-01-03")}) if windowed else
        lake.items.initialize("item", start="2025-01-02", end="2025-01-03"))
    assert report.frozen_receipt_id is not None
    original = lake.inputs.get(report.frozen_receipt_id)
    calls.clear()
    return lake, metadata, data, daily, producer, calls, original


def maintain_pair(lake, old, end=4, days=()):
    new = freeze(lake, old, end)
    index(lake, old)
    for day in (*days, end):
        index(lake, new, as_of=date(2025, 1, day))
    return new


def append_daily(lake, daily, day=4):
    lake.raw.ingest(daily, pl.DataFrame({"time": [date(2025, 1, day)], "asset_id": ["A"], "value": [float(day)]}),
        ingested_at=received(day))


@pytest.mark.parametrize("future", [False, True])
def test_item_unused_general_change_reuses_history_and_computes_only_tail(tmp_path, monkeypatch, future):
    lake, spec, data, daily, producer, calls, old = item_setup(tmp_path, future=future)
    if not future:
        lake.raw.ingest(spec, data.with_columns(pl.lit("new actor").alias("act_name")), ingested_at=received(4))
    append_daily(lake, daily)
    maintain_pair(lake, old)
    original = lake.inputs.get(old)
    with monkeypatch.context() as patch:
        ban_originals(patch, lake)
        assert lake.items.plan_update("item", start="2025-01-02", end="2025-01-04") == [
            {"mode": "incremental", "start": "2025-01-04", "end": "2025-01-04"}]
        assert not calls
    report = lake.items.update("item", start="2025-01-02", end="2025-01-04")
    assert report.status == "success" and calls and all(first == last == date(2025, 1, 4) for first, last in calls)
    assert lake.inputs.get(old) == original
    lake.items.register_producer("reference", "1", producer)
    lake.items.register(DataItemSpec("reference", lake.items.get("item").inputs,
        producer_key="reference", producer_revision="1"))
    lake.items.initialize("reference", start="2025-01-02", end="2025-01-04")
    for view in ("snapshot", "history"):
        for strict in (False, True):
            fields = ["time", "asset_id", "value", "available_date", "version_available_date", "_baseline"]
            actual = lake.items.read("item", view=view, as_of="2025-01-04", strict=strict).collect().select(fields)
            expected = lake.items.read("reference", view=view, as_of="2025-01-04", strict=strict).collect().select(fields)
            assert_frame_equal(actual.sort("time"), expected.sort("time"))


@pytest.mark.parametrize("change", ["market", "new_asset", "missing", "intermediate", "baseline"])
def test_item_general_unknown_or_actual_change_is_actionable_before_any_values(tmp_path, monkeypatch, change):
    lake, spec, data, daily, _, calls, old = item_setup(tmp_path, baseline=change == "baseline")
    changed = data.with_columns(pl.lit("growth").alias("market")) if change == "market" else (
        pl.concat([data, data.head(1).with_columns(pl.lit("C").alias("asset_id"))]) if change == "new_asset" else
        data.with_columns(pl.lit("new actor").alias("act_name")))
    lake.raw.ingest(spec, changed, ingested_at=received(4))
    end = 4
    if change == "intermediate":
        lake.raw.ingest(spec, data.with_columns(pl.lit("growth").alias("market")), ingested_at=received(4))
        lake.raw.ingest(spec, data.with_columns(pl.lit("restored actor").alias("act_name")), ingested_at=received(5))
        end = 5
    append_daily(lake, daily, end)
    if change != "missing":
        new = maintain_pair(lake, old, end, days=(4,) if end == 5 else ())
        if change == "intermediate":
            assert lake.inputs.selection_equivalent(old, new, "metadata", view="snapshot", fields=FIELDS) is True
    ban_originals(monkeypatch, lake)
    actions = lake.items.plan_update("item", start="2025-01-02", end=date(2025, 1, end))
    if change == "missing":
        # Immutable per-field segment metadata proves this unused-field-only
        # update without any optional exact summary.
        assert actions == [{"mode": "incremental", "start": "2025-01-04", "end": "2025-01-04"}]
    else:
        assert actions[0]["mode"] == "blocked"
        assert ("timing" if change == "baseline" else "consumed fields changed") in actions[0]["reason"]
    assert not calls


def test_general_field_policy_does_not_replace_timing_neutral_contract(tmp_path, monkeypatch):
    lake, spec, data, daily, _, calls, old = item_setup(tmp_path, timing_neutral=False)
    lake.raw.ingest(spec, data.with_columns(pl.lit("new actor").alias("act_name")), ingested_at=received(4))
    append_daily(lake, daily)
    maintain_pair(lake, old)
    ban_originals(monkeypatch, lake)
    action = lake.items.plan_update("item", start="2025-01-02", end="2025-01-04")[0]
    assert action["mode"] == "blocked" and "timing-neutral" in action["reason"] and not calls


@pytest.mark.parametrize("declaration", [[], (("metadata", ["market"]),), (("metadata", ()),),
    (("metadata", ("market", "market")),), (("metadata", ("_baseline",)),),
    (("metadata", ("market",)), ("metadata", ("asset_id",)))])
def test_general_field_policy_requires_an_immutable_unambiguous_declaration(declaration):
    with pytest.raises(ValueError, match="general_fields"):
        ProducerUpdatePolicy("same_date", general_fields=declaration)


def test_general_summary_group_cancellation_and_budget_preserve_originals(tmp_path):
    lake, _, _, old = setup(tmp_path)
    before = lake.inputs.get(old)
    with pytest.raises(MemoryError):
        index(lake, old, config=ExecutionOptions(max_buffer_bytes=64))
    def cancelled():
        raise CancelledError
    with pytest.raises(CancelledError):
        index(lake, old, check_canceled=cancelled)
    assert lake.inputs.selection_identity(old, "metadata", view="snapshot", fields=FIELDS) is not None
    assert lake.inputs.get(old) == before
    assert index(lake, old) is not None


@pytest.mark.parametrize("consumed_change", [False, True])
def test_same_pit_intermediate_original_commit_requires_its_own_summary(tmp_path, monkeypatch, consumed_change):
    lake, spec, data, daily, _, calls, old = item_setup(tmp_path)
    intermediate = data.with_columns(pl.lit("intermediate actor").alias("act_name"))
    if consumed_change:
        intermediate = intermediate.with_columns(pl.lit("growth").alias("market"))
    lake.raw.ingest(spec, intermediate, ingested_at=received(4))
    middle = lake.inputs.freeze(old.requests, information_cutoff="2025-01-04")
    ceiling = middle.max_commit
    lake.raw.ingest(spec, data.with_columns(pl.lit("final actor").alias("act_name")), ingested_at=received(4))
    append_daily(lake, daily)
    current = maintain_pair(lake, old)
    assert lake.inputs.selection_equivalent(old, current, "metadata", view="snapshot", fields=FIELDS) is True
    with monkeypatch.context() as patch:
        ban_originals(patch, lake)
        action = lake.items.plan_update("item", start="2025-01-02", end="2025-01-04")[0]
        if consumed_change:
            assert action["mode"] == "blocked" and "summary is missing" in action["reason"]
            assert f"max_commit={ceiling}" in action["reason"]
        else:
            assert action == {"mode": "incremental", "start": "2025-01-04", "end": "2025-01-04"}
    bounded = lake.inputs.freeze(old.requests, information_cutoff="2025-01-04", max_commit=ceiling)
    assert index(lake, middle) == index(lake, bounded)
    with monkeypatch.context() as patch:
        ban_originals(patch, lake)
        actions = lake.items.plan_update("item", start="2025-01-02", end="2025-01-04")
        if consumed_change:
            assert actions[0]["mode"] == "blocked" and "consumed fields changed" in actions[0]["reason"]
            assert f"max_commit={ceiling}" in actions[0]["reason"]
        else:
            assert actions == [{"mode": "incremental", "start": "2025-01-04", "end": "2025-01-04"}]
        assert not calls


def test_backdated_unused_general_change_cannot_backdate_first_known_timing(tmp_path, monkeypatch):
    lake, spec, data, daily, _, calls, old = item_setup(tmp_path, baseline=True, known_at=3)
    lake.raw.ingest(spec, data.with_columns(pl.lit("backdated actor").alias("act_name")), ingested_at=received(2))
    append_daily(lake, daily)
    current = maintain_pair(lake, old)
    assert lake.inputs.selection_equivalent(old, current, "metadata", view="snapshot", fields=FIELDS) is True
    ban_originals(monkeypatch, lake)
    action = lake.items.plan_update("item", start="2025-01-02", end="2025-01-04")[0]
    assert action["mode"] == "blocked" and "first visibility/baseline timing changed at 2025-01-02" in action["reason"]
    assert not calls


def test_general_typed_noop_cancel_keeps_original_completion_and_zero_producers(tmp_path, monkeypatch):
    lake, spec, data, _, _, calls, old = item_setup(tmp_path)
    declaration = lake.items.get("item")
    original = lake.inputs.get(old)
    manifest = lake.items.manifest("item")
    completions = lake.items.completed_builds("item")
    lake.raw.ingest(spec, data.with_columns(pl.lit("same-range actor").alias("act_name")), ingested_at=received(3))
    maintain_pair(lake, old, 3)
    with monkeypatch.context() as patch:
        ban_originals(patch, lake)
        assert lake.items.plan_update("item", start="2025-01-02", end="2025-01-03") == []
        cancelled = lake.items.recertify("item", start="2025-01-02", end="2025-01-03", cancelled=lambda: True)
        assert cancelled.status == "cancelled"
        assert lake.items.completed_builds("item") == completions
        renewed = lake.items.recertify("item", start="2025-01-02", end="2025-01-03")
        assert renewed.status == "unchanged" and renewed.rows_committed == 0
        assert not calls
    assert lake.inputs.get(old) == original
    assert lake.items.get("item") == declaration
    assert lake.items.manifest("item") == manifest


def test_general_summary_rejects_forged_digest_and_explicit_audit_checks_values(tmp_path, monkeypatch):
    lake, _, _, old = setup(tmp_path)
    with pytest.raises(RuntimeError, match="checksum"):
        index(lake, replace(old, digest="0" * 64))
    first = index(lake, old)
    with lake._data_meta.connect() as db:
        row = db.execute("select * from input_projection_index where source_receipt_id=?", (old.receipt_id,)).fetchone()
        db.execute("update input_projection_index set identity=? where source_receipt_id=?", ("0" * 64, old.receipt_id))
    with monkeypatch.context() as patch:
        ban_originals(patch, lake)
        with pytest.raises(RuntimeError, match="damaged"):
            lake.inputs.selection_identity(old, "metadata", view="snapshot", fields=FIELDS)
    with lake._data_meta.connect() as db:
        db.execute("update input_projection_index set identity=? where source_receipt_id=?", (first, old.receipt_id))
    assert row["identity"] == first and lake.inputs.verify(old)["valid"]


@pytest.mark.parametrize("entry", ["update", "recertify", "plan_context"])
def test_public_general_planning_inherits_the_callers_resource_budget(tmp_path, monkeypatch, entry):
    lake, spec, data, _, _, calls, old = item_setup(tmp_path)
    lake.raw.ingest(spec, data.with_columns(pl.lit("new actor").alias("act_name")), ingested_at=received(3))
    options = ExecutionOptions(max_buffer_bytes=1024 * 1024)
    capture = lake.inputs._capture
    charged = []
    def admitted(db, requests, **kwargs):
        if set(requests) == {"metadata"}:
            charged.append(kwargs["max_buffer_bytes"])
            assert kwargs["max_buffer_bytes"] == options.max_buffer_bytes
            raise MemoryError("test caller budget admission")
        return capture(db, requests, **kwargs)
    monkeypatch.setattr(lake.inputs, "_capture", admitted)
    ban_originals(monkeypatch, lake)
    with pytest.raises(MemoryError, match="caller budget admission"):
        if entry == "plan_context":
            with lake.inputs.read_context(old, config=options):
                lake.items.plan_update("item", start="2025-01-02", end="2025-01-03")
        else:
            getattr(lake.items, entry)("item", start="2025-01-02", end="2025-01-03", config=options)
    assert charged == [options.max_buffer_bytes] and not calls


def test_general_window_extension_preflight_also_uses_public_update_budget(tmp_path, monkeypatch):
    lake, spec, data, _, _, calls, old = item_setup(tmp_path, windowed=True)
    lake.raw.ingest(spec, data.with_columns(pl.lit("new actor").alias("act_name")), ingested_at=received(4))
    options = ExecutionOptions(max_buffer_bytes=1024 * 1024)
    capture = lake.inputs._capture
    charged = []
    def admitted(db, requests, **kwargs):
        if set(requests) == {"daily", "metadata"}:
            context = lake.inputs._read_context()
            assert context is not None
            charged.append(context.options.max_buffer_bytes)
            assert context.options.max_buffer_bytes == options.max_buffer_bytes
            raise MemoryError("test window extension caller budget admission")
        return capture(db, requests, **kwargs)
    monkeypatch.setattr(lake.inputs, "_capture", admitted)
    ban_originals(monkeypatch, lake)
    with pytest.raises(MemoryError, match="window extension caller budget"):
        lake.items.update("item", start="2025-01-02", end="2025-01-04", config=options,
            input_windows={"daily": ("2025-01-02", "2025-01-04")})
    assert charged == [options.max_buffer_bytes] and not calls
    assert lake.inputs.get(old).digest == old.digest


def test_no_eligible_general_projection_never_borrows_future_schema(tmp_path, monkeypatch):
    data = frame().with_columns(pl.lit(1, dtype=pl.Int64).alias("quality"))
    lake, spec, data, old = setup(tmp_path, baseline=True, data=data)
    fields = (*FIELDS, "quality")
    original = lake.inputs.get(old)
    lake.raw.ingest(spec, data.with_columns(pl.lit(1.0, dtype=pl.Float64).alias("quality")),
        ingested_at=received(5))
    bounded = lake.inputs.freeze(old.requests, information_cutoff="2025-01-03", max_commit=old.max_commit)
    all_retained = lake.inputs.freeze(old.requests, information_cutoff="2025-01-03")
    before = lake.inputs.read(bounded, "metadata", view="snapshot", strict=True, fields=fields).collect()
    after = lake.inputs.read(all_retained, "metadata", view="snapshot", strict=True, fields=fields).collect()
    assert before.is_empty() and after.is_empty()
    assert before.schema["quality"] == pl.Int64 and after.schema["quality"] == pl.Float64
    with lake._data_meta.connect() as db:
        summaries = db.execute("select count(*) from input_projection_index").fetchone()[0]
    ban_originals(monkeypatch, lake)
    for source in (old, bounded, all_retained):
        with pytest.raises(ValueError, match="selected atomic snapshot"):
            lake.inputs.index_selection(source, "metadata", view="snapshot", strict=True, fields=fields)
        with pytest.raises(ValueError, match="selected atomic snapshot"):
            lake.inputs.selection_identity(source, "metadata", view="snapshot", strict=True, fields=fields)
    with pytest.raises(ValueError, match="selected atomic snapshot"):
        lake.inputs.selection_equivalent(bounded, all_retained, "metadata", view="snapshot", strict=True, fields=fields)
    with lake._data_meta.connect() as db:
        assert db.execute("select count(*) from input_projection_index").fetchone()[0] == summaries
    assert lake.inputs.get(old) == original
