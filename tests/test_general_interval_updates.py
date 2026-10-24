"""Compact interval impact preserves immutable history and bounds native work."""
from datetime import UTC, date, datetime
import json
import sqlite3

import polars as pl
from polars.testing import assert_frame_equal
import pytest

from bagelquant_data import DataLake, DataItemSpec, DatasetSpec, ProducerUpdatePolicy, RawInput, input_index

FIELDS = ("asset_id", "value", "begin", "until")


def received(day):
    return datetime(2025, 1, day, tzinfo=UTC)


def setup(tmp_path, *, verified_baseline=False, original_b=2.):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    general = DatasetSpec("members", "general")
    frame = pl.DataFrame({"asset_id": ["A", "B"], "value": [1., original_b],
        "begin": ["20250102", "20250104"], "until": [None, "20250105"]})
    lake.raw.ingest(general, frame, mode="initialize" if verified_baseline else "incremental", ingested_at=received(2))
    if verified_baseline:
        lake.raw.ingest(general, frame, ingested_at=received(3))
    calls = []

    def producer(context):
        calls.append((context.start, context.end))
        members = context.frames["members"].select(FIELDS).with_columns(
            pl.col("begin").str.to_date("%Y%m%d"), pl.col("until").str.to_date("%Y%m%d"))
        return (pl.DataFrame({"time": pl.date_range(context.start, context.end, eager=True)})
            .join(members, how="cross").filter((pl.col("time") >= pl.col("begin"))
                & (pl.col("until").is_null() | (pl.col("time") < pl.col("until"))))
            .select("time", "asset_id", "value"))

    policy = ProducerUpdatePolicy("same_date", timing_neutral_inputs=("members",),
        general_fields=(("members", FIELDS),), general_intervals=(("members", "begin", "until"),))
    lake.items.register_producer("p", "1", producer, update_policy=policy)
    lake.items.register(DataItemSpec("item", (RawInput("custom", "members", alias="members"),),
        producer_key="p", producer_revision="1"))
    report = lake.items.initialize("item", start="2025-01-02", end="2025-01-05")
    assert report.frozen_receipt_id is not None
    calls.clear()
    return lake, general, frame, lake.inputs.get(report.frozen_receipt_id), producer, policy, calls


def prepare(lake, original):
    return lake.inputs.prepare_general_projections(original, "members", FIELDS,
        through="2025-01-07", interval_fields=("begin", "until"))


@pytest.mark.parametrize("change", ["append", "revise", "delete", "signed_zero"])
@pytest.mark.parametrize("verified_baseline", [False, True])
def test_interval_append_and_repair_match_reference_without_prefix_replay(tmp_path, monkeypatch, change, verified_baseline):
    lake, spec, frame, original, producer, policy, calls = setup(tmp_path,
        verified_baseline=verified_baseline, original_b=-0. if change == "signed_zero" else 2.)
    old_builds = lake.items.completed_builds("item")
    if change == "append":
        current = pl.concat([frame, pl.DataFrame({"asset_id": ["C"], "value": [3.],
            "begin": ["20250107"], "until": [None]}, schema_overrides={"until": pl.String})])
    elif change in {"revise", "signed_zero"}:
        current = frame.with_columns(pl.when(pl.col("asset_id") == "B").then(0. if change == "signed_zero" else 20.)
            .otherwise(pl.col("value")).alias("value"))
    else:
        current = frame.filter(pl.col("asset_id") != "B")
    lake.raw.ingest(spec, current, ingested_at=received(7))
    assert lake.items.plan_update("item", start="2025-01-02", end="2025-01-07")[0]["mode"] == "blocked"
    prepared = prepare(lake, original)
    assert prepared["intervals"]["prepared"] >= 1 and not calls
    expected = [] if change == "append" else [{"mode": "incremental", "start": "2025-01-04", "end": "2025-01-04"}]
    expected.append({"mode": "incremental", "start": "2025-01-06", "end": "2025-01-07"})
    with monkeypatch.context() as guarded:
        def forbidden(*_args, **_kwargs):
            raise AssertionError("Planning/known preparation read Original payloads")
        guarded.setattr(input_index, "project_original", forbidden)
        assert lake.items.plan_update("item", start="2025-01-02", end="2025-01-07") == expected
        assert prepare(lake, original)["intervals"]["prepared"] == 0
    assert lake.items.update("item", start="2025-01-02", end="2025-01-07").status == "success"
    assert set(calls) == {(date.fromisoformat(row["start"]), date.fromisoformat(row["end"])) for row in expected}
    # Both original and revised information states are evaluated, exclusively
    # inside the admitted repair/tail intervals.
    assert len(calls) == 2 * len(expected)
    assert all(row in lake.items.completed_builds("item") for row in old_builds)
    lake.items.register_producer("reference", "1", producer, update_policy=policy)
    lake.items.register(DataItemSpec("reference", (RawInput("custom", "members", alias="members"),),
        producer_key="reference", producer_revision="1"))
    lake.items.initialize("reference", start="2025-01-02", end="2025-01-07")
    def values(name):
        return lake.items.read(name, view="snapshot", as_of="2025-01-07").select(
            "time", "asset_id", "value").collect().sort("time", "asset_id")
    assert_frame_equal(values("item"), values("reference"))
    if change == "signed_zero":
        import math
        assert math.copysign(1., values("item").filter(pl.col("asset_id") == "B")["value"].item()) == 1.
    calls.clear()
    with monkeypatch.context() as guarded:
        guarded.setattr(lake.inputs, "read", forbidden)
        assert lake.items.plan_update("item", start="2025-01-02", end="2025-01-07") == []
        assert lake.items.update("item", start="2025-01-02", end="2025-01-07").status == "unchanged"
    assert not calls
    lake.inputs.verify(original)


def test_intermediate_same_day_state_is_not_hidden_by_equal_final_content(tmp_path):
    lake, spec, frame, original, _producer, _policy, _calls = setup(tmp_path)
    middle = frame.with_columns(pl.when(pl.col("asset_id") == "B").then(20.)
        .otherwise(pl.col("value")).alias("value"))
    lake.raw.ingest(spec, middle, ingested_at=received(7))
    lake.raw.ingest(spec, frame, ingested_at=received(7))
    prepared = prepare(lake, original)
    assert prepared["intervals"]["prepared"] >= 1
    assert lake.items.plan_update("item", start="2025-01-02", end="2025-01-07") == [
        {"mode": "incremental", "start": "2025-01-04", "end": "2025-01-04"},
        {"mode": "incremental", "start": "2025-01-06", "end": "2025-01-07"}]


@pytest.mark.parametrize("field,value", [("begin", None), ("begin", "invalid"), ("until", "invalid")])
def test_unknown_interval_bounds_remain_blocked(tmp_path, field, value):
    lake, spec, frame, original, _producer, _policy, calls = setup(tmp_path)
    current = frame.with_columns(pl.when(pl.col("asset_id") == "B").then(pl.lit(value, dtype=pl.String))
        .otherwise(pl.col(field)).alias(field))
    lake.raw.ingest(spec, current, ingested_at=received(7))
    assert any(row["status"] == "unsupported" for row in prepare(lake, original)["intervals"]["states"])
    assert lake.items.plan_update("item", start="2025-01-02", end="2025-01-07")[0]["mode"] == "blocked"
    assert not calls


def test_explicit_audit_recomputes_interval_evidence(tmp_path):
    lake, spec, frame, original, _producer, _policy, _calls = setup(tmp_path)
    lake.raw.ingest(spec, frame.filter(pl.col("asset_id") != "B"), ingested_at=received(7))
    prepare(lake, original)
    current = lake.inputs.freeze(original.requests, information_cutoff="2025-01-07")
    lake.inputs.verify(current)
    with sqlite3.connect(lake.data_meta_path) as db:
        db.row_factory = sqlite3.Row
        row = dict(db.execute("select * from input_projection_index where version='general_interval_impact.v1' limit 1").fetchone())
        value = json.loads(row["identity"])
        value["ranges"] = []
        row["identity"] = json.dumps(value)
        row["digest"] = input_index._digest({key: item for key, item in row.items() if key != "digest"})
        db.execute("update input_projection_index set identity=?,digest=? where evidence_key=? and request_key=?",
            (row["identity"], row["digest"], row["evidence_key"], row["request_key"]))
    with pytest.raises(RuntimeError, match="interval impact differs"):
        lake.inputs.verify(current)


def test_complete_history_impact_stays_blocked(tmp_path):
    lake, spec, frame, original, _producer, _policy, calls = setup(tmp_path)
    lake.raw.ingest(spec, frame.with_columns((pl.col("value") * 10).alias("value")), ingested_at=received(7))
    prepare(lake, original)
    plan = lake.items.plan_update("item", start="2025-01-02", end="2025-01-07")
    assert plan[0]["mode"] == "blocked" and "covers the completed history" in plan[0]["reason"]
    assert not calls


@pytest.mark.parametrize("representation", ["sign", "payload"])
@pytest.mark.parametrize("mixed", [False, True])
def test_nan_representation_changes_remain_unknown_even_with_other_tuple_changes(representation, mixed):
    import struct
    from bagelquant_data.general_intervals import _ranges
    original = struct.unpack(">d", bytes.fromhex("7ff8000000000000"))[0]
    changed = struct.unpack(">d", bytes.fromhex(
        "fff8000000000000" if representation == "sign" else "7ff8000000000001"))[0]
    before = pl.DataFrame({"asset_id": ["A", "B"], "value": [original, 1.],
        "begin": ["20250102", "20250104"], "until": ["20250103", "20250105"]})
    after = before.with_columns(pl.Series("value", [changed, 2. if mixed else 1.]))
    assert input_index.identity(before) != input_index.identity(after)
    with pytest.raises(ValueError, match="exact unchanged-tuple"):
        _ranges(before, after, ("begin", "until"))


def test_cancelled_preparation_retains_completed_proof_and_retry_reads_no_payload(tmp_path, monkeypatch):
    from bagelquant_data import general_intervals
    lake, spec, frame, original, _producer, _policy, calls = setup(tmp_path)
    lake.raw.ingest(spec, frame.filter(pl.col("asset_id") != "B"), ingested_at=received(7))
    stopped = False
    def check():
        if stopped:
            raise RuntimeError("stop proof preparation")
    def progress(work):
        nonlocal stopped
        stopped = work.get("stage") == "general_interval_preparation"
    with pytest.raises(RuntimeError, match="stop proof preparation"):
        lake.inputs.prepare_general_projections(original, "members", FIELDS,
            through="2025-01-07", interval_fields=("begin", "until"), check_canceled=check, progress=progress)
    def forbidden(*_args, **_kwargs):
        raise AssertionError("Retry reread a completed interval proof")
    monkeypatch.setattr(general_intervals, "_general_frame", forbidden)
    result = prepare(lake, original)
    assert result["intervals"]["prepared"] == 0 and result["intervals"]["reused"] >= 1
    assert not calls
