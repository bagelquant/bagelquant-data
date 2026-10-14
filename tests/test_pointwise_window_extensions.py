"""Only positively proven future Raw scope additions reuse completed timing."""
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, date, datetime

import polars as pl
from polars.testing import assert_frame_equal
import pytest

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, ItemInput, ProducerUpdatePolicy, RawInput
from bagelquant_data import input_index


START, LAST, TAIL = date(2026, 10, 1), date(2026, 10, 12), date(2026, 10, 13)


def _lake(root):
    return DataLake.open(data_meta_path=root / "data.sqlite", lake_path=root / "lake")


def _received(day):
    return datetime.combine(day, datetime.min.time(), UTC)


def _row(day, value=1.):
    return pl.DataFrame({"time": [day], "asset_id": ["A"], "value": [value],
                         "marker": ["caller"], "time_right": [day]})


def _fixture(root, *, view="history", strict=False, empty_check=False):
    lake = _lake(root)
    spec = DatasetSpec("daily", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    lake.raw.ingest(spec, _row(START), mode="initialize", ingested_at=_received(START))
    lake.raw.ingest(spec, _row(LAST, 12.), ingested_at=_received(LAST))
    if empty_check:
        lake.raw.ingest(spec, _row(LAST).head(0), ingested_at=_received(LAST))
    calls = []

    def producer(context):
        frame = context.frames["raw"]
        calls.append((context.start, context.end, context.historical_baseline,
                      frame["source_time"].to_list()))
        return frame.select(pl.col("source_time").alias("time"), "asset_id",
                            (pl.col("value") * 2).alias("value"), "marker", "time_right")

    lake.items.register_producer("twice", "1", producer, update_policy=ProducerUpdatePolicy("same_date"))
    lake.items.register(DataItemSpec("execution", (RawInput("custom", "daily", alias="raw", view=view, strict=strict),),
        producer_key="twice", producer_revision="1"))
    first = lake.items.update("execution", start=START, end=LAST, input_windows={"raw": (START, LAST)})
    assert first.status == "success" and first.frozen_receipt_id is not None
    original = lake.inputs.get(first.frozen_receipt_id)
    lake.raw.ingest(spec, _row(TAIL, 13.), ingested_at=_received(TAIL))
    calls.clear()
    return lake, spec, original, calls


@pytest.mark.parametrize("view", ["history", "snapshot", "versions"])
@pytest.mark.parametrize("strict", [False, True])
def test_mixed_raw_upper_extension_plans_from_metadata_and_consumes_only_tail(tmp_path, monkeypatch, view, strict):
    lake, _, original, calls = _fixture(tmp_path, view=view, strict=strict, empty_check=True)
    definition = lake.items.get("execution")
    retained = lake.inputs.freeze({"execution": ItemInput("execution", view="versions")}, information_cutoff=LAST)
    historical = lake.inputs.read(retained, "execution").collect()
    source_historical = lake.inputs.read(original, "raw").collect()
    originals = {path: path.read_bytes() for path in lake.lake_path.rglob("*.parquet")}
    old_builds = lake.items.completed_builds("execution")
    assert {bool(row["baseline"]) for row in original.evidence["raw"]["batches"]} == {False, True}
    assert original.evidence["raw"]["empty_checks"]

    def forbidden(*_args, **_kwargs):
        pytest.fail("Metadata-only scope planning/repeat consumed values, froze or prepared proofs")

    with monkeypatch.context() as guard:
        guard.setattr(lake.inputs, "read", forbidden)
        guard.setattr(lake.inputs, "freeze", forbidden)
        guard.setattr(input_index, "project_original", forbidden)
        assert lake.items.plan_update("execution", start=START, end=TAIL,
            input_windows={"raw": (START, TAIL)}) == [
                {"mode": "incremental", "start": TAIL.isoformat(), "end": TAIL.isoformat()}]
    assert not calls
    reads = []
    original_read = lake.inputs.read

    def read(*args, **kwargs):
        frame = original_read(*args, **kwargs).collect()
        reads.append((kwargs.get("start"), kwargs.get("end"), frame.height))
        return frame.lazy()

    with monkeypatch.context() as guard:
        guard.setattr(lake.inputs, "read", read)
        guard.setattr(input_index, "project_original", forbidden)
        report = lake.items.update("execution", start=START, end=TAIL, input_windows={"raw": (START, TAIL)})
    assert report.status == "success" and report.rows_committed == 1
    assert calls == [(TAIL, TAIL, False, [TAIL])]
    assert reads == [(TAIL, TAIL, 1)]
    result = lake.items.read("execution", start=TAIL, end=TAIL, view="snapshot", as_of=TAIL, strict=True).collect()
    assert result["value"].to_list() == [26.]
    assert result["_baseline"].to_list() == [False]
    assert result["available_date"].to_list() == [TAIL]
    assert result["version_available_date"].to_list() == [TAIL]
    assert result["marker"].to_list() == ["caller"] and result["time_right"].to_list() == [TAIL]
    assert lake.items.read("execution", start=TAIL, end=TAIL, view="snapshot", as_of=LAST).collect().is_empty()
    assert_frame_equal(lake.inputs.read(retained, "execution").collect(), historical)
    assert_frame_equal(lake.inputs.read(original, "raw").collect(), source_historical)
    assert lake.inputs.is_current(original)
    assert all(path.read_bytes() == body for path, body in originals.items())
    assert lake.items.get("execution") == definition
    assert all(build in lake.items.completed_builds("execution") for build in old_builds)
    calls.clear()
    with monkeypatch.context() as guard:
        guard.setattr(lake.inputs, "read", forbidden)
        guard.setattr(lake.inputs, "freeze", forbidden)
        guard.setattr(input_index, "project_original", forbidden)
        assert lake.items.plan_update("execution", start=START, end=TAIL, input_windows={"raw": (START, TAIL)}) == []
        assert lake.items.update("execution", start=START, end=TAIL, input_windows={"raw": (START, TAIL)}).status == "unchanged"
    assert not calls


@pytest.mark.parametrize("mode", ["initialize", "incremental"])
@pytest.mark.parametrize("view", ["history", "snapshot", "versions"])
def test_future_observation_cannot_be_visible_before_old_cutoff_even_negative_offset(tmp_path, mode, view):
    lake = _lake(tmp_path)
    raw = DatasetSpec("future", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"},
        availability_day_offset=-1, availability_cutoff_time="12:00:00")
    lake.raw.ingest(raw, _row(TAIL), mode=mode, ingested_at=_received(LAST))
    original = lake.raw.read("future", source="custom", view="versions").collect()
    assert original["source_time"].to_list() == original["time"].to_list() == [TAIL]
    receipt = lake.inputs.freeze({"future": RawInput("custom", "future", start=START, end=TAIL,
        view=view, include_historical_baseline=True)}, information_cutoff=LAST)
    assert lake.inputs.read(receipt, "future").collect().is_empty()


def test_lower_extension_can_change_old_empty_timing_and_remains_blocked(tmp_path, monkeypatch):
    lake = _lake(tmp_path)
    past = date(2026, 9, 30)
    raw = DatasetSpec("daily", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    lake.raw.ingest(raw, _row(past), mode="initialize", ingested_at=_received(past))
    calls = []

    def count(context):
        calls.append(context.historical_baseline)
        return _row(context.end, float(context.frames["raw"].height))

    lake.items.register_producer("count", "1", count, update_policy=ProducerUpdatePolicy("same_date"))
    lake.items.register(DataItemSpec("count", (RawInput("custom", "daily", alias="raw"),),
        producer_key="count", producer_revision="1"))
    first = lake.items.update("count", start=START, end=START, input_windows={"raw": (START, START)})
    assert first.frozen_receipt_id is not None
    original = lake.inputs.get(first.frozen_receipt_id)
    wider = lake.inputs.freeze({"raw": RawInput("custom", "daily", alias="raw", view="versions",
        start=past, end=TAIL, include_historical_baseline=True)}, information_cutoff=START,
        max_commit=original.max_commit)
    assert lake.inputs._baseline_at(original, "raw", START) is False
    assert lake.inputs._baseline_at(wider, "raw", START) is True
    calls.clear()
    monkeypatch.setattr(lake.inputs, "read", lambda *_a, **_k: pytest.fail("Rejected scope read values"))
    plan = lake.items.plan_update("count", start=START, end=TAIL, input_windows={"raw": (past, TAIL)})
    assert plan[0]["mode"] == "blocked" and "baseline timing" in plan[0]["reason"]
    with pytest.raises(RuntimeError, match="baseline timing"):
        lake.items.update("count", start=START, end=TAIL, input_windows={"raw": (past, TAIL)})
    assert not calls


@pytest.mark.parametrize("field", ["request_json", "input_receipt_id", "baseline"])
def test_changed_complete_empty_check_authority_does_not_gain_future_scope_proof(tmp_path, monkeypatch, field):
    lake, _, original, calls = _fixture(tmp_path, empty_check=True)
    assert original.evidence["raw"]["empty_checks"]
    capture = lake.inputs._capture

    def altered(*args, **kwargs):
        payload = capture(*args, **kwargs)
        payload = deepcopy(payload)
        rows = payload["evidence"]["raw"]["empty_checks"]
        if field == "baseline":
            rows[0][field] = not bool(rows[0][field])
        elif field == "request_json":
            rows[0][field] = '[{"item_range":{"start":"2026-10-13","end":"2026-10-13"}}]'
        else:
            rows[0][field] = "different-parent-authority"
        return payload

    monkeypatch.setattr(lake.inputs, "_capture", altered)
    plan = lake.items.plan_update("execution", start=START, end=TAIL, input_windows={"raw": (START, TAIL)})
    assert plan[0]["mode"] == "blocked" and "baseline timing" in plan[0]["reason"]
    assert not calls


def test_general_runtime_windows_remain_rejected_before_consumption(tmp_path, monkeypatch):
    lake = _lake(tmp_path)
    raw = DatasetSpec("general", "general", date_kind="calendar")
    lake.raw.ingest(raw, _row(START), mode="initialize", ingested_at=_received(START))
    lake.items.register(DataItemSpec("item", (RawInput("custom", "general", alias="raw"),)))
    monkeypatch.setattr(lake.inputs, "read", lambda *_a, **_k: pytest.fail("General scope consumed values"))
    with pytest.raises(ValueError, match="by-date"):
        lake.items.plan_update("item", start=START, end=TAIL, input_windows={"raw": (START, TAIL)})


def test_safe_future_baseline_does_not_reclassify_an_old_empty_output(tmp_path):
    lake = _lake(tmp_path)
    raw = DatasetSpec("daily", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    lake.raw.ingest(raw, _row(TAIL), mode="initialize", ingested_at=_received(START))
    calls = []

    def count(context):
        calls.append((context.start, context.end, context.historical_baseline))
        return _row(context.end, float(context.frames["raw"].height))

    lake.items.register_producer("count", "1", count, update_policy=ProducerUpdatePolicy("same_date"))
    lake.items.register(DataItemSpec("count", (RawInput("custom", "daily", alias="raw"),),
        producer_key="count", producer_revision="1"))
    first = lake.items.update("count", start=START, end=LAST, input_windows={"raw": (START, LAST)})
    assert first.frozen_receipt_id is not None
    old_input = lake.inputs.get(first.frozen_receipt_id)
    historical = lake.items.read("count", view="snapshot", as_of=LAST, strict=True).collect()
    assert historical["value"].to_list() == [0.]
    assert lake.inputs._baseline_at(old_input, "raw", LAST) is False
    calls.clear()
    assert lake.items.update("count", start=START, end=TAIL, input_windows={"raw": (START, TAIL)}).status == "success"
    assert calls == [(TAIL, TAIL, True)]
    assert_frame_equal(lake.items.read("count", view="snapshot", as_of=LAST, strict=True).collect(), historical)
    current = lake.items.read("count", start=TAIL, end=TAIL, view="snapshot", as_of=TAIL).collect()
    assert current["value"].to_list() == [1.] and current["_baseline"].to_list() == [True]
    assert lake.items.read("count", start=TAIL, end=TAIL, view="snapshot", as_of=TAIL, strict=True).collect().is_empty()


def test_old_upper_before_information_cutoff_cannot_use_future_scope_proof(tmp_path, monkeypatch):
    lake = _lake(tmp_path)
    end = date(2026, 10, 11)
    raw = DatasetSpec("daily", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    lake.raw.ingest(raw, _row(LAST), mode="initialize", ingested_at=_received(START))
    lake.raw.ingest(raw, _row(START), ingested_at=_received(START))
    request = RawInput("custom", "daily", alias="raw", view="versions", include_historical_baseline=True)
    old = lake.inputs.freeze({"raw": replace(request, start=START, end=end)}, information_cutoff=LAST)
    wider = lake.inputs.freeze({"raw": replace(request, start=START, end=TAIL)}, information_cutoff=LAST)
    assert lake.inputs._baseline_at(old, "raw", LAST) is False
    assert lake.inputs._baseline_at(wider, "raw", LAST) is True
    lake.items.register_producer("identity", "1", lambda context: context.frames["raw"],
        update_policy=ProducerUpdatePolicy("same_date"))
    lake.items.register(DataItemSpec("item", (request,), producer_key="identity", producer_revision="1",
        time_column="source_time"))
    lake.items.replace_range("item", _row(START), start=START, end=end, available_date=LAST, input_receipt=old)
    monkeypatch.setattr(lake.inputs, "read", lambda *_a, **_k: pytest.fail("Unproven old-cutoff extension consumed values"))
    plan = lake.items.plan_update("item", start=START, end=TAIL, input_windows={"raw": (START, TAIL)})
    assert plan[0]["mode"] == "blocked" and "baseline timing" in plan[0]["reason"]


def test_empty_item_upper_extension_keeps_original_transitive_guard(tmp_path, monkeypatch):
    lake = _lake(tmp_path)
    raw = DatasetSpec("daily", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    lake.raw.ingest(raw, _row(START), mode="initialize", ingested_at=_received(START))
    lake.raw.ingest(raw, _row(LAST, 12.), ingested_at=_received(LAST))
    declared = RawInput("custom", "daily", alias="raw", view="versions", include_historical_baseline=True)
    lake.items.register(DataItemSpec("empty", (declared,)))
    empty = pl.DataFrame(schema={"time": pl.Date, "asset_id": pl.String, "value": pl.Float64})
    wide_parent = lake.inputs.freeze({"raw": replace(declared, start=START, end=TAIL)}, information_cutoff=LAST)
    lake.items.replace_range("empty", empty, start=START, end=TAIL, available_date=LAST, input_receipt=wide_parent)
    narrow_parent = lake.inputs.freeze({"raw": replace(declared, start=date(2026, 10, 2), end=LAST)},
        information_cutoff=LAST)
    lake.items.replace_range("empty", empty, start=START, end=LAST, available_date=LAST, input_receipt=narrow_parent)
    request = ItemInput("empty", alias="leaf", view="versions")
    old = lake.inputs.freeze({"leaf": replace(request, start=START, end=LAST)}, information_cutoff=LAST)
    wider = lake.inputs.freeze({"leaf": replace(request, start=START, end=TAIL)}, information_cutoff=LAST)
    assert lake.inputs.read(old, "leaf").collect().is_empty() and lake.inputs.read(wider, "leaf").collect().is_empty()
    assert lake.inputs._baseline_at(old, "leaf", LAST) is False
    assert lake.inputs._baseline_at(wider, "leaf", LAST) is True
    calls = []
    lake.items.register_producer("count", "1", lambda context: calls.append(context) or _row(context.end, 0.),
        update_policy=ProducerUpdatePolicy("same_date"))
    lake.items.register(DataItemSpec("child", (request,), producer_key="count", producer_revision="1"))
    lake.items.replace_range("child", _row(LAST, 0.), start=START, end=LAST, available_date=LAST, input_receipt=old)
    monkeypatch.setattr(lake.inputs, "read", lambda *_a, **_k: pytest.fail("Unproven transitive Item scope consumed values"))
    plan = lake.items.plan_update("child", start=START, end=TAIL, input_windows={"leaf": (START, TAIL)})
    assert plan[0]["mode"] == "blocked" and "baseline timing" in plan[0]["reason"]
    assert not calls
