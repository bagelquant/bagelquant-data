from datetime import UTC, date, datetime, timedelta
import hashlib
import json
import sqlite3

import polars as pl
from polars.testing import assert_frame_equal
import pytest

from bagelquant_data import DataLake, DatasetSpec, DataItemSpec, ItemInput, RawInput, ProducerUpdatePolicy, ExecutionOptions
from bagelquant_data.items import timing


class Fixture:
    def __init__(self, root, *, empty=False, calendar_known=None, category="one", first=date(2020, 1, 29)):
        self.first, self.previous, self.known = first, date(2020, 2, 1), date(2020, 2, 2)
        self.lake = DataLake.open(data_meta_path=root / "meta.sqlite", lake_path=root / "lake")
        self.calls = []
        self.fail_tail = False
        days = (self.known - self.first).days + 1
        self.calendar = pl.DataFrame({"time": [self.first + timedelta(days=i) for i in range(days)], "is_open": [1]*days})
        self.membership = pl.DataFrame({"asset_id": [] if empty else ["A"], "category": [] if empty else [category]},
            schema={"asset_id": pl.String, "category": pl.String})
        for name, frame in (("calendar", self.calendar), ("membership", self.membership)):
            known = calendar_known if name == "calendar" and calendar_known is not None else datetime(2020, 2, 2, 1, tzinfo=UTC)
            self.lake.raw.ingest(DatasetSpec(name, "general"), frame, mode="initialize",
                ingested_at=datetime(2020, 1, 28, tzinfo=UTC) if calendar_known else datetime(2020, 2, 2, tzinfo=UTC))
            self.lake.raw.ingest(DatasetSpec(name, "general"), frame,
                ingested_at=known)
        self.requests = (RawInput("custom", "calendar", alias="calendar"), RawInput("custom", "membership", alias="membership"))
        self.policy = ProducerUpdatePolicy("forward", timing_neutral_inputs=("calendar", "membership"))
        self.lake.items.register_producer("p", "1", self.producer, update_policy=self.policy)
        self.lake.items.register(DataItemSpec("item", self.requests, value_dtype="string", producer_key="p", producer_revision="1"))
        self.original = self.lake.items.initialize("item", start=self.first, end=self.previous)
        self.calls.clear()

    def producer(self, context):
        self.calls.append((context.start, context.end))
        if self.fail_tail and context.start == self.known:
            raise RuntimeError("tail interrupted")
        return context.frames["calendar"].select("time").join(
            context.frames["membership"].select("asset_id", pl.col("category").alias("value")), how="cross")

    def plan(self):
        return self.lake.items.plan_update("item", start=self.first, end=self.known)

    def update(self, **kwargs):
        return self.lake.items.update("item", start=self.first, end=self.known, **kwargs)


@pytest.mark.parametrize("empty", [False, True])
def test_first_general_verification_reuses_saved_values_and_matches_full_reference(tmp_path, monkeypatch, empty):
    f = Fixture(tmp_path, empty=empty)
    originals = {path: path.read_bytes() for path in f.lake.lake_path.rglob("*.parquet")}
    from bagelquant_data import inputs, input_index
    def forbidden(*args, **kwargs):
        pytest.fail("planning read values or prepared proof")
    with monkeypatch.context() as patch:
        patch.setattr(inputs, "read_batch", forbidden)
        patch.setattr(input_index, "project_original", forbidden)
        assert f.plan() == [
            {"mode": "attest", "start": str(f.first), "end": str(f.previous), "available_date": str(f.known)},
            {"mode": "incremental", "start": str(f.known), "end": str(f.known)}]
    with pytest.raises(RuntimeError, match="recertification requires unchanged"):
        f.lake.items.recertify("item", start=f.first, end=f.known)
    assert not f.calls
    report = f.update()
    assert report.status == "success"
    assert f.calls == [(f.known, f.known)]
    assert all(path.read_bytes() == content for path, content in originals.items())
    f.lake.items.register_producer("reference", "1", f.producer)
    f.lake.items.register(DataItemSpec("reference", f.requests, value_dtype="string", producer_key="reference", producer_revision="1"))
    f.lake.items.initialize("reference", start=f.first, end=f.known)
    fields = ["time", "asset_id", "value", "observation_date", "available_date", "version_available_date", "_baseline"]
    for cutoff in (f.previous, f.known):
        for view in ("history", "snapshot"):
            for strict in (False, True):
                actual = f.lake.items.read("item", view=view, as_of=cutoff, strict=strict).collect().select(fields)
                expected = f.lake.items.read("reference", view=view, as_of=cutoff, strict=strict).collect().select(fields)
                assert_frame_equal(actual.sort("time", "asset_id"), expected.sort("time", "asset_id"))
    f.calls.clear()
    assert f.plan() == []
    assert f.update().status == "unchanged"
    assert not f.calls
    assert f.original.frozen_receipt_id is not None
    assert f.lake.inputs.verify(f.original.frozen_receipt_id)["valid"]


def test_timing_cancellation_reuses_completed_month_on_retry(tmp_path, monkeypatch):
    f = Fixture(tmp_path)
    canceled = False
    def progress(report):
        nonlocal canceled
        if report.end == date(2020, 1, 31):
            canceled = True
    assert f.update(cancelled=lambda: canceled, progress=progress).status == "cancelled"
    assert not f.calls
    before = f.lake.items.completion_statuses(names=["item"])[0]
    assert before["completed_end"] == str(f.previous)
    reads = []
    original_read = f.lake.items.read
    def read(name, **kwargs):
        reads.append((kwargs.get("start"), kwargs.get("end")))
        return original_read(name, **kwargs)
    monkeypatch.setattr(f.lake.items, "read", read)
    assert f.update().status == "success"
    assert (f.first, date(2020, 1, 31)) not in reads
    assert f.calls == [(f.known, f.known)]


def test_many_timing_checkpoints_do_not_reread_completed_visibility_events(tmp_path, monkeypatch):
    f = Fixture(tmp_path, first=date(2019, 2, 1))
    scans, completed = [], []
    scan = pl.scan_parquet

    def tracked(path, **options):
        if not isinstance(path, list) and ".visibility" in str(path):
            scans.append(path)
        return scan(path, **options)

    monkeypatch.setattr(pl, "scan_parquet", tracked)
    assert f.update(progress=lambda report: completed.append((report.start, report.end))).status == "success"
    assert scans == []
    assert f.calls == [(f.known, f.known)]
    assert len({(first.year, first.month) for first, _ in completed}) == 13
    assert f.lake.items.read("item", view="snapshot", as_of=f.known, strict=True).collect().height == 367


def test_timing_check_and_unit_certificate_roll_back_together(tmp_path, monkeypatch):
    f = Fixture(tmp_path)
    paths = set(f.lake.lake_path.rglob("*.parquet"))
    def fail(*args, **kwargs):
        raise RuntimeError("checkpoint failure")
    with monkeypatch.context() as patch:
        patch.setattr(timing, "save_completion", fail)
        with pytest.raises(RuntimeError, match="checkpoint failure"):
            f.update()
    with sqlite3.connect(f.lake.data_meta_path) as db:
        assert db.execute("select count(*) from version_checks where source='items' and dataset='item'").fetchone()[0] == 0
    assert set(f.lake.lake_path.rglob("*.parquet")) == paths
    assert f.update().status == "success"


def test_successful_timing_prefix_survives_failed_append(tmp_path):
    f = Fixture(tmp_path)
    f.fail_tail = True
    with pytest.raises(RuntimeError, match="tail interrupted"):
        f.update()
    assert f.plan() == [{"mode": "incremental", "start": str(f.known), "end": str(f.known)}]
    completed = f.lake.items.completed_builds("item")[-1]
    assert completed["end_date"] == str(f.previous)
    f.fail_tail = False
    assert f.update().status == "success"
    assert all(start == end == f.known for start, end in f.calls)


def test_changed_general_content_and_undeclared_timing_remain_blocked(tmp_path):
    f = Fixture(tmp_path)
    f.lake.raw.ingest(DatasetSpec("membership", "general"), f.membership.with_columns(pl.lit("two").alias("category")),
        ingested_at=datetime(2020, 2, 2, 2, tzinfo=UTC))
    assert f.plan()[0]["mode"] == "blocked"
    assert not f.calls


@pytest.mark.parametrize("change", ["scope", "policy", "date", "unit_range", "cycle", "splice"])
def test_corrupt_timing_authority_fails_even_with_recomputed_container_digest(tmp_path, change):
    f = Fixture(tmp_path)
    f.fail_tail = True
    with pytest.raises(RuntimeError, match="tail interrupted"):
        f.update()
    with sqlite3.connect(f.lake.data_meta_path) as db:
        row = db.execute("select build_id,payload_json from item_completion_authorities "
            "where json_extract(payload_json,'$.timing.unit')=? order by build_id desc limit 1",
            (0 if change in {"cycle", "splice"} else 1,)).fetchone()
        body = json.loads(row[1])
        value = body["timing"]
        if change == "scope":
            value["scope"] = "0" * 64
        elif change == "policy":
            value["policy"]["timing_neutral_inputs"] = []
        elif change == "date":
            value["available_date"] = "2020-02-01"
        elif change == "unit_range":
            value["start"] = "2020-01-30"
        elif change == "cycle":
            value["units"] = [row[0]]
        else:
            value["units"] = [value["units"][0]] * 2
        encoded = timing.canonical(body)
        db.execute("update item_completion_authorities set payload_json=?,digest=? where build_id=?",
            (encoded, hashlib.sha256(encoded.encode()).hexdigest(), row[0]))
    with pytest.raises(RuntimeError, match="authority is damaged"):
        f.lake.items.completed_builds("item")


@pytest.mark.parametrize("category,budget", [("one", 1), ("x" * (256 * 1024), 1024 * 1024)])
def test_timing_memory_admission_precedes_saved_value_collection(tmp_path, monkeypatch, category, budget):
    f = Fixture(tmp_path, category=category)
    def forbidden(*args, **kwargs):
        pytest.fail("saved values collected before memory admission")
    monkeypatch.setattr(f.lake.items, "read", forbidden)
    with pytest.raises(MemoryError, match="before value reads"):
        f.update(config=ExecutionOptions(max_buffer_bytes=budget))
    assert not f.calls


@pytest.mark.parametrize("surviving_baseline", [False, True])
@pytest.mark.parametrize("reverse", [False, True])
def test_calendar_verification_plan_uses_only_metadata_for_partial_raw_checks(
    tmp_path, monkeypatch, surviving_baseline, reverse,
):
    lake = DataLake.open(data_meta_path=tmp_path / "data.sqlite", lake_path=tmp_path / "lake")
    first, previous, through = date(2020, 1, 1), date(2020, 2, 2), date(2020, 2, 3)
    frame = pl.DataFrame({"time": [first, date(2020, 2, 1)] if surviving_baseline else [date(2020, 2, 1)],
        "asset_id": ["A", "A"] if surviving_baseline else ["A"],
        "value": [1., 2.] if surviving_baseline else [2.]})
    calendar = pl.DataFrame({"time": [first, previous, through], "is_open": [1, 1, 1]})
    raw = DatasetSpec("raw", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    general = DatasetSpec("calendar", "general")
    received = datetime(2020, 2, 2, tzinfo=UTC)
    lake.raw.ingest(raw, frame, mode="initialize", ingested_at=received)
    lake.raw.ingest(general, calendar, mode="initialize", ingested_at=received)
    raw_alias, calendar_alias = ("z", "a") if reverse else ("a", "z")
    requests = (RawInput("custom", "raw", alias=raw_alias), RawInput("custom", "calendar", alias=calendar_alias))
    policy = ProducerUpdatePolicy("forward", timing_neutral_inputs=(calendar_alias,))
    calls = []
    def producer(context):
        calls.append((context.start, context.end))
        return context.frames[raw_alias].select(pl.col("source_time").alias("time"), "asset_id", "value")
    lake.items.register_producer("p", "1", producer, update_policy=policy)
    lake.items.register(DataItemSpec("item", requests,
        producer_key="p", producer_revision="1"))
    lake.items.initialize("item", start=first, end=previous)
    calls.clear()
    verified = datetime(2020, 2, 3, tzinfo=UTC)
    lake.raw.ingest(raw, frame.tail(1), ingested_at=verified)
    lake.raw.ingest(general, calendar, ingested_at=verified)

    from bagelquant_data import input_index, inputs
    def forbidden(*args, **kwargs):
        pytest.fail("timing prerequisite planning decoded originals or ran a producer")
    monkeypatch.setattr(input_index, "project_original", forbidden)
    monkeypatch.setattr(inputs, "read_batch", forbidden)
    monkeypatch.setattr(lake.inputs, "_baseline_at", forbidden)
    actions = lake.items.plan_update("item", start=first, end=through)
    assert not calls
    if surviving_baseline:
        assert actions == [{"mode": "incremental", "start": str(through), "end": str(through)}]
    else:
        assert len(actions) == 1 and actions[0]["mode"] == "blocked"
        assert "timing" in actions[0]["reason"]


def test_staggered_general_verification_and_unrelated_counters_keep_content_identity(tmp_path):
    f = Fixture(tmp_path, calendar_known=datetime(2020, 1, 30, tzinfo=UTC))
    selection = {"item": ItemInput("item", view="snapshot", start=f.first, end=f.previous)}
    before = f.lake.inputs.freeze(selection, information_cutoff=f.known)
    identity = f.lake.inputs.selection_identity(before, "item", view="snapshot", values_only=True)
    f.lake.raw.ingest(DatasetSpec("unrelated", "general"), pl.DataFrame({"v": [1]}),
        ingested_at=datetime(2020, 2, 2, tzinfo=UTC))
    assert f.plan()[0]["available_date"] == str(f.known)
    f.update()
    after = f.lake.inputs.freeze(selection, information_cutoff=f.known)
    assert f.lake.inputs.selection_identity(after, "item", view="snapshot", values_only=True) == identity
    assert f.calls == [(f.known, f.known)]


def test_unqualified_policy_and_pending_outputs_do_not_admit_attestation(tmp_path):
    f = Fixture(tmp_path)
    spec = f.lake.items.get("item")
    from dataclasses import replace
    f.lake.items.register_producer("unqualified", "1", f.producer, update_policy=ProducerUpdatePolicy("forward"))
    f.lake.items.register(replace(spec, name="unqualified", producer_key="unqualified"))
    f.lake.items.initialize("unqualified", start=f.first, end=f.previous)
    f.calls.clear()
    assert f.lake.items.plan_update("unqualified", start=f.first, end=f.known)[0]["mode"] == "blocked"
    original_pending = timing.pending_dates
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(timing, "pending_dates", lambda *args: [(str(f.first), str(f.known))])
        assert f.plan()[0]["mode"] == "blocked"
    assert timing.pending_dates is original_pending
    assert not f.calls


def test_downstream_item_keeps_historical_producer_work_after_parent_attestation(tmp_path):
    f = Fixture(tmp_path)
    calls = []
    def child(context):
        calls.append((context.start, context.end))
        return context.frames["parent"].select("time", "asset_id", "value")
    f.lake.items.register_producer("child", "1", child, update_policy=ProducerUpdatePolicy("same_date"))
    f.lake.items.register(DataItemSpec("child", (ItemInput("item", alias="parent"),), value_dtype="string",
        producer_key="child", producer_revision="1"))
    f.lake.items.initialize("child", start=f.first, end=f.previous)
    calls.clear()
    f.update()
    assert f.lake.items.plan_update("child", start=f.first, end=f.known) == [
        {"mode": "incremental", "start": str(f.known), "end": str(f.known)}]
    f.lake.items.update("child", start=f.first, end=f.known)
    assert calls == [(f.known, f.known)]
