"""Old asset-addition descriptors cannot hide a lost selected baseline witness."""
from datetime import UTC, date, datetime
import json
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, ProducerUpdatePolicy, RawInput
from bagelquant_data import general_key_appends, input_index

FIELDS = ("asset_id", "market")


def fixture(root, *, attested=True, remove_original=False):
    lake = DataLake.open(data_meta_path=root / "meta.sqlite", lake_path=root / "lake")
    metadata = DatasetSpec("metadata", "general")
    before = pl.DataFrame({"asset_id": ["A"], "market": ["main"]})
    lake.raw.ingest(metadata, before, mode="initialize" if attested else "incremental",
        ingested_at=datetime(2025, 1, 2, tzinfo=UTC))
    if attested:
        lake.raw.ingest(metadata, before, ingested_at=datetime(2025, 1, 3, tzinfo=UTC))
    daily = DatasetSpec("daily", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    lake.raw.ingest(daily, pl.DataFrame({"time": [date(2025, 1, 2), date(2025, 1, 3)],
        "asset_id": ["A", "A"], "value": [1., 2.]}),
        ingested_at=datetime(2025, 1, 3, tzinfo=UTC))
    calls = []

    def producer(context):
        calls.append((context.start, context.end))
        return context.frames["daily"].join(context.frames["metadata"].select(FIELDS),
            on="asset_id").select(pl.col("source_time").alias("time"), "asset_id", "value")

    policy = ProducerUpdatePolicy("same_date", timing_neutral_inputs=("metadata",),
        general_fields=(("metadata", FIELDS),), output_anchors=("daily",),
        general_asset_local_inputs=("metadata",))
    lake.items.register_producer("p", "1", producer, update_policy=policy)
    lake.items.register(DataItemSpec("item", (RawInput("custom", "daily", alias="daily"),
        RawInput("custom", "metadata", alias="metadata")), producer_key="p", producer_revision="1"))
    report = lake.items.initialize("item", start="2025-01-02", end="2025-01-03")
    assert report.frozen_receipt_id is not None
    original = lake.inputs.get(report.frozen_receipt_id)
    after = pl.DataFrame({"asset_id": ["B"], "market": ["growth"]})
    if not remove_original:
        after = pl.concat([before, after])
    lake.raw.ingest(metadata, after, ingested_at=datetime(2025, 1, 4, tzinfo=UTC))
    lake.raw.ingest(daily, pl.DataFrame({"time": [date(2025, 1, 4)], "asset_id": ["B"],
        "value": [3.]}), ingested_at=datetime(2025, 1, 4, tzinfo=UTC))
    calls.clear()
    return lake, original, calls


def prepare(lake, original):
    return lake.inputs.prepare_general_projections(original, "metadata", FIELDS,
        through="2025-01-04", key_anchors=("daily",),
        completed_start="2025-01-02", completed_end="2025-01-03")


def unmark(lake):
    """Restore the exact pre-witness-marker serialized optional-proof shape."""
    count = 0
    with sqlite3.connect(lake.data_meta_path) as db:
        db.row_factory = sqlite3.Row
        for saved in db.execute("select * from input_projection_index where version='general_asset_append.v1'").fetchall():
            row = dict(saved)
            body = json.loads(row["identity"])
            body.pop("selected_witness", None)
            row["identity"] = json.dumps(body, sort_keys=True, separators=(",", ":"))
            row["digest"] = input_index._digest({key: value for key, value in row.items() if key != "digest"})
            db.execute("update input_projection_index set identity=?,digest=? where evidence_key=? and request_key=?",
                (row["identity"], row["digest"], row["evidence_key"], row["request_key"]))
            count += 1
    assert count


def plan(lake):
    return lake.items.plan_update("item", start="2025-01-02", end="2025-01-04")


def forbidden(*_args, **_kwargs):
    raise AssertionError("Metadata proof admission consumed Original payloads")


def test_old_attested_baseline_proof_cannot_certify_removed_historical_asset(tmp_path, monkeypatch):
    lake, original, calls = fixture(tmp_path, remove_original=True)
    completed = lake.items.completed_builds("item")
    assert lake.items.read("item", view="snapshot", as_of="2025-01-03").select(
        "time", "asset_id", "value").collect().sort("time").to_dicts() == [
            {"time": date(2025, 1, 2), "asset_id": "A", "value": 1.},
            {"time": date(2025, 1, 3), "asset_id": "A", "value": 2.}]
    correct = general_key_appends._general_frame

    def old_selected_frame(inputs, source, alias, fields, cutoff, options, check):
        frame = correct(inputs, source, alias, fields, cutoff, options, check)
        # Before selected-witness overlay, the later selected check could not
        # match physical baseline snapshot_date, so the original read was empty.
        return frame.head(0) if source.digest == original.digest else frame

    with monkeypatch.context() as patch:
        patch.setattr(general_key_appends, "_general_frame", old_selected_frame)
        assert prepare(lake, original)["key_appends"]["prepared"] == 1
    unmark(lake)

    with monkeypatch.context() as patch:
        patch.setattr(input_index, "project_original", forbidden)
        assert plan(lake)[0]["mode"] == "blocked"
    retried = prepare(lake, original)
    assert retried["key_appends"]["prepared"] == 0
    assert any(row["status"] == "unsupported" for row in retried["key_appends"]["states"])
    assert plan(lake)[0]["mode"] == "blocked"
    assert not calls and lake.items.completed_builds("item") == completed
    assert lake.inputs.get(original).digest == original.digest
    current = lake.inputs.freeze(original.requests, information_cutoff="2025-01-04")
    with pytest.raises(ValueError, match="changed or removed original consumed tuples"):
        lake.inputs.verify(current)


def test_sound_old_nonbaseline_descriptor_keeps_metadata_reuse_and_audit(tmp_path, monkeypatch):
    lake, original, calls = fixture(tmp_path, attested=False)
    assert prepare(lake, original)["key_appends"]["prepared"] == 1
    unmark(lake)
    with monkeypatch.context() as patch:
        patch.setattr(input_index, "project_original", forbidden)
        assert plan(lake) == [{"mode": "incremental", "start": "2025-01-04", "end": "2025-01-04"}]
        assert prepare(lake, original)["key_appends"]["prepared"] == 0
    current = lake.inputs.freeze(original.requests, information_cutoff="2025-01-04")
    lake.inputs.verify(current)
    assert not calls


def test_sound_old_attested_descriptor_refreshes_without_numerical_replay(tmp_path, monkeypatch):
    lake, original, calls = fixture(tmp_path)
    completed = lake.items.completed_builds("item")
    assert prepare(lake, original)["key_appends"]["prepared"] == 1
    unmark(lake)
    current = lake.inputs.freeze(original.requests, information_cutoff="2025-01-04")
    lake.inputs.verify(current)
    with monkeypatch.context() as patch:
        patch.setattr(input_index, "project_original", forbidden)
        assert plan(lake)[0]["mode"] == "blocked"
    assert prepare(lake, original)["key_appends"]["prepared"] == 1
    with monkeypatch.context() as patch:
        patch.setattr(input_index, "project_original", forbidden)
        assert plan(lake) == [{"mode": "incremental", "start": "2025-01-04", "end": "2025-01-04"}]
        assert prepare(lake, original)["key_appends"]["prepared"] == 0
    current = lake.inputs.freeze(original.requests, information_cutoff="2025-01-04")
    lake.inputs.verify(current)
    assert not calls and lake.items.completed_builds("item") == completed
