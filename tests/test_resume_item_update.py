"""Original-bound recovery never turns partial rows into completion evidence."""
from concurrent.futures import CancelledError
from dataclasses import replace
from datetime import UTC, date, datetime
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, ProducerUpdatePolicy, RawInput
from bagelquant_data.items import api


@pytest.fixture
def interrupted(tmp_path, monkeypatch):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    raw = DatasetSpec("daily", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    lake.raw.ingest(raw, pl.DataFrame({"time": [date(2020, 1, 2), date(2020, 1, 3)],
        "asset_id": ["A", "A"], "value": [2., 3.]}), mode="initialize",
        ingested_at=datetime(2020, 1, 3, tzinfo=UTC))
    calls = []

    def producer(context):
        calls.append((context.start, context.end))
        return context.frames["daily"].select(pl.col("source_time").alias("time"),
            "asset_id", (pl.col("value") * 3).alias("value"))

    for revision in ("old", "new"):
        lake.items.register_producer("p", revision, producer, update_policy=ProducerUpdatePolicy("same_date"))
        spec = DataItemSpec("item", (RawInput("custom", "daily", alias="daily"),),
            producer_key="p", producer_revision=revision)
        lake.items.register(spec)
        if revision == "old":
            lake.items.initialize("item", start="2020-01-01", end="2020-01-03")
    # Reproduce interruption after _ingest's durable publication report but
    # before _build's complete requested-range certificate.
    with monkeypatch.context() as patch:
        patch.setattr(lake.items, "read", lambda *a, **k: (_ for _ in ()).throw(CancelledError()))
        report = lake.items.update("item", start="2020-01-01", end="2020-01-03")
    assert report.status == "cancelled"
    original = lake.items.builds("item")[-1]
    assert original["start_date"] == "2020-01-02"
    assert lake.items.plan_update("item", start="2020-01-01", end="2020-01-03")[0]["mode"] == "blocked"
    calls.clear()
    return lake, raw, spec, original, calls


def resume(fixture, **options):
    lake, _, _, original, _ = fixture
    return lake.items.resume_update("item", build_id=original["id"],
        start="2020-01-01", end="2020-01-03", **options)


def test_resume_reuses_original_source_preserves_partial_files_and_certifies_after_success(interrupted, monkeypatch):
    lake, _, _, original, calls = interrupted
    files = {path: path.read_bytes() for path in lake.lake_path.rglob("*.parquet")}
    old_builds = lake.items.builds("item")
    monkeypatch.setattr(lake.inputs, "freeze", lambda *a, **k: pytest.fail("resume replaced its original Source"))
    report = resume(interrupted)
    assert report.status == "success" and report.frozen_receipt_id == original["frozen_receipt_id"]
    assert calls == [(date(2020, 1, 1), date(2020, 1, 3))]
    assert all(path.read_bytes() == content for path, content in files.items())
    assert lake.items.builds("item")[:len(old_builds)] == old_builds
    completed = lake.items.completed_builds("item")[-1]
    assert completed["start_date"] == "2020-01-01" and completed["end_date"] == "2020-01-03"
    actual = lake.items.read("item", view="snapshot", as_of="2020-01-03").collect()
    assert actual["value"].to_list() == [6., 9.]
    calls.clear()
    assert lake.items.plan_update("item", start="2020-01-01", end="2020-01-03") == []
    assert not calls
    with pytest.raises(ValueError, match="completed current definition"):
        resume(interrupted)


def test_interrupted_resume_retains_partial_frontier_without_inventing_completion(interrupted, monkeypatch):
    lake, _, _, original, _ = interrupted
    with monkeypatch.context() as patch:
        patch.setattr(lake.items, "read", lambda *a, **k: (_ for _ in ()).throw(CancelledError()))
        assert resume(interrupted).status == "cancelled"
    assert not any(row["definition_hash"] == original["definition_hash"] for row in lake.items.completed_builds("item"))
    assert resume(interrupted).status == "success"


@pytest.mark.parametrize("change", ["definition", "source", "cutoff", "foreign_build", "lost_authority"])
def test_resume_rejects_changed_or_ambiguous_original_authority(interrupted, change):
    lake, raw, spec, original, calls = interrupted
    options = {"build_id": original["id"], "start": "2020-01-01", "end": "2020-01-03"}
    if change == "definition":
        lake.items.register(replace(spec, producer_revision="different"))
    elif change == "source":
        lake.raw.ingest(raw, pl.DataFrame({"time": [date(2020, 1, 2)], "asset_id": ["A"],
            "value": [100.]}), ingested_at=datetime(2020, 1, 3, 1, tzinfo=UTC))
    elif change == "cutoff":
        options["end"] = "2020-01-04"
    elif change == "foreign_build":
        options["build_id"] = lake.items.builds("item")[0]["id"]
    else:
        assert resume(interrupted).status == "success"
        with sqlite3.connect(lake.data_meta_path) as db:
            db.execute("delete from item_completion_proofs where build_id in "
                "(select id from item_builds where name='item' and definition_hash=?)", (original["definition_hash"],))
        calls.clear()
    with pytest.raises((ValueError, RuntimeError)):
        lake.items.resume_update("item", **options)
    assert not calls


@pytest.mark.parametrize("race", ["source", "definition", "same_source_output", "cancel"])
def test_final_recovery_fences_reject_competing_changes_and_late_cancellation(interrupted, monkeypatch, race):
    lake, raw, spec, original, _ = interrupted
    compare = api._same_ordered_rows
    fired = False
    canceled = False

    def during_final_comparison(*args, **kwargs):
        nonlocal fired, canceled
        result = compare(*args, **kwargs)
        if not fired:
            fired = True
            if race == "source":
                lake.raw.ingest(raw, pl.DataFrame({"time": [date(2020, 1, 2)],
                    "asset_id": ["A"], "value": [100.]}),
                    ingested_at=datetime(2020, 1, 3, 1, tzinfo=UTC))
            elif race == "definition":
                lake.items.register(replace(spec, producer_revision="different"))
            elif race == "same_source_output":
                lake.items.ingest("item", pl.DataFrame({"time": [date(2020, 1, 2)],
                    "asset_id": ["A"], "value": [99.]}), available_date=date(2020, 1, 3),
                    input_receipt=original["frozen_receipt_id"])
            else:
                canceled = True
        return result

    monkeypatch.setattr(api, "_same_ordered_rows", during_final_comparison)
    if race == "cancel":
        assert resume(interrupted, cancelled=lambda: canceled).status == "cancelled"
    else:
        with pytest.raises(RuntimeError):
            resume(interrupted)
    assert fired
    assert not any(row["definition_hash"] == original["definition_hash"] for row in lake.items.completed_builds("item"))


def test_cancellation_after_certificate_write_rolls_back_whole_range(interrupted, monkeypatch):
    lake, _, _, original, _ = interrupted
    save = api._save_completion
    canceled = False

    def after_save(*args, **kwargs):
        nonlocal canceled
        result = save(*args, **kwargs)
        canceled = True
        return result

    monkeypatch.setattr(api, "_save_completion", after_save)
    assert resume(interrupted, cancelled=lambda: canceled).status == "cancelled"
    assert canceled
    assert not any(row["definition_hash"] == original["definition_hash"] for row in lake.items.completed_builds("item"))


def test_existing_read_context_or_competing_writer_rejects_before_producer(interrupted):
    lake, _, _, original, calls = interrupted
    with lake.inputs.read_context(original["frozen_receipt_id"]):
        with pytest.raises(ValueError, match="input read context"):
            resume(interrupted)
    with lake._data_meta.dataset_writer("items", "item", "other-writer"):
        with pytest.raises(RuntimeError, match="already active"):
            resume(interrupted)
    assert not calls
