"""Explicit producer prerequisites share the authoritative atomic planner states."""
from datetime import UTC, date, datetime
import json
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, ExecutionOptions, ProducerUpdatePolicy, RawInput, input_index

FIELDS = ("asset_id", "market")


def received(day):
    return datetime(2025, 1, day, tzinfo=UTC)


def setup(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    general = DatasetSpec("metadata", "general")
    frame = pl.DataFrame({"asset_id": ["A", "B"], "market": ["main", "growth"], "unused": ["old", "old"]})
    lake.raw.ingest(general, frame, ingested_at=received(2))
    daily = DatasetSpec("daily", "by_date", date_kind="calendar", field_mappings={"time": "time", "asset_id": "asset_id"})
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
    original = lake.inputs.get(report.frozen_receipt_id)
    calls.clear()
    return lake, general, frame, original, calls


@pytest.mark.parametrize("middle_changed", [False, True])
def test_original_and_every_same_day_atomic_scope_prepared_without_producer(tmp_path, monkeypatch, middle_changed):
    lake, spec, frame, original, calls = setup(tmp_path)
    middle = frame.with_columns(pl.lit("middle unused").alias("unused"))
    if middle_changed:
        middle = middle.with_columns(pl.lit("growth").alias("market"))
    lake.raw.ingest(spec, middle, ingested_at=received(4))
    with lake._data_meta.connect() as db:
        middle_seq = db.execute("select max(seq) from version_commits where status='committed'").fetchone()[0]
    lake.raw.ingest(spec, frame.with_columns(pl.lit("final unused").alias("unused")), ingested_at=received(4))
    completed, manifest = lake.items.completed_builds("item"), lake.items.manifest("item")
    assert lake.items.plan_update("item", start="2025-01-02", end="2025-01-04")[0]["mode"] == ("blocked" if middle_changed else "incremental")
    progress = []
    prepared = lake.inputs.prepare_general_projections(original, "metadata", FIELDS, through="2025-01-04",
        progress=progress.append)
    assert prepared["work"]["prepared"] == 3 and prepared["work"]["batches"] == 3
    assert any(state["original_commit_ceiling"] == middle_seq for state in prepared["states"])
    assert all(state["status"] == "known" and state["proof_source"] for state in prepared["states"])
    assert not calls and lake.items.completed_builds("item") == completed and lake.items.manifest("item") == manifest
    assert lake.inputs.get(original).digest == original.digest
    def forbidden(*_a, **_k):
        raise AssertionError("Known preparation consumed original bytes")
    monkeypatch.setattr(input_index, "project_original", forbidden)
    again = lake.inputs.prepare_general_projections(original, "metadata", FIELDS, through="2025-01-04")
    assert again["work"]["prepared"] == again["work"]["new_sources"] == again["work"]["batches"] == 0
    assert again["work"]["reused"] == 3
    plan = lake.items.plan_update("item", start="2025-01-02", end="2025-01-04")
    if middle_changed:
        assert plan[0]["mode"] == "blocked" and f"max_commit={middle_seq}" in plan[0]["reason"]
        assert "consumed fields changed" in plan[0]["reason"]
    else:
        assert plan == [{"mode": "incremental", "start": "2025-01-04", "end": "2025-01-04"}]
    assert not calls


def test_partial_cancel_retains_completed_groups_and_retry_skips_originals(tmp_path, monkeypatch):
    lake, spec, frame, original, calls = setup(tmp_path)
    lake.raw.ingest(spec, frame.with_columns(pl.lit("new unused").alias("unused")), ingested_at=received(4))
    canceled = False
    def check():
        if canceled:
            raise RuntimeError("cancel preparation")
    def progress(work):
        nonlocal canceled
        canceled = work["prepared"] == 1
    with pytest.raises(RuntimeError, match="cancel preparation"):
        lake.inputs.prepare_general_projections(original, "metadata", FIELDS, through="2025-01-04",
            check_canceled=check, progress=progress)
    with sqlite3.connect(lake.data_meta_path) as db:
        assert db.execute("select count(*) from input_projection_index").fetchone()[0] == 1
    reads = []
    project = input_index.project_original
    monkeypatch.setattr(input_index, "project_original", lambda *a, **k: (reads.append(1), project(*a, **k))[1])
    result = lake.inputs.prepare_general_projections(original, "metadata", FIELDS, through="2025-01-04")
    assert result["work"]["reused"] == 1 and result["work"]["prepared"] == 1 and len(reads) == 1
    assert not calls and lake.inputs.get(original).digest == original.digest


def test_preparation_budget_rejects_before_sources_or_original_reads(tmp_path, monkeypatch):
    lake, _, _, original, calls = setup(tmp_path)
    before = lake.items.completed_builds("item")
    def forbidden(*_a, **_k):
        raise AssertionError("Rejected preparation consumed original bytes")
    monkeypatch.setattr(input_index, "project_original", forbidden)
    with sqlite3.connect(lake.data_meta_path) as db:
        count = db.execute("select count(*) from frozen_inputs").fetchone()[0]
    with pytest.raises(MemoryError, match="metadata"):
        lake.inputs.prepare_general_projections(original, "metadata", FIELDS, through="2025-01-04",
            config=ExecutionOptions(max_buffer_bytes=64))
    with sqlite3.connect(lake.data_meta_path) as db:
        assert db.execute("select count(*) from frozen_inputs").fetchone()[0] == count
    assert not calls and lake.items.completed_builds("item") == before


def test_preparation_does_not_invent_no_eligible_schema_or_relax_first_timing(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("metadata", "general")
    data = pl.DataFrame({"asset_id": ["A"], "market": ["main"]})
    lake.raw.ingest(spec, data, ingested_at=received(5))
    old = lake.inputs.freeze({"metadata": RawInput("custom", "metadata", include_historical_baseline=False)},
        information_cutoff="2025-01-03")
    result = lake.inputs.prepare_general_projections(old, "metadata", FIELDS, through="2025-01-03")
    assert result["work"]["unsupported"] and result["work"]["batches"] == result["work"]["prepared"] == 0
    assert result["states"] == []
    assert lake.inputs.get(old).digest == old.digest


@pytest.mark.parametrize("damage", ["source", "summary"])
def test_later_general_source_audit_follows_original_consumed_field_authority(tmp_path, damage):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.raw.ingest(DatasetSpec("metadata", "general"),
        pl.DataFrame({"asset_id": ["A"], "market": ["main"]}), ingested_at=received(2))
    original = lake.inputs.freeze({"metadata": RawInput("custom", "metadata")}, information_cutoff="2025-01-03")
    token = lake.inputs.index_selection(original, "metadata", view="snapshot", fields=FIELDS)
    current = lake.inputs.freeze(original.requests, information_cutoff="2025-01-04")
    assert lake.inputs.index_selection(current, "metadata", view="snapshot", fields=FIELDS) == token
    assert lake.inputs.verify(current)["upstream_receipt_count"] == 1
    with sqlite3.connect(lake.data_meta_path) as db:
        if damage == "source":
            db.execute("update frozen_inputs set payload_json=payload_json||' ' where receipt_id=?", (original.receipt_id,))
        else:
            db.row_factory = sqlite3.Row
            row = dict(db.execute("select * from input_projection_index").fetchone())
            row["identity"] = "0" * 64
            row["digest"] = input_index._digest({name: value for name, value in row.items() if name != "digest"})
            db.execute("update input_projection_index set identity=?,digest=?", (row["identity"], row["digest"]))
            assert json.loads(row["request_key"])["fields"] == list(FIELDS)
    with pytest.raises(RuntimeError, match="checksum" if damage == "source" else "differs from original"):
        lake.inputs.verify(current)
