from storage_fixtures import damage_original_segments
from datetime import UTC, date, datetime
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec, DataItemSpec, RawInput, ProducerUpdatePolicy


def fixture(tmp_path, *, declared_start=None):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("values", "by_date", date_kind="calendar", field_mappings={"time": "time", "asset_id": "asset_id"})
    frame = pl.DataFrame({"time": [date(2020, month, 1) for month in (1, 2, 3)],
                          "asset_id": ["A"] * 3, "value": [1., 2., 3.]})
    lake.raw.ingest(spec, frame, mode="initialize", ingested_at=datetime(2020, 3, 2, tzinfo=UTC))
    def pointwise(context):
        return context.frames["raw"].select(pl.col("source_time").alias("time"), "asset_id", "value")
    lake.items.register_producer("pointwise", "1", pointwise)
    definition = DataItemSpec("derived", (RawInput("custom", "values", alias="raw", start=declared_start),),
                              producer_key="pointwise", producer_revision="1")
    lake.items.register(definition)
    return lake, spec, definition


def test_explicit_month_scope_is_frozen_without_changing_definition(tmp_path):
    lake, _, definition = fixture(tmp_path)
    report = lake.items.update("derived", start="2020-02-01", end="2020-02-29", input_windows={"raw": ("2020-02-01", "2020-02-29")})
    assert report.frozen_receipt_id is not None
    receipt = lake.inputs.get(report.frozen_receipt_id)
    assert str(receipt.requests["raw"].start) == "2020-02-01"
    assert str(receipt.requests["raw"].end) == "2020-02-29"
    assert lake.inputs.read(receipt, "raw").collect()["value"].to_list() == [2.]
    assert lake.items.get("derived") == definition
    assert lake.inputs.verify(receipt)["valid"] and lake.inputs.is_current(receipt)
    result = lake.items.read("derived", start="2020-02-01", end="2020-02-29", view="versions").collect()
    assert result["value"].to_list() == [2.]
    second = lake.items.update("derived", start="2020-03-01", end="2020-03-31", input_windows={"raw": ("2020-03-01", "2020-03-31")})
    assert lake.items.get("derived") == definition
    assert lake.inputs.is_current(receipt)
    assert second.rows_committed == 1


def test_pointwise_widened_scope_reuses_completed_month_and_repairs_only_changed_day(tmp_path):
    lake, spec, definition = fixture(tmp_path)
    calls = []
    def producer(context):
        calls.append((context.start, context.end, context.frames["raw"]["source_time"].to_list()))
        return context.frames["raw"].select(pl.col("source_time").alias("time"), "asset_id", "value")
    lake.items.register_producer("pointwise", "1", producer, update_policy=ProducerUpdatePolicy("same_date"))
    first = lake.items.update("derived", start="2020-02-01", end="2020-02-29",
        input_windows={"raw": ("2020-02-01", "2020-02-29")})
    calls.clear()
    lake.raw.ingest(spec, pl.DataFrame({"time": [date(2020, 2, 1)], "asset_id": ["A"], "value": [9.]}),
        ingested_at=datetime(2020, 3, 3, tzinfo=UTC))
    lake.items.update("derived", start="2020-02-01", end="2020-03-31")
    assert lake.items.get("derived") == definition
    assert calls and all(start == date(2020, 2, 1) or start >= date(2020, 3, 1) for start, _, _ in calls)
    assert all(date(2020, 1, 1) not in days for _, _, days in calls)
    assert lake.items.read("derived", view="snapshot", as_of="2020-03-31").collect()["value"].to_list() == [9., 3.]
    assert first.frozen_receipt_id is not None
    assert lake.inputs.get(first.frozen_receipt_id).requests["raw"].start == "2020-02-01"


def test_pointwise_scope_extension_cannot_certify_missing_old_input_range(tmp_path):
    lake, _, _ = fixture(tmp_path)
    lake.items.update("derived", start="2020-02-01", end="2020-03-31",
        input_windows={"raw": ("2020-02-01", "2020-02-29")})
    lake.items.register_producer("pointwise", "1", lambda context: context.frames["raw"].select(
        pl.col("source_time").alias("time"), "asset_id", "value"), update_policy=ProducerUpdatePolicy("same_date"))
    with pytest.raises(RuntimeError, match="input windows changed"):
        lake.items.update("derived", start="2020-02-01", end="2020-03-31")


@pytest.mark.parametrize("older_baseline", [False, True])
def test_read_only_plan_and_update_share_whole_scope_timing_preflight(tmp_path, monkeypatch, older_baseline):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    raw = DatasetSpec("daily", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    def row(day, value):
        return pl.DataFrame({"time": [day], "asset_id": ["A"], "value": [value]})
    if older_baseline:
        lake.raw.ingest(raw, row(date(2019, 12, 31), 1.), mode="initialize",
            ingested_at=datetime(2019, 12, 31, tzinfo=UTC))
    lake.raw.ingest(raw, row(date(2020, 1, 2), 2.),
        mode="incremental" if older_baseline else "initialize",
        ingested_at=datetime(2020, 1, 2, tzinfo=UTC))
    calls = []
    def producer(context):
        calls.append(context.start)
        return context.frames["raw"].select(pl.col("source_time").alias("time"), "asset_id", "value")
    lake.items.register_producer("p", "1", producer, update_policy=ProducerUpdatePolicy("same_date"))
    lake.items.register(DataItemSpec("derived", (RawInput("custom", "daily", alias="raw"),),
        producer_key="p", producer_revision="1"))
    windows = {"raw": ("2020-01-01", "2020-01-31")}
    lake.items.update("derived", start="2020-01-01", end="2020-01-31", input_windows=windows)
    original = lake.items.builds("derived")
    calls.clear()
    def forbidden(*args, **kwargs):
        raise AssertionError("Planning/rejected update read values or created a frozen receipt")
    monkeypatch.setattr(lake.inputs, "read", forbidden)
    monkeypatch.setattr(lake.inputs, "freeze", forbidden)
    assert lake.items.plan_update("derived", start="2020-01-01", end="2020-01-31", input_windows=windows) == []
    plan = lake.items.plan_update("derived", start="2020-01-01", end="2020-01-31")
    assert len(plan) == 1 and plan[0]["mode"] == "blocked" and "baseline timing" in plan[0]["reason"]
    with pytest.raises(RuntimeError, match="baseline timing"):
        lake.items.update("derived", start="2020-01-01", end="2020-01-31")
    assert not calls and lake.items.builds("derived") == original


@pytest.mark.parametrize("windows,error", [({"missing": ("2020-02-01", "2020-02-29")}, "Unknown"),
                                         ({"raw": ("2020-02-29", "2020-02-01")}, "precedes"),
                                         ({"raw": ("2019-01-01", "2020-02-29")}, "widen")])
def test_invalid_window_fails_before_build_evidence(tmp_path, windows, error):
    lake, _, _ = fixture(tmp_path, declared_start="2020-01-01")
    with pytest.raises(ValueError, match=error):
        lake.items.update("derived", start="2020-02-01", end="2020-02-29", input_windows=windows)
    assert not lake.items.builds("derived")


def test_reuse_trusts_records_and_explicit_audit_verifies_original_bytes(tmp_path, monkeypatch):
    lake, _, _ = fixture(tmp_path)
    options = {"start": "2020-02-01", "end": "2020-02-29", "input_windows": {"raw": ("2020-02-01", "2020-02-29")}}
    first = lake.items.update("derived", **options)
    def unexpected_read(*args, **kwargs):
        raise AssertionError("unchanged build decoded dependency frames")
    monkeypatch.setattr(lake.inputs, "read", unexpected_read)
    reused = lake.items.update("derived", **options)
    assert reused.status == "unchanged" and reused.commit_seq == first.commit_seq
    with sqlite3.connect(lake.data_meta_path) as db:
        damage_original_segments(db, 'commit_seq=1', ())
    assert lake.items.update("derived", **options).status == "unchanged"
    with pytest.raises(RuntimeError):
        assert reused.frozen_receipt_id is not None
        lake.inputs.verify(reused.frozen_receipt_id)


def test_month_pruning_keeps_later_revision_of_earlier_observation(tmp_path, monkeypatch):
    lake, spec, _ = fixture(tmp_path)
    lake.raw.ingest(spec, pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": [9.]}),
                    ingested_at=datetime(2020, 2, 10, tzinfo=UTC))
    receipt = lake.inputs.freeze({"raw": RawInput("custom", "values", start="2020-01-01", end="2020-01-31", view="snapshot")}, information_cutoff="2020-02-29")
    from bagelquant_data import inputs
    original = inputs.read_batch
    partitions = []
    def tracked(*args, **kwargs):
        partitions.append(args[1])
        return original(*args, **kwargs)
    monkeypatch.setattr(inputs, "read_batch", tracked)
    result = lake.inputs.read(receipt, "raw").collect()
    assert result["value"].to_list() == [9.]
    assert not any("month=03" in partition for partition in partitions)
    assert any("month=02" in partition for partition in partitions)
    assert len(receipt.evidence["raw"]["batches"]) == 2
    assert lake.inputs.verify(receipt)["valid"]


def test_unbounded_producer_retains_history_and_window_change_requires_impact_proof(tmp_path):
    lake, _, _ = fixture(tmp_path)
    old = lake.items.update("derived", start="2020-02-01", end="2020-02-29")
    assert old.frozen_receipt_id is not None
    receipt = lake.inputs.get(old.frozen_receipt_id)
    assert receipt.requests["raw"].start is None
    assert lake.inputs.read(receipt, "raw").collect()["value"].to_list() == [1., 2.]
    with pytest.raises(RuntimeError, match="input windows changed"):
        lake.items.update("derived", start="2020-02-01", end="2020-02-29", input_windows={"raw": ("2020-02-01", "2020-02-29")})
    assert lake.inputs.is_current(receipt)


def test_windowed_item_receipt_ignores_other_month_and_detects_late_revision(tmp_path):
    from bagelquant_data import ItemInput
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("published"))
    january = pl.DataFrame({"time": [date(2020, 1, 2)], "asset_id": ["A"], "value": [1.]})
    lake.items.ingest("published", january, available_date="2020-01-02")
    frozen = lake.inputs.freeze({"item": ItemInput("published", start="2020-01-01", end="2020-01-31", view="versions")})
    original_digest = frozen.digest
    lake.items.ingest("published", january.with_columns(pl.lit(date(2020, 2, 2)).alias("time"), pl.lit(2.).alias("value")), available_date="2020-02-02")
    assert lake.inputs.is_current(frozen)
    lake.items.ingest("published", january.with_columns(pl.lit(9.).alias("value")), available_date="2020-02-15")
    assert not lake.inputs.is_current(frozen)
    assert lake.inputs.get(frozen).digest == original_digest
    assert lake.inputs.read(frozen, "item").collect()["value"].to_list() == [1.]
    assert lake.inputs.verify(frozen)["valid"]


def test_legacy_windowed_receipt_keeps_original_representation_current(tmp_path, monkeypatch):
    lake, _, _ = fixture(tmp_path)
    original = lake.inputs._capture
    def legacy_capture(*args, **kwargs):
        return original(*args, **{**kwargs, "scoped_aliases": set()})
    with monkeypatch.context() as patch:
        patch.setattr(lake.inputs, "_capture", legacy_capture)
        legacy = lake.inputs.freeze({"raw": RawInput("custom", "values", start="2020-02-01", end="2020-02-29")}, information_cutoff="2020-02-29")
    assert "scoped_batches" not in legacy.evidence["raw"]
    assert len(legacy.evidence["raw"]["batches"]) == 3
    modern = lake.inputs.freeze(legacy.requests, information_cutoff="2020-02-29")
    assert len(modern.evidence["raw"]["batches"]) == 1
    assert lake.inputs.is_current(legacy) and lake.inputs.is_current(modern)
    assert lake.inputs.get(legacy).digest == legacy.digest


def test_empty_month_receipt_stays_current_across_other_month_build(tmp_path):
    from bagelquant_data import ItemInput
    lake, _, _ = fixture(tmp_path)
    lake.items.update("derived", start="2019-12-01", end="2019-12-31", input_windows={"raw": ("2019-12-01", "2019-12-31")})
    frozen = lake.inputs.freeze({"item": ItemInput("derived", start="2019-12-01", end="2019-12-31", view="versions")})
    lake.items.update("derived", start="2020-02-01", end="2020-02-29", input_windows={"raw": ("2020-02-01", "2020-02-29")})
    assert lake.inputs.is_current(frozen)


def test_empty_observation_window_detects_later_in_window_content(tmp_path):
    from bagelquant_data import ItemInput
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("published"))
    february = pl.DataFrame({"time": [date(2020, 2, 2)], "asset_id": ["A"], "value": [2.]})
    lake.items.ingest("published", february, available_date="2020-02-02")
    frozen = lake.inputs.freeze({"item": ItemInput("published", start="2020-01-01", end="2020-01-31", view="versions")})
    assert lake.inputs.is_current(frozen)
    lake.items.ingest("published", february.with_columns(pl.lit(date(2020, 1, 2)).alias("time")), available_date="2020-03-02")
    assert not lake.inputs.is_current(frozen)


def test_frozen_reads_ignore_later_mutation_of_live_batch_summaries(tmp_path, monkeypatch):
    lake, _, _ = fixture(tmp_path)
    original = lake.inputs._capture
    def legacy_capture(*args, **kwargs):
        return original(*args, **{**kwargs, "scoped_aliases": set()})
    requests = {"raw": RawInput("custom", "values", start="2020-02-01", end="2020-02-29", view="snapshot")}
    with monkeypatch.context() as patch:
        patch.setattr(lake.inputs, "_capture", legacy_capture)
        legacy = lake.inputs.freeze(requests, information_cutoff="2020-02-29")
    scoped = lake.inputs.freeze(requests, information_cutoff="2020-02-29")
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("update version_segments set min_observation='1899-01-01',max_observation='1899-01-02',min_available='2099-01-01'")
    for frozen in (legacy, scoped):
        assert lake.inputs.read(frozen, "raw").collect()["value"].to_list() == [2.]
        assert lake.inputs.verify(frozen)["valid"]
        assert lake.inputs.get(frozen).digest == frozen.digest
