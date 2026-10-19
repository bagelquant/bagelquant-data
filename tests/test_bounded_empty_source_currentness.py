"""Mixed Sources retain exact empty completion authority per captured alias."""
from dataclasses import replace
from datetime import UTC, date, datetime
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, ItemInput, ItemPublication, ProducerUpdatePolicy, RawInput


def prepared(tmp_path, *, gap=False):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    raw_spec = DatasetSpec("raw", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    rows = pl.DataFrame({"time": [date(2020, 1, 2), date(2020, 2, 3)],
        "asset_id": ["A", "A"], "value": [1., 2.]})
    lake.raw.ingest(raw_spec, rows, ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    dependency = RawInput("custom", "raw", view="versions")
    source = lake.inputs.freeze({"raw": dependency}, information_cutoff="2020-02-03")
    for name in ("physical", "empty"):
        lake.items.register(DataItemSpec(name, (dependency,), producer_key="external", producer_revision="1"))
    periods = [("2020-01-02", "2020-01-15" if gap else "2020-01-31"),
        ("2020-02-01", "2020-02-03")]
    for offset, (first, last) in enumerate(periods):
        with lake.items.publication(input_receipt=source) as publisher:
            publisher.publish([
                ItemPublication("physical", rows.slice(offset, 1).with_columns(
                    pl.lit(date(2020, 2, 3)).alias("version_available_date")),
                    start=first, end=last, complete_at="2020-02-03"),
                ItemPublication("empty", rows.clear(), start=first, end=last, complete_at="2020-02-03"),
            ])
    bounded = lake.inputs.freeze({name: ItemInput(name, start="2020-01-02", end="2020-02-03", view="versions")
        for name in ("physical", "empty")}, information_cutoff="2020-02-03")
    unbounded = lake.inputs.freeze({name: ItemInput(name, view="versions") for name in ("physical", "empty")},
        information_cutoff="2020-02-03")
    return lake, raw_spec, rows, bounded, unbounded


def forbid_values(monkeypatch, lake):
    def forbidden(*args, **kwargs):
        pytest.fail("Currentness must use metadata only")
    monkeypatch.setattr(lake.inputs, "read", forbidden)
    monkeypatch.setattr(lake.inputs, "verify", forbidden)
    monkeypatch.setattr(pl, "read_parquet", forbidden)
    monkeypatch.setattr(pl, "scan_parquet", forbidden)


def test_bounded_mixed_source_keeps_current_empty_completion_without_reads(tmp_path, monkeypatch):
    lake, _, _, bounded, unbounded = prepared(tmp_path)
    flags = {row["dataset"]: row for row in lake.items.completion_statuses()}
    assert flags["physical"]["completed_start"] == "2020-02-01"
    assert flags["empty"]["completed_start"] == "2020-01-02"
    assert bounded.evidence["empty"]["empty_item_build"] is None
    original = (bounded.receipt_id, bounded.digest, bounded.dependency_digest)
    forbid_values(monkeypatch, lake)
    assert lake.inputs.is_current(unbounded)
    assert lake.inputs.is_current(bounded)
    assert lake.inputs.is_current([bounded, unbounded])
    retained = lake.inputs.get(bounded.receipt_id)
    assert (retained.receipt_id, retained.digest, retained.dependency_digest) == original


def test_gapped_empty_completions_do_not_certify_bounded_source(tmp_path, monkeypatch):
    lake, _, _, bounded, _ = prepared(tmp_path, gap=True)
    forbid_values(monkeypatch, lake)
    assert not lake.inputs.is_current(bounded)


@pytest.mark.parametrize("change", ["empty_definition", "dependency_revision", "empty_removed", "physical_removed", "physical_revision"])
def test_relevant_change_or_removed_alias_still_rejects_source(tmp_path, monkeypatch, change):
    lake, raw_spec, rows, bounded, _ = prepared(tmp_path)
    if change == "empty_definition":
        lake.items.register(replace(lake.items.get("empty"), producer_revision="2"))
    elif change == "dependency_revision":
        lake.raw.ingest(raw_spec, rows.with_columns(pl.lit(99.).alias("value")),
            ingested_at=datetime(2020, 2, 2, tzinfo=UTC))
    elif change.endswith("_removed"):
        lake.items.remove(change.removesuffix("_removed"))
    else:
        source = lake.inputs.freeze({"raw": RawInput("custom", "raw", view="versions")}, information_cutoff="2020-02-03")
        lake.items.ingest("physical", rows.head(1).with_columns(pl.lit(99.).alias("value")),
            available_date="2020-02-02", input_receipt=source)
    forbid_values(monkeypatch, lake)
    assert not lake.inputs.is_current(bounded)


def test_unrelated_publication_does_not_invalidate_original_bounded_source(tmp_path, monkeypatch):
    lake, _, rows, bounded, _ = prepared(tmp_path)
    original = (bounded.receipt_id, bounded.digest, bounded.dependency_digest)
    lake.items.register(DataItemSpec("unrelated"))
    lake.items.ingest("unrelated", rows, available_date="2020-02-03")
    forbid_values(monkeypatch, lake)
    assert lake.inputs.is_current(bounded)
    retained = lake.inputs.get(bounded)
    assert (retained.receipt_id, retained.digest, retained.dependency_digest) == original


def test_matured_pending_output_is_not_admitted_by_alias_completion(tmp_path, monkeypatch):
    lake, _, _, _, _ = prepared(tmp_path)
    def delayed(context):
        return context.frames["raw"].filter(pl.col("source_time") == date(2020, 1, 2)).select(
            pl.col("source_time").alias("time"), "asset_id", "value",
            pl.lit(date(2020, 2, 4)).alias("available_date"))
    lake.items.register_producer("delayed", "1", delayed, update_policy=ProducerUpdatePolicy("same_date"))
    lake.items.register(DataItemSpec("pending", (RawInput("custom", "raw", alias="raw"),),
        producer_key="delayed", producer_revision="1"))
    lake.items.initialize("pending", start="2020-01-02", end="2020-02-03")
    source = lake.inputs.freeze({"pending": ItemInput("pending", start="2020-01-02", end="2020-02-03", view="versions"),
        "physical": ItemInput("physical", start="2020-01-02", end="2020-02-03", view="versions")},
        information_cutoff="2020-02-04")
    forbid_values(monkeypatch, lake)
    progress = []
    assert not lake.inputs.is_current(source, progress=progress.append)
    assert any(event["phase"] == "currentness_pending_outputs" and event["alias"] == "pending" for event in progress)


def test_compact_alias_headers_are_loaded_once_for_fallback_source(tmp_path, monkeypatch):
    lake, _, _, bounded, _ = prepared(tmp_path)
    actual = lake.inputs.describe
    calls = []
    def describe(source):
        calls.append(source.receipt_id if hasattr(source, "receipt_id") else source)
        return actual(source)
    monkeypatch.setattr(lake.inputs, "describe", describe)
    forbid_values(monkeypatch, lake)
    assert lake.inputs.is_current(bounded)
    assert calls.count(bounded.receipt_id) == 1


def test_corrupt_empty_completion_cannot_be_hidden_by_sibling_fallback(tmp_path):
    lake, _, _, bounded, _ = prepared(tmp_path)
    latest = lake.items.completed_builds("empty")[-1]
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("update item_completion_authorities set payload_json='{}' where build_id=?", (latest["id"],))
    with pytest.raises((RuntimeError, ValueError), match="completion|authority|digest|checksum"):
        lake.inputs.is_current(bounded)
