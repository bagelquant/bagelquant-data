from datetime import UTC, date, datetime
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec, DataItemSpec, RawInput


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


@pytest.mark.parametrize("windows,error", [({"missing": ("2020-02-01", "2020-02-29")}, "Unknown"),
                                         ({"raw": ("2020-02-29", "2020-02-01")}, "precedes"),
                                         ({"raw": ("2019-01-01", "2020-02-29")}, "widen")])
def test_invalid_window_fails_before_build_evidence(tmp_path, windows, error):
    lake, _, _ = fixture(tmp_path, declared_start="2020-01-01")
    with pytest.raises(ValueError, match=error):
        lake.items.update("derived", start="2020-02-01", end="2020-02-29", input_windows=windows)
    assert not lake.items.builds("derived")


def test_reuse_verifies_original_bytes_without_input_decoding(tmp_path, monkeypatch):
    lake, _, _ = fixture(tmp_path)
    options = {"start": "2020-02-01", "end": "2020-02-29", "input_windows": {"raw": ("2020-02-01", "2020-02-29")}}
    first = lake.items.update("derived", **options)
    def unexpected_read(*args, **kwargs):
        raise AssertionError("unchanged build decoded dependency frames")
    monkeypatch.setattr(lake.inputs, "read", unexpected_read)
    reused = lake.items.update("derived", **options)
    assert reused.status == "unchanged" and reused.commit_seq == first.commit_seq
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("update version_batches set payload=x'00' where commit_seq=1")
    with pytest.raises(RuntimeError):
        lake.items.update("derived", **options)


def test_month_pruning_keeps_later_revision_of_earlier_observation(tmp_path, monkeypatch):
    lake, spec, _ = fixture(tmp_path)
    lake.raw.ingest(spec, pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": [9.]}),
                    ingested_at=datetime(2020, 2, 10, tzinfo=UTC))
    receipt = lake.inputs.freeze({"raw": RawInput("custom", "values", start="2020-01-01", end="2020-01-31", view="snapshot")}, information_cutoff="2020-02-29")
    from bagelquant_data import inputs
    original = inputs.read_batch
    partitions = []
    def tracked(*args):
        partitions.append(args[1])
        return original(*args)
    monkeypatch.setattr(inputs, "read_batch", tracked)
    result = lake.inputs.read(receipt, "raw").collect()
    assert result["value"].to_list() == [9.]
    assert not any("month=03" in partition for partition in partitions)
    assert any("month=02" in partition for partition in partitions)
    assert len(receipt.evidence["raw"]["batches"]) == 2
    assert lake.inputs.verify(receipt)["valid"]


def test_unbounded_producer_retains_history_and_explicit_window_changes_dependency_identity(tmp_path):
    lake, _, _ = fixture(tmp_path)
    old = lake.items.update("derived", start="2020-02-01", end="2020-02-29")
    assert old.frozen_receipt_id is not None
    receipt = lake.inputs.get(old.frozen_receipt_id)
    assert receipt.requests["raw"].start is None
    assert lake.inputs.read(receipt, "raw").collect()["value"].to_list() == [1., 2.]
    scoped = lake.items.update("derived", start="2020-02-01", end="2020-02-29", input_windows={"raw": ("2020-02-01", "2020-02-29")})
    assert old.dependency_digest != scoped.dependency_digest
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
        db.execute("update version_batches set min_observation='1899-01-01',max_observation='1899-01-02',min_available='2099-01-01'")
    for frozen in (legacy, scoped):
        assert lake.inputs.read(frozen, "raw").collect()["value"].to_list() == [2.]
        assert lake.inputs.verify(frozen)["valid"]
        assert lake.inputs.get(frozen).digest == frozen.digest
