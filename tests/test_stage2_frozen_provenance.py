from datetime import UTC, date, datetime

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, ItemInput, RawInput


def _lake(root, *, read_only=False):
    return DataLake.open(data_meta_path=root / "data_meta.sqlite",
                         lake_path=root / "lake", read_only=read_only)


def _raw(kind="by_date"):
    return DatasetSpec("raw", kind, date_kind="calendar",
                       field_mappings={"time": "time", "asset_id": "asset_id"})


def _rows(value=1.0):
    return pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": [value]})


def _received(day):
    return datetime(2020, 1, day, tzinfo=UTC)


@pytest.mark.parametrize("kind", ["by_date", "general"])
def test_external_receipt_propagates_selected_baseline_and_later_attestation(tmp_path, kind):
    lake = _lake(tmp_path)
    lake.raw.ingest(_raw(kind), _rows(), mode="initialize", ingested_at=_received(1))
    request = RawInput("custom", "raw", view="versions", include_historical_baseline=True,
                       fields=("time", "asset_id", "value"))
    before = lake.inputs.freeze({"raw": request}, information_cutoff="2020-01-03")
    lake.items.register(DataItemSpec("item", (request,), producer_key="external", producer_revision="1"))
    lake.items.ingest("item", _rows(), available_date="2020-01-01", input_receipt=before)
    assert lake.items.read("item", view="snapshot", as_of="2020-01-01", strict=True).collect().is_empty()
    assert lake.items.read("item", view="versions").collect()["_baseline"].to_list() == [True]

    lake.raw.ingest(_raw(kind), _rows(), ingested_at=_received(3))
    after = lake.inputs.freeze({"raw": request}, information_cutoff="2020-01-03")
    lake.items.ingest("item", _rows(), available_date="2020-01-03", input_receipt=after)
    assert lake.items.read("item", view="snapshot", as_of="2020-01-02", strict=True).collect().is_empty()
    assert lake.items.read("item", view="snapshot", as_of="2020-01-03", strict=True).collect()["value"].to_list() == [1.0]
    assert lake.items.read("item", view="versions").collect()["_baseline"].to_list() == [True, False]


def test_external_empty_general_receipt_keeps_baseline_evidence(tmp_path):
    lake = _lake(tmp_path)
    lake.raw.ingest(_raw("general"), _rows().head(0), mode="initialize", ingested_at=_received(1))
    request = RawInput("custom", "raw", view="versions", include_historical_baseline=True)
    lake.items.register(DataItemSpec("count", (request,), value_dtype="int64",
                                      producer_key="count", producer_revision="1"))
    output = _rows(0)
    before = lake.inputs.freeze({"raw": request}, information_cutoff="2020-01-03")
    lake.items.ingest("count", output, available_date="2020-01-01", input_receipt=before)
    assert lake.items.read("count", view="snapshot", as_of="2020-01-01", strict=True).collect().is_empty()
    lake.raw.ingest(_raw("general"), _rows().head(0), ingested_at=_received(3))
    after = lake.inputs.freeze({"raw": request}, information_cutoff="2020-01-03")
    lake.items.ingest("count", output, available_date="2020-01-03", input_receipt=after)
    assert lake.items.read("count", view="snapshot", as_of="2020-01-03", strict=True).collect()["value"].to_list() == [0]


def test_one_external_batch_keeps_baseline_and_verified_versions_separate(tmp_path):
    lake = _lake(tmp_path)
    lake.raw.ingest(_raw(), _rows(), mode="initialize", ingested_at=_received(1))
    lake.raw.ingest(_raw(), _rows(), ingested_at=_received(3))
    request = RawInput("custom", "raw", view="versions", include_historical_baseline=True)
    receipt = lake.inputs.freeze({"raw": request}, information_cutoff="2020-01-03")
    lake.items.register(DataItemSpec("item", (request,), producer_key="external", producer_revision="1"))
    output = pl.concat([
        _rows().with_columns(pl.lit(date(2020, 1, 1)).alias("version_available_date")),
        _rows().with_columns(pl.lit(date(2020, 1, 3)).alias("version_available_date")),
    ])
    report = lake.items.ingest("item", output, input_receipt=receipt)
    assert report.rows_committed == 1
    assert lake.items.read("item", view="snapshot", as_of="2020-01-02", strict=True).collect().is_empty()
    assert lake.items.read("item", view="snapshot", as_of="2020-01-03", strict=True).collect()["value"].to_list() == [1.0]
    assert lake.items.read("item", view="versions").collect()["_baseline"].to_list() == [True, False]


def test_explicit_item_availability_attests_unchanged_baseline_at_that_date(tmp_path):
    lake = _lake(tmp_path)
    lake.items.register(DataItemSpec("item"))
    lake.items.ingest("item", _rows(), historical_baseline=True, available_date="2020-01-01")
    lake.items.ingest("item", _rows(), available_date="2020-01-03")
    receipt = lake.inputs.freeze({"item": ItemInput("item", view="snapshot", strict=True)},
                                 information_cutoff="2020-01-03")
    assert lake.inputs.read(receipt, "item").collect()["value"].to_list() == [1.0]
    assert lake.items.read("item", view="snapshot", as_of="2020-01-02", strict=True).collect().is_empty()
    assert lake.items.read("item", view="snapshot", as_of="2020-01-03", strict=True).collect()["value"].to_list() == [1.0]


def test_frozen_read_overrides_keep_window_and_cutoff_after_archive(tmp_path):
    lake = _lake(tmp_path)
    lake.raw.ingest(_raw(), _rows(), ingested_at=_received(1))
    lake.raw.ingest(_raw(), _rows(2.0), ingested_at=_received(2))
    request = RawInput("custom", "raw", view="snapshot", fields=("value",),
                       observation_start="2020-01-01", observation_end="2020-01-01")
    receipt = lake.inputs.freeze({"raw": request}, information_cutoff="2020-01-02")
    lake.raw.ingest(_raw(), _rows(3.0), ingested_at=_received(3))
    lake.raw.remove("raw", source="custom")
    reader = _lake(tmp_path, read_only=True).inputs
    assert reader.read(receipt, "raw").collect().to_dict(as_series=False) == {"value": [2.0]}
    assert reader.read(receipt, "raw", view="history").collect()["value"].to_list() == [1.0]
    assert reader.read(receipt, "raw", view="versions").collect()["value"].to_list() == [1.0, 2.0]
    assert reader.read(receipt, "raw", as_of="2020-01-01").collect()["value"].to_list() == [1.0]
    observed = reader.read(receipt, "raw", observations=True, fields=("time", "value")).collect()
    assert observed.to_dict(as_series=False) == {"time": [date(2020, 1, 1)], "value": [2.0]}
    with pytest.raises(ValueError, match="frozen information cutoff"):
        reader.read(receipt, "raw", as_of="2020-01-03")


def test_frozen_item_projection_and_strict_override_preserve_original_evidence(tmp_path):
    lake = _lake(tmp_path)
    lake.items.register(DataItemSpec("item"))
    lake.items.ingest("item", _rows(), historical_baseline=True, available_date="2020-01-01")
    receipt = lake.inputs.freeze({"item": ItemInput("item", view="snapshot")}, information_cutoff="2020-01-02")
    lake.items.remove("item")
    reader = _lake(tmp_path, read_only=True).inputs
    assert reader.read(receipt, "item", fields=("time", "value"), observations=True).collect().columns == ["time", "value"]
    assert reader.read(receipt, "item", strict=True).collect().is_empty()
    assert reader.read(receipt, "item", view="versions", fields=("value",)).collect()["value"].to_list() == [1.0]


def test_frozen_transitive_input_receipts_survive_archive_and_fail_on_corruption(tmp_path):
    lake = _lake(tmp_path)
    lake.raw.ingest(_raw(), _rows(), ingested_at=_received(1))
    raw = RawInput("custom", "raw", view="snapshot")
    first = lake.inputs.freeze({"raw": raw}, information_cutoff="2020-01-01")
    lake.items.register(DataItemSpec("first", (raw,), producer_key="first", producer_revision="1"))
    lake.items.ingest("first", _rows(), available_date="2020-01-01", input_receipt=first)
    item = ItemInput("first", view="snapshot")
    second = lake.inputs.freeze({"item": item}, information_cutoff="2020-01-01")
    lake.items.register(DataItemSpec("second", (item,), producer_key="second", producer_revision="1"))
    lake.items.ingest("second", _rows(), available_date="2020-01-01", input_receipt=second)
    # Same-content publication also retains the exact check's input receipt.
    lake.items.ingest("second", _rows(), available_date="2020-01-01", input_receipt=second)
    final = lake.inputs.freeze({"item": ItemInput("second", view="snapshot")}, information_cutoff="2020-01-02")
    assert final.evidence["item"]["parent_receipts"] == {second.receipt_id: second.digest}
    assert final.evidence["item"]["audit_checks"][0]["input_receipt_id"] == second.receipt_id
    lake.items.remove("second")
    lake.items.remove("first")
    lake.raw.remove("raw", source="custom")
    reader = _lake(tmp_path, read_only=True).inputs
    assert reader.verify(final)["upstream_receipt_count"] == 2
    assert reader.read(final, "item").collect()["value"].to_list() == [1.0]
    with lake._data_meta.connect() as db:
        db.execute("update frozen_inputs set payload_json='{}' where receipt_id=?", (first.receipt_id,))
    with pytest.raises(RuntimeError, match="receipt checksum"):
        reader.verify(final)


@pytest.mark.parametrize("kind", ["by_date", "general"])
def test_verified_same_content_checks_preserve_semantic_dependency_identity(tmp_path, kind):
    lake = _lake(tmp_path)
    lake.raw.ingest(_raw(kind), _rows(), ingested_at=_received(1))
    request = RawInput("custom", "raw", view="versions")
    before = lake.inputs.freeze({"raw": request}, information_cutoff="2020-01-04")
    lake.raw.ingest(_raw(kind), _rows(), ingested_at=_received(3))
    after = lake.inputs.freeze({"raw": request}, information_cutoff="2020-01-04")
    assert after.max_check_id > before.max_check_id
    assert after.digest != before.digest
    assert after.dependency_digest == before.dependency_digest
    assert after.evidence["raw"]["checks"] == []
    assert after.evidence["raw"]["record_checks"] == []
    assert len(after.evidence["raw"]["audit_checks"]) == 1


def test_fresh_parent_receipt_retains_check_proof_without_invalidating_item_content(tmp_path):
    lake = _lake(tmp_path)
    lake.raw.ingest(_raw(), _rows(), ingested_at=_received(1))
    request = RawInput("custom", "raw", view="versions")
    first = lake.inputs.freeze({"raw": request}, information_cutoff="2020-01-04")
    lake.items.register(DataItemSpec("item", (request,), producer_key="external", producer_revision="1"))
    lake.items.ingest("item", _rows(), available_date="2020-01-01", input_receipt=first)
    before = lake.inputs.freeze({"item": ItemInput("item", view="versions")}, information_cutoff="2020-01-04")
    lake.raw.ingest(_raw(), _rows(), ingested_at=_received(3))
    second = lake.inputs.freeze({"raw": request}, information_cutoff="2020-01-04")
    report = lake.items.ingest("item", _rows(), available_date="2020-01-03", input_receipt=second)
    after = lake.inputs.freeze({"item": ItemInput("item", view="versions")}, information_cutoff="2020-01-04")
    assert report.rows_committed == 0
    assert after.dependency_digest == before.dependency_digest
    assert after.evidence["item"]["parent_receipts"] == {first.receipt_id: first.digest, second.receipt_id: second.digest}
    assert after.evidence["item"]["audit_checks"][0]["input_receipt_id"] == second.receipt_id
    assert lake.inputs.verify(after)["upstream_receipt_count"] == 2
