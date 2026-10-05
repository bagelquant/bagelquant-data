from datetime import UTC, date, datetime

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, ItemInput, RawInput, input_read_boundary


def _lake(root):
    return DataLake.open(data_meta_path=root / "data_meta.sqlite", lake_path=root / "lake")


def _rows():
    return pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": [1.0]})


def _general():
    return DatasetSpec("raw", "general", date_kind="calendar",
                       field_mappings={"time": "time", "asset_id": "asset_id"})


def _received(day):
    return datetime(2020, 1, day, tzinfo=UTC)


@pytest.mark.parametrize("native", [False, True])
@pytest.mark.parametrize("attested", [False, True])
def test_empty_item_preserves_transitive_baseline_and_attestation_cutoff(tmp_path, native, attested):
    lake = _lake(tmp_path)
    lake.raw.ingest(_general(), _rows().head(0), mode="initialize", ingested_at=_received(1))
    if attested:
        lake.raw.ingest(_general(), _rows().head(0), ingested_at=_received(3))
    raw = RawInput("custom", "raw", view="versions", include_historical_baseline=True)
    lake.items.register(DataItemSpec("empty", (raw,), producer_key="empty", producer_revision="1"))
    lake.items.register_producer("empty", "1", lambda context: _rows().head(0))
    if native:
        lake.items.initialize("empty", start="2020-01-01", end="2020-01-04")
    else:
        inputs = lake.inputs.freeze({"raw": raw}, information_cutoff="2020-01-04")
        lake.items.replace_range("empty", _rows().head(0), start="2020-01-01", end="2020-01-04",
                                 available_date="2020-01-04", input_receipt=inputs)
    item = ItemInput("empty", view="versions", start="2020-01-01", end="2020-01-04")
    lake.items.register(DataItemSpec("count", (item,), producer_key="count", producer_revision="1"))
    if native:
        def count(context):
            return _rows().with_columns(pl.lit(float(context.read("empty").height)).alias("value"))
        lake.items.register_producer("count", "1", count)
        lake.items.initialize("count", start="2020-01-01", end="2020-01-04")
    else:
        inputs = lake.inputs.freeze({"empty": item}, information_cutoff="2020-01-04")
        frame = pl.concat([_rows().with_columns(pl.lit(0.0).alias("value"),
                           pl.lit(date(2020, 1, day)).alias("version_available_date")) for day in (1, 3)])
        lake.items.ingest("count", frame, input_receipt=inputs)
    assert lake.items.read("count", strict=True, view="snapshot", as_of="2020-01-02").collect().is_empty()
    strict = lake.items.read("count", strict=True, view="snapshot", as_of="2020-01-04").collect()
    assert strict["value"].to_list() == ([0.0] if attested else [])
    frozen = lake.inputs.freeze({"count": ItemInput("count", view="snapshot", strict=True)},
                                information_cutoff="2020-01-04")
    assert lake.inputs.verify(frozen)["valid"]
    assert lake.inputs.read(frozen, "count").collect()["value"].to_list() == ([0.0] if attested else [])


def test_later_empty_build_cannot_cross_commit_or_check_boundaries(tmp_path):
    lake = _lake(tmp_path)
    lake.raw.ingest(_general(), _rows().head(0), mode="initialize", ingested_at=_received(1))
    raw = RawInput("custom", "raw", view="versions", include_historical_baseline=True)
    lake.items.register(DataItemSpec("empty", (raw,)))
    first_inputs = lake.inputs.freeze({"raw": raw}, information_cutoff="2020-01-04")
    lake.items.replace_range("empty", _rows().head(0), start="2020-01-01", end="2020-01-04",
                             available_date="2020-01-04", input_receipt=first_inputs)
    item = ItemInput("empty", view="versions", start="2020-01-01", end="2020-01-04")
    before = lake.inputs.freeze({"empty": item}, information_cutoff="2020-01-04")
    lake.raw.ingest(_general(), _rows().head(0), ingested_at=_received(3))
    after_inputs = lake.inputs.freeze({"raw": raw}, information_cutoff="2020-01-04")
    lake.items.replace_range("empty", _rows().head(0), start="2020-01-01", end="2020-01-04",
                             available_date="2020-01-04", input_receipt=after_inputs)
    with input_read_boundary(lake.data_meta_path, before.max_commit, "2020-01-04", max_check_id=before.max_check_id):
        bounded = _lake(tmp_path)
    bounded_inputs = bounded.inputs.freeze({"empty": item}, information_cutoff="2020-01-04")
    assert bounded_inputs.evidence["empty"]["empty_item_build"]["frozen_receipt_id"] == first_inputs.receipt_id
    bounded.items.register(DataItemSpec("bounded_count", (item,)))
    bounded.items.ingest("bounded_count", _rows().with_columns(pl.lit(0.0).alias("value")),
                          available_date="2020-01-04", input_receipt=bounded_inputs)
    assert lake.items.read("bounded_count", view="versions", strict=True).collect().is_empty()
    lake.raw.ingest(_general(), _rows(), ingested_at=_received(4))
    later = lake.inputs.freeze({"raw": raw}, information_cutoff="2020-01-04")
    lake.items.replace_range("empty", _rows().head(0), start="2020-01-01", end="2020-01-04",
                             available_date="2020-01-04", input_receipt=later)
    retained = lake.inputs.freeze({"empty": item}, information_cutoff="2020-01-04", max_commit=before.max_commit)
    assert retained.evidence["empty"]["empty_item_build"]["input_commit"] <= before.max_commit


@pytest.mark.parametrize("leaf_item", [False, True])
def test_empty_baseline_without_content_commit_is_not_verified_absence(tmp_path, leaf_item):
    lake = _lake(tmp_path)
    if leaf_item:
        lake.items.register(DataItemSpec("input"))
        lake.items.ingest("input", _rows().head(0), historical_baseline=True, available_date="2020-01-01")
        request = ItemInput("input", start="2020-01-01", end="2020-01-03", view="versions")
        # An unscoped later empty response cannot certify this complete window.
        lake.items.ingest("input", _rows().head(0), available_date="2020-01-03")
    else:
        spec = DatasetSpec("input", "by_date", date_kind="calendar",
                           field_mappings={"time": "time", "asset_id": "asset_id"})
        lake.raw.ingest(spec, _rows().head(0), mode="initialize", ingested_at=_received(1))
        lake.raw.ingest(spec, _rows().head(0), ingested_at=_received(3))
        request = RawInput("custom", "input", view="versions", include_historical_baseline=True)
    inputs = lake.inputs.freeze({"input": request}, information_cutoff="2020-01-03")
    assert inputs.evidence["input"]["empty_checks"]
    lake.items.register(DataItemSpec("count", (request,)))
    lake.items.ingest("count", _rows().with_columns(pl.lit(0.0).alias("value")),
                      available_date="2020-01-03", input_receipt=inputs)
    assert lake.items.read("count", view="snapshot", as_of="2020-01-03", strict=True).collect().is_empty()
    lake.items.register(DataItemSpec("native_count", (request,), producer_key="count", producer_revision="1"))
    lake.items.register_producer("count", "1", lambda context: _rows().with_columns(pl.lit(0.0).alias("value")))
    lake.items.initialize("native_count", start="2020-01-01", end="2020-01-03")
    assert lake.items.read("native_count", view="snapshot", as_of="2020-01-03", strict=True).collect().is_empty()
    if leaf_item:
        lake.items.replace_range("input", _rows().head(0), start="2020-01-01", end="2020-01-03", available_date="2020-01-03")
        verified = lake.inputs.freeze({"input": request}, information_cutoff="2020-01-03")
        assert verified.dependency_digest != inputs.dependency_digest
        assert not lake.inputs.is_current(inputs)
        lake.items.ingest("count", _rows().with_columns(pl.lit(0.0).alias("value")),
                          available_date="2020-01-03", input_receipt=verified)
        assert lake.items.read("count", view="snapshot", as_of="2020-01-03", strict=True).collect()["value"].to_list() == [0.0]
        lake.items.update("native_count", start="2020-01-01", end="2020-01-03")
        assert lake.items.read("native_count", view="snapshot", as_of="2020-01-02", strict=True).collect().is_empty()
        assert lake.items.read("native_count", view="snapshot", as_of="2020-01-03", strict=True).collect()["value"].to_list() == [0.0]
        lake.items.replace_range("input", _rows().head(0), start="2020-01-01", end="2020-01-03", available_date="2020-01-04")
        rechecked = lake.inputs.freeze({"input": request}, information_cutoff="2020-01-03")
        assert rechecked.dependency_digest == verified.dependency_digest
