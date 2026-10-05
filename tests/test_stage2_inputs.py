from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, ItemInput, RawInput, input_read_boundary


def lake_at(tmp_path: Path, **kwargs) -> DataLake:
    return DataLake.open(data_meta_path=tmp_path / "data_meta.sqlite", lake_path=tmp_path / "lake", **kwargs)


def test_frozen_bytes_survive_revisions_archival_and_missing_parquet(tmp_path):
    lake = lake_at(tmp_path)
    spec = DatasetSpec("prices", "by_date", date_kind="calendar", field_mappings={"time":"time", "asset_id":"asset_id"})
    first = pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "close": [1.0]})
    lake.raw.ingest(spec, first, ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    frozen = lake.inputs.freeze({"raw": RawInput("custom", "prices", view="snapshot")}, information_cutoff="2020-01-01")
    lake.raw.ingest(spec, first.with_columns(pl.lit(9.0).alias("close")), ingested_at=datetime(2020, 1, 2, tzinfo=UTC))
    lake.raw.remove("prices", source="custom")
    for file in (tmp_path / "lake").rglob("*.parquet"):
        file.unlink()
    assert lake.inputs.read(frozen, "raw").collect()["close"].to_list() == [1.0]
    assert lake.inputs.verify(frozen)["valid"] is True
    reopened = lake_at(tmp_path, read_only=True)
    assert reopened.inputs.get(frozen.receipt_id).digest == frozen.digest
    assert reopened.inputs.read(frozen.receipt_id, "raw").collect()["close"].to_list() == [1.0]


def test_item_freeze_cutoff_and_definition_evidence(tmp_path):
    lake = lake_at(tmp_path)
    lake.items.register(DataItemSpec("item"))
    first = pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": [1.0]})
    lake.items.ingest("item", first, available_date="2020-01-01")
    first_commit = lake.inputs.max_commit()
    lake.items.ingest("item", first.with_columns(pl.lit(2.0).alias("value")), available_date="2020-01-02")
    frozen = lake.inputs.freeze({"item": ItemInput("item", view="snapshot")}, information_cutoff="2020-01-02", max_commit=first_commit)
    assert frozen.max_commit == first_commit
    assert frozen.evidence["item"]["definition"]["value_dtype"] == "float64"
    assert frozen.evidence["item"]["schema_hash"]
    lake.items.remove("item")
    assert lake.inputs.read(frozen, "item").collect()["value"].to_list() == [1.0]
    assert lake.inputs.verify(frozen)["batch_count"] == 1


def test_scoped_input_boundary_captured_by_items(tmp_path):
    lake = lake_at(tmp_path)
    lake.items.register(DataItemSpec("item"))
    frame = pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": [1.0]})
    lake.items.ingest("item", frame, available_date="2020-01-01")
    maximum = lake.inputs.max_commit()
    with input_read_boundary(tmp_path / "data_meta.sqlite", maximum, "2020-01-01"):
        bounded = lake_at(tmp_path)
    lake.items.ingest("item", frame.with_columns(pl.lit(3.0).alias("value")), available_date="2020-01-02")
    assert bounded.items.read("item").collect()["value"].to_list() == [1.0]
    with pytest.raises(ValueError, match="boundary"):
        bounded.items.read("item", max_commit=maximum + 1)
    with pytest.raises(ValueError, match="cutoff"):
        bounded.items.read("item", view="snapshot", as_of="2020-01-02")


def test_frozen_corrupt_receipt_and_payload_fail_closed(tmp_path):
    lake = lake_at(tmp_path)
    lake.items.register(DataItemSpec("item"))
    lake.items.ingest("item", pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": [1.0]}))
    frozen = lake.inputs.freeze({"item": ItemInput("item")})
    with lake._data_meta.connect() as db:
        db.execute("update version_batches set content_hash='wrong'")
    with pytest.raises(RuntimeError, match="checksum"):
        lake.inputs.verify(frozen)
    with lake._data_meta.connect() as db:
        db.execute("update frozen_inputs set payload_json='{}'")
    with pytest.raises(RuntimeError, match="receipt checksum"):
        lake.inputs.get(frozen)


@pytest.mark.parametrize("update_type", ["general", "by_date"])
def test_same_value_attestations_are_frozen_and_causal(tmp_path, update_type):
    lake = lake_at(tmp_path)
    spec = DatasetSpec("raw", update_type, date_kind="calendar", field_mappings={"time": "time", "asset_id": "asset_id"})
    frame = pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": [1.0]})
    lake.raw.ingest(spec, frame, mode="initialize", ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    original_commit = lake.inputs.max_commit()
    request = RawInput("custom", "raw", view="snapshot", strict=True)
    before = lake.inputs.freeze({"raw": request}, information_cutoff="2020-01-03")
    assert lake.inputs.read(before, "raw").collect().is_empty()
    with input_read_boundary(tmp_path / "data_meta.sqlite", original_commit, "2020-01-03"):
        bounded = lake_at(tmp_path)
    # An unchanged collection supplies timing evidence while retaining content.
    lake.raw.ingest(spec, frame, ingested_at=datetime(2020, 1, 3, tzinfo=UTC))
    assert lake.inputs.max_commit() == original_commit
    after = lake.inputs.freeze({"raw": request}, information_cutoff="2020-01-03")
    assert after.max_check_id > before.max_check_id
    assert lake.inputs.read(after, "raw").collect()["value"].to_list() == [1.0]
    assert lake.inputs.read(before, "raw").collect().is_empty()
    assert bounded.raw.read("raw", source="custom", strict=True, as_of="2020-01-03").collect().is_empty()
    early = lake.inputs.freeze({"raw": request}, information_cutoff="2020-01-02")
    assert lake.inputs.read(early, "raw").collect().is_empty()
    assert lake.inputs.verify(before)["valid"] is True
    assert lake.inputs.verify(after)["valid"] is True

