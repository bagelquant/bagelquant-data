"""Natural first-visibility events replace SQLite row-witness relationships."""
from datetime import UTC, date, datetime
import sqlite3
import polars as pl
import pytest
from bagelquant_data import DataLake, DatasetSpec, RawInput


def prepared(root):
    lake = DataLake.open(data_meta_path=root / "meta.sqlite", lake_path=root / "lake")
    spec = DatasetSpec("raw", "by_date", date_kind="calendar", field_mappings={"time": "time", "asset_id": "asset_id"})
    frame = pl.DataFrame({"time": [date(2024, 1, 1), date(2024, 1, 2)], "asset_id": ["A", "B"], "value": [1., 2.]})
    lake.raw.ingest(spec, frame, mode="initialize", ingested_at=datetime(2024, 1, 2, tzinfo=UTC))
    frozen = lake.inputs.freeze({"raw": RawInput("custom", "raw", strict=True, view="snapshot")}, information_cutoff="2024-01-04")
    return lake, spec, frame, frozen


def test_partial_first_visibility_keeps_exact_natural_observations(tmp_path):
    lake, spec, frame, frozen = prepared(tmp_path)
    lake.raw.ingest(spec, frame.head(1), ingested_at=datetime(2024, 1, 3, tzinfo=UTC))
    current = lake.inputs.freeze(frozen.requests, information_cutoff="2024-01-04")
    assert lake.inputs.read(frozen, "raw").collect().is_empty()
    result = lake.inputs.read(current, "raw").collect()
    assert result["asset_id"].to_list() == ["A"]
    assert result["time"].to_list() == [date(2024, 1, 3)]
    assert lake.inputs.verify((frozen, current))["valid"]
    with lake._data_meta.connect() as db:
        headers = lake._data_meta.visibility_headers(db, "custom", "raw")
        assert len(headers) == 1 and headers[0]["row_count"] == 1
        assert db.execute("select 1 from sqlite_master where name='version_check_records'").fetchone() is None


def test_repeated_verified_acquisition_adds_no_visibility_body(tmp_path):
    lake, spec, frame, _ = prepared(tmp_path)
    lake.raw.ingest(spec, frame, ingested_at=datetime(2024, 1, 3, tzinfo=UTC))
    with sqlite3.connect(lake.data_meta_path) as db:
        first = db.execute("select * from visibility_segments").fetchall()
    lake.raw.ingest(spec, frame, ingested_at=datetime(2024, 1, 4, tzinfo=UTC))
    with sqlite3.connect(lake.data_meta_path) as db:
        assert db.execute("select * from visibility_segments").fetchall() == first
    values = lake.raw.read("raw", source="custom", view="versions").collect()
    assert values.height == 4
    assert values.filter(~pl.col("_baseline"))["time"].unique().to_list() == [date(2024, 1, 3)]


def test_explicit_audit_checks_retained_visibility_original(tmp_path):
    lake, spec, frame, before = prepared(tmp_path)
    lake.raw.ingest(spec, frame, ingested_at=datetime(2024, 1, 3, tzinfo=UTC))
    current = lake.inputs.freeze(before.requests, information_cutoff="2024-01-04")
    path = next((lake._paths.dataset_root("custom", "raw") / ".visibility").glob("*.parquet"))
    path.write_bytes(b"damaged first visibility")
    with pytest.raises(RuntimeError, match="visibility.*checksum"):
        lake.inputs.verify(current)
