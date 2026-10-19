"""Selected rows retain their one Original's schema, including sparse payloads."""
from datetime import UTC, date, datetime

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec, RawInput
from bagelquant_data.storage.recovery import _registered, read_batch, read_date_range


@pytest.mark.parametrize("row_group_size", [1, 2, 64])
@pytest.mark.parametrize("empty", [False, True])
def test_same_original_sparse_selected_payload_preserves_exact_schema(tmp_path, row_group_size, empty):
    frame = pl.DataFrame({"ex_date": ["20240601", "20240701", "20240602", "20240702", "20240603"],
        "stk_bo_rate": [None, .2, None, .3, None],
        "stk_co_rate": [None, .1, None, .4, None],
        "div_listdate": [None, "20240703", None, "20240704", None]})
    path = tmp_path / "original.parquet"
    frame.write_parquet(path, row_group_size=row_group_size)
    first, last = (date(2024, 8, 1), date(2024, 8, 31)) if empty else (date(2024, 6, 1), date(2024, 6, 30))
    expected = pl.read_parquet(path).filter(pl.col("ex_date").str.strptime(pl.Date, "%Y%m%d").is_between(first, last))
    actual = read_date_range(path, ("ex_date", first, last), max_buffer_bytes=1024**2)
    assert actual.equals(expected)
    assert actual.schema == frame.schema
    assert actual.schema["stk_bo_rate"] == pl.Float64
    assert actual.schema["div_listdate"] == pl.String


def fixture(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("events", "by_date", date_kind="calendar", primary_key_extra=("ex_date",),
        field_mappings={"time": "time", "asset_id": "asset_id"})
    first = pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "ex_date": ["20240701"],
        "stk_bo_rate": [.2], "stk_co_rate": [.1], "div_listdate": ["20240702"]})
    second = pl.DataFrame({"time": [date(2020, 1, 2)], "asset_id": ["B"], "ex_date": ["20240601"],
        "stk_bo_rate": [None], "stk_co_rate": [None], "div_listdate": [None]})
    lake.raw.ingest(spec, first, mode="initialize", ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    lake.raw.ingest(spec, second, ingested_at=datetime(2020, 1, 3, tzinfo=UTC))
    receipt = lake.inputs.freeze({"x": RawInput("custom", "events", view="versions")}, information_cutoff="2024-07-31")
    return lake, receipt


def test_native_typed_all_null_original_and_public_projection_match_full_versions(tmp_path):
    lake, receipt = fixture(tmp_path)
    batch = list(receipt.evidence["x"]["batches"])[-1]
    full_original = read_batch(lake._data_meta, batch["partition_path"], batch["commit_seq"], batch["content_hash"])
    assert full_original["stk_bo_rate"].null_count() == full_original.height
    assert full_original.schema["stk_bo_rate"] == pl.Float64
    assert full_original.schema["div_listdate"] == pl.String
    selected = read_batch(lake._data_meta, batch["partition_path"], batch["commit_seq"], batch["content_hash"],
        date_range=("ex_date", date(2024, 6, 1), date(2024, 6, 30)))
    assert selected.equals(full_original)
    fields = ("asset_id", "ex_date", "stk_bo_rate", "stk_co_rate", "div_listdate", "time", "_attestation_id")
    expected = lake.inputs.read(receipt, "x", fields=fields).collect().filter(pl.col("ex_date") == "20240601")
    actual = lake.inputs.read(receipt, "x", fields=fields, date_range=("ex_date", "2024-06-01", "2024-06-30")).collect()
    assert actual.equals(expected) and actual.schema == expected.schema


def test_public_single_original_all_null_subset_retains_captured_payload_types(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("events", "by_date", date_kind="calendar", primary_key_extra=("ex_date",),
        field_mappings={"time": "time", "asset_id": "asset_id"})
    frame = pl.DataFrame({"time": [date(2020, 1, 1)] * 3, "asset_id": ["A", "B", "C"],
        "ex_date": ["20240601", "20240701", "20240602"],
        "stk_bo_rate": [None, .2, None], "div_listdate": [None, "20240702", None]})
    lake.raw.ingest(spec, frame, mode="initialize", ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    receipt = lake.inputs.freeze({"x": RawInput("custom", "events", view="versions")}, information_cutoff="2024-07-31")
    expected = lake.inputs.read(receipt, "x").collect().filter(pl.col("ex_date").str.starts_with("202406"))
    actual = lake.inputs.read(receipt, "x", date_range=("ex_date", "2024-06-01", "2024-06-30")).collect()
    assert actual.equals(expected) and actual.schema == expected.schema
    assert actual.schema["stk_bo_rate"] == pl.Float64 and actual.schema["div_listdate"] == pl.String
    empty = lake.inputs.read(receipt, "x", date_range=("ex_date", "2024-08-01", "2024-08-31")).collect()
    assert empty.is_empty() and empty.schema == expected.schema


def test_selected_source_reconciles_added_fields_from_original_schemas(tmp_path):
    lake, original = fixture(tmp_path)
    spec = lake.raw.get("events", source="custom")
    lake.raw.ingest(spec, pl.DataFrame({"time": [date(2020, 1, 3)], "asset_id": ["C"], "ex_date": ["20240602"],
        "stk_bo_rate": [.1], "stk_co_rate": [None], "div_listdate": [None], "added_weight": [2.5]}),
        ingested_at=datetime(2020, 1, 4, tzinfo=UTC))
    current = lake.inputs.freeze(original.requests, information_cutoff="2024-07-31")
    expected = lake.inputs.read(current, "x").collect().filter(pl.col("ex_date").str.starts_with("202406"))
    actual = lake.inputs.read(current, "x", date_range=("ex_date", "2024-06-01", "2024-06-30")).collect()
    assert actual.equals(expected) and actual.schema == expected.schema
    assert actual.schema["added_weight"] == pl.Float64 and actual["added_weight"].to_list() == [None, 2.5]
    assert lake.inputs.get(original).digest == original.digest


def test_pruned_nullable_original_uses_its_own_schema_and_leaves_ordinary_normalization(tmp_path):
    lake, _ = fixture(tmp_path)
    receipt = lake.inputs.freeze({"x": RawInput("custom", "events", start="2020-01-02", end="2020-01-02", view="versions")},
        information_cutoff="2024-07-31")
    assert len(receipt.evidence["x"]["batches"]) == 1
    ordinary = lake.inputs.read(receipt, "x").collect()
    assert ordinary.schema["stk_bo_rate"] == pl.Null
    selected = lake.inputs.read(receipt, "x", date_range=("ex_date", "2024-06-01", "2024-06-30")).collect()
    assert selected.schema["stk_bo_rate"] == pl.Float64
    assert selected.schema["div_listdate"] == pl.String
    assert selected["stk_bo_rate"].to_list() == ordinary["stk_bo_rate"].to_list() == [None]
    assert lake.inputs.read(receipt, "x").collect().schema == ordinary.schema


def test_selected_source_reconciles_compatible_numeric_original_types(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("events", "by_date", date_kind="calendar", primary_key_extra=("ex_date",),
        field_mappings={"time": "time", "asset_id": "asset_id"})
    for day, value in ((1, 1), (2, 2.5)):
        lake.raw.ingest(spec, pl.DataFrame({"time": [date(2020, 1, day)], "asset_id": [str(day)],
            "ex_date": [f"202406{day:02}"], "payload": [value]}),
            mode="initialize" if day == 1 else "incremental", ingested_at=datetime(2020, 1, day, tzinfo=UTC))
    receipt = lake.inputs.freeze({"x": RawInput("custom", "events", view="versions")}, information_cutoff="2024-07-31")
    expected = lake.inputs.read(receipt, "x").collect()
    actual = lake.inputs.read(receipt, "x", date_range=("ex_date", "2024-06-01", "2024-06-30")).collect()
    assert actual.equals(expected) and actual.schema == expected.schema
    assert actual.schema["payload"] == pl.Float64 and actual["payload"].to_list() == [1., 2.5]


@pytest.mark.parametrize("damage", ["rows", "schema"])
def test_selected_original_still_rejects_physical_footer_corruption(tmp_path, damage):
    lake, receipt = fixture(tmp_path)
    batch = list(receipt.evidence["x"]["batches"])[-1]
    _, path = _registered(lake._data_meta, batch["partition_path"], batch["commit_seq"], batch["content_hash"])
    frame = pl.read_parquet(path)
    damaged = pl.concat([frame, frame]) if damage == "rows" else frame.with_columns(pl.col("stk_bo_rate").cast(pl.String))
    damaged.write_parquet(path)
    with pytest.raises(RuntimeError, match="typed schema/row count mismatch"):
        lake.inputs.read(receipt, "x", date_range=("ex_date", "2024-06-01", "2024-06-30"))
