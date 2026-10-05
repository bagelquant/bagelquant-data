from __future__ import annotations

import polars as pl

from bagelquant_data import DataLake, DatasetSpec


def test_admin_registers_plain_dataset_and_reports_status(tmp_path) -> None:
    lake = DataLake.open(data_meta_path=(tmp_path) / "data_meta.sqlite", lake_path=(tmp_path) / "lake")
    spec = lake.raw.register(
        DatasetSpec("daily", "by_date", calendar="trade_cal", field_mappings={"trade_date": "time", "ts_code": "asset_id"})
    )
    lake.raw.ingest(spec, pl.DataFrame({"trade_date": ["20250102"], "ts_code": ["000001.SZ"], "close": [11.37]}))

    assert lake.raw.get("daily", source="custom") == spec
    assert lake.raw.status("daily", source="custom")["row_count"] == 1


def test_standard_normalizer_renames_to_canonical_fields(tmp_path) -> None:
    lake = DataLake.open(data_meta_path=(tmp_path) / "data_meta.sqlite", lake_path=(tmp_path) / "lake")
    lake.raw.ingest(
        DatasetSpec("daily", "by_date", calendar="trade_cal", field_mappings={"trade_date": "time", "ts_code": "asset_id"}),
        pl.DataFrame({"trade_date": ["20250102"], "ts_code": ["000001.SZ"], "close": [11.37]}),
    )

    assert lake.raw.read("daily", source="custom", fields=["time", "asset_id"], view="latest").collect().to_dicts() == [
        {"time": __import__("datetime").date(2025, 1, 2), "asset_id": "000001.SZ"}
    ]
    columns = lake.raw.read("daily", source="custom", view="latest").collect().columns
    assert "trade_date" in columns
    assert "ts_code" in columns
