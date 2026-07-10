from __future__ import annotations

import polars as pl

from bagelquant_data import DataLake, DatasetSpec


def test_admin_registers_plain_dataset_and_reports_status(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    spec = lake.admin.datasets.register(DatasetSpec("daily", "by_daily", calendar="trade_cal"))
    lake.ingest(spec, pl.DataFrame({"trade_date": ["20250102"], "ts_code": ["000001.SZ"], "close": [11.37]}))

    assert lake.admin.datasets.get("daily", source="custom") == spec
    assert lake.admin.status.dataset("daily", source="custom")["row_count"] == 1


def test_standard_normalizer_adds_canonical_fields(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    lake.ingest(DatasetSpec("daily", "by_daily", calendar="trade_cal"), pl.DataFrame({"trade_date": ["20250102"], "ts_code": ["000001.SZ"], "close": [11.37]}))

    assert lake.query.query("daily", source="custom", fields=["time", "asset_id"]).collect().to_dicts() == [
        {"time": __import__("datetime").date(2025, 1, 2), "asset_id": "000001.SZ"}
    ]
