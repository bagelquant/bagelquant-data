from __future__ import annotations

import polars as pl

from bagelquant_data.lake import LocalDataLake


def test_local_lake_writes_and_reads_polars_panel_field(tmp_path) -> None:
    lake = LocalDataLake(tmp_path)
    lake.write(
        "demo",
        "daily",
        pl.DataFrame(
            {
                "trade_date": ["2024-01-01"],
                "ts_code": ["000001.SZ"],
                "close": [10.0],
            }
        ),
        mode="overwrite",
    )

    panel = lake.read_panel_field("demo_daily_close")

    assert panel.columns == ["time", "asset_id", "value"]
    assert panel.to_dicts()[0]["asset_id"] == "000001.SZ"


def test_local_lake_projection_and_time_filter(tmp_path) -> None:
    lake = LocalDataLake(tmp_path)
    lake.write(
        "demo",
        "daily",
        pl.DataFrame(
            {
                "time": ["2024-01-01", "2024-01-02"],
                "asset_id": ["a", "a"],
                "close": [1.0, 2.0],
                "open": [0.5, 1.5],
            }
        ),
        mode="overwrite",
    )

    data = lake.read("demo", "daily", columns=("close",), start_date="2024-01-02")

    assert data.columns == ["time", "asset_id", "close"]
    assert data["close"].to_list() == [2.0]
