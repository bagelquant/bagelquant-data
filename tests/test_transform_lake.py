from __future__ import annotations

from datetime import date

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


def test_local_lake_partitioned_projection_and_time_filter(tmp_path) -> None:
    lake = LocalDataLake(tmp_path)
    lake.write(
        "tushare",
        "daily",
        pl.DataFrame(
            {
                "trade_date": ["2024-01-03", "2024-01-04"],
                "ts_code": ["000001.SZ", "000001.SZ"],
                "close": [10.0, 11.0],
                "open": [9.5, 10.5],
            }
        ),
        mode="append",
        partition_column="time",
        partition_granularity="day",
    )

    data = lake.read(
        "tushare",
        "daily",
        columns=("close",),
        start_date="2024-01-04",
        end_date="2024-01-04",
    )

    assert data.columns == ["time", "asset_id", "close"]
    assert data["close"].to_list() == [11.0]


def test_local_lake_system_table_append_writes_one_snapshot_per_append(
    tmp_path,
) -> None:
    lake = LocalDataLake(tmp_path)
    for rows in (1, 2):
        lake.write(
            "tushare",
            "__api_call_log",
            pl.DataFrame({"called_at": [f"2024-01-0{rows}"], "rows": [rows]}),
            mode="append",
        )

    snapshots = lake.snapshots("tushare", "__api_call_log")
    snapshot_rows = [
        pl.read_parquet(ref.path / "data.parquet").height
        for ref in snapshots
        if ref.path is not None
    ]
    data = lake.read("tushare", "__api_call_log")

    assert len(snapshots) == 2
    assert snapshot_rows == [1, 1]
    assert data["rows"].to_list() == [1, 2]


def test_local_lake_reads_legacy_root_snapshots_after_partition_migration(
    tmp_path,
) -> None:
    lake = LocalDataLake(tmp_path)
    lake.write(
        "tushare",
        "__api_call_log",
        pl.DataFrame({"called_at": ["2024-01-01"], "rows": [1]}),
        mode="append",
    )
    lake.write(
        "tushare",
        "__api_call_log",
        pl.DataFrame({"update_date": ["2024-01-02"], "rows": [2]}),
        mode="append",
        partition_column="update_date",
        partition_granularity="day",
    )

    snapshots = lake.snapshots("tushare", "__api_call_log")
    data = lake.read("tushare", "__api_call_log")

    assert len(snapshots) == 2
    assert data["rows"].to_list() == [1, 2]


def test_local_lake_writes_daily_price_partitions(tmp_path) -> None:
    lake = LocalDataLake(tmp_path)

    ref = lake.write(
        "tushare",
        "daily",
        pl.DataFrame(
            {
                "trade_date": ["2024-01-03"],
                "ts_code": ["000001.SZ"],
                "close": [10.0],
            }
        ),
        mode="append",
        partition_column="time",
        partition_granularity="day",
    )

    assert (
        tmp_path
        / "tushare"
        / "daily"
        / "year=2024"
        / "month=01"
        / "day=03"
        / "snapshots"
        / ref.snapshot_id
        / "data.parquet"
    ).exists()
    data = lake.read("tushare", "daily", start_date="2024-01-03")
    assert data.columns == ["time", "asset_id", "close"]
    assert data.to_dicts() == [
        {"time": date(2024, 1, 3), "asset_id": "000001.SZ", "close": 10.0}
    ]


def test_local_lake_partitioned_append_rewrites_only_touched_day(tmp_path) -> None:
    lake = LocalDataLake(tmp_path)
    lake.write(
        "tushare",
        "daily",
        pl.DataFrame(
            {
                "trade_date": ["2024-01-03", "2024-01-04"],
                "ts_code": ["000001.SZ", "000001.SZ"],
                "close": [10.0, 11.0],
            }
        ),
        mode="append",
        partition_column="time",
        partition_granularity="day",
    )
    before = lake.snapshots("tushare", "daily")

    lake.write(
        "tushare",
        "daily",
        pl.DataFrame(
            {
                "trade_date": ["2024-01-03"],
                "ts_code": ["000001.SZ"],
                "close": [12.0],
            }
        ),
        mode="append",
        partition_column="time",
        partition_granularity="day",
    )

    after = lake.snapshots("tushare", "daily")
    assert len(before) == 2
    assert len(after) == 3
    data = lake.read("tushare", "daily")
    assert data.select("time", "asset_id", "close").to_dicts() == [
        {"time": date(2024, 1, 3), "asset_id": "000001.SZ", "close": 12.0},
        {"time": date(2024, 1, 4), "asset_id": "000001.SZ", "close": 11.0},
    ]


def test_local_lake_writes_fundamental_by_announcement_year(tmp_path) -> None:
    lake = LocalDataLake(tmp_path)

    ref = lake.write(
        "tushare",
        "income",
        pl.DataFrame(
            {
                "f_ann_date": ["2024-04-30"],
                "end_date": ["2023-12-31"],
                "ts_code": ["000001.SZ"],
                "revenue": [1.0],
            }
        ),
        mode="append",
        partition_column="time",
        partition_granularity="year",
    )

    assert (
        tmp_path
        / "tushare"
        / "income"
        / "year=2024"
        / "snapshots"
        / ref.snapshot_id
        / "data.parquet"
    ).exists()
    data = lake.read("tushare", "income")
    assert data.columns == ["time", "end_date", "asset_id", "revenue"]
    assert data.to_dicts() == [
        {
            "time": date(2024, 4, 30),
            "end_date": "2023-12-31",
            "asset_id": "000001.SZ",
            "revenue": 1.0,
        }
    ]


def test_local_lake_fundamental_append_deduplicates_with_end_date(tmp_path) -> None:
    lake = LocalDataLake(tmp_path)
    for revenue in (1.0, 2.0):
        lake.write(
            "tushare",
            "income",
            pl.DataFrame(
                {
                    "f_ann_date": ["2024-04-30"],
                    "end_date": ["2023-12-31"],
                    "ts_code": ["000001.SZ"],
                    "revenue": [revenue],
                }
            ),
            mode="append",
            partition_column="time",
            partition_granularity="year",
        )

    data = lake.read("tushare", "income")
    assert data["revenue"].to_list() == [2.0]
