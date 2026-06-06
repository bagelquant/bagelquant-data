from __future__ import annotations

from typing import cast

import pandas as pd
import pytest

from bagelquant_data.datasource import DataRequest, DataSourceRegistry
from bagelquant_data.lake import (
    DataLakeManager,
    LocalDataLake,
    PartitionSpec,
    TushareTableUpdateSpec,
    TushareTradingCalendarRef,
    TushareUniverseRef,
)
from bagelquant_data.lake.manager import (
    TUSHARE_FUNDAMENTAL_UPDATE_RECORDS,
    TUSHARE_PRICE_UPDATE_RECORDS,
)
from bagelquant_data.loader import Loader
from bagelquant_data.transform import Transform


def test_transform_pipeline_is_stateless() -> None:
    frame = pd.DataFrame({"a": [1.0]}, index=pd.Index(["x"]))
    result = Transform().align(columns=pd.Index(["a", "b"])).run(frame)

    assert result.columns.tolist() == ["a", "b"]
    assert frame.columns.tolist() == ["a"]


def test_partition_spec_builds_hive_style_path() -> None:
    spec = PartitionSpec(keys=("region", "date"))

    assert spec.path({"region": "CN", "date": "2024-01-01"}) == (
        "region=CN/date=2024-01-01"
    )


def test_local_lake_separates_datasets_by_source(tmp_path) -> None:
    lake = LocalDataLake(tmp_path)

    lake.add("tushare", "daily", pd.DataFrame({"value": [1]}))
    lake.add("local", "daily", pd.DataFrame({"value": [2]}))

    assert lake.list_sources() == ("local", "tushare")
    assert lake.list_datasets() == (("local", "daily"), ("tushare", "daily"))
    assert lake.read("tushare", "daily")["value"].tolist() == [1]
    assert lake.read("local", "daily")["value"].tolist() == [2]


def test_local_lake_partitions_table_by_year_and_month(tmp_path) -> None:
    lake = LocalDataLake(tmp_path)

    ref = lake.add(
        "tushare",
        "daily",
        pd.DataFrame(
            {
                "trade_date": ["20240131", "20240201"],
                "value": [1, 2],
            }
        ),
    )

    assert ref.year == 2024
    assert lake.read("tushare", "daily").index.name == "date"
    assert (tmp_path / "tushare" / "daily" / "year=2024" / "month=01").exists()
    assert (tmp_path / "tushare" / "daily" / "year=2024" / "month=02").exists()
    assert lake.read("tushare", "daily", year=2024, month=1)["value"].tolist() == [1]
    assert lake.read("tushare", "daily", year=2024, month=2)["value"].tolist() == [2]
    assert (
        tmp_path
        / "tushare"
        / "daily"
        / "year=2024"
        / "month=01"
        / "snapshots"
        / ref.snapshot_id
        / "data.parquet"
    ).exists()


def test_local_lake_reads_latest_snapshot_per_partition(tmp_path) -> None:
    lake = LocalDataLake(tmp_path)
    lake.add(
        "tushare",
        "daily",
        pd.DataFrame({"trade_date": ["20240131"], "value": [1]}),
    )
    lake.write(
        "tushare",
        "daily",
        pd.DataFrame({"trade_date": ["20240201"], "value": [2]}),
        mode="append",
        partition_column="trade_date",
    )

    assert lake.read("tushare", "daily")["value"].tolist() == [1, 2]


def test_lake_panel_like_date_index_is_sorted(tmp_path) -> None:
    lake = LocalDataLake(tmp_path)

    lake.add(
        "tushare",
        "daily",
        pd.DataFrame(
            {
                "trade_date": ["20240103", "20240102"],
                "value": [2, 1],
            }
        ),
    )

    assert lake.read("tushare", "daily").index.tolist() == [
        pd.Timestamp("2024-01-02"),
        pd.Timestamp("2024-01-03"),
    ]


def test_lake_tables_have_lifecycle_columns_and_main_id_tables(tmp_path) -> None:
    lake = LocalDataLake(tmp_path)

    lake.add(
        "tushare",
        "daily",
        pd.DataFrame(
            {
                "ts_code": ["000300.SH"],
                "trade_date": ["20240131"],
                "close": [1.0],
            }
        ),
    )
    daily = lake.read("tushare", "daily")

    assert daily.index.name == "date"
    assert {"create_time", "delete_flag"}.issubset(daily.columns)
    assert lake.asset_ids("tushare") == ("tushare_000300.SH",)
    assert "tushare_daily_close" in lake.field_ids("tushare")
    fields = lake.fields("tushare")
    close = fields.loc[fields["field_id"] == "tushare_daily_close"].iloc[0]
    assert close["source"] == "tushare"
    assert close["table"] == "daily"
    assert close["field"] == "close"
    assert "tushare_daily_close" in lake.data_item_ids("tushare")
    stored_fields = lake.read("tushare", "__fields")
    assert "field_id" in stored_fields.columns


def test_lake_data_items_support_old_one_column_catalog(tmp_path) -> None:
    lake = LocalDataLake(tmp_path)
    lake.add(
        "tushare",
        "daily",
        pd.DataFrame(
            {
                "ts_code": ["000300.SH"],
                "trade_date": ["20240131"],
                "close": [1.0],
            }
        ),
    )
    lake.write(
        "tushare",
        "__data_item_ids",
        pd.DataFrame({"data_item_id": ["tushare_daily_close"]}),
        mode="overwrite",
        update_catalogs=False,
    )

    fields = lake.fields("tushare")
    data_items = lake.data_items("tushare")

    assert "tushare_daily_close" in fields["field_id"].tolist()
    assert "tushare_daily_close" in data_items["data_item_id"].tolist()
    assert {"source", "table", "field", "field_id"}.issubset(fields.columns)


def test_panel_field_ids_uses_catalog_metadata_without_reading_table(tmp_path) -> None:
    lake = CountingLocalDataLake(tmp_path)
    lake.add(
        "tushare",
        "daily",
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": ["20240102"],
                "close": [10.0],
            }
        ),
    )
    lake.read_calls.clear()

    assert lake.panel_field_ids() == ("tushare_daily_close",)
    assert ("tushare", "daily") not in lake.read_calls


def test_lake_reads_qualified_panel_field(tmp_path) -> None:
    lake = LocalDataLake(tmp_path)
    lake.add(
        "tushare",
        "daily",
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ", "600000.SH", "000001.SZ"],
                "trade_date": ["20240102", "20240102", "20240103"],
                "close": [10.0, 20.0, 11.0],
            }
        ),
    )

    panel = lake.read_panel_field(
        "tushare_daily_close",
        start_date="2024-01-02",
        end_date="2024-01-03",
    )

    assert lake.panel_field_ids() == ("tushare_daily_close",)
    assert panel.index.name == "date"
    assert panel.columns.tolist() == ["000001.SZ", "600000.SH"]
    assert panel.loc[pd.Timestamp("2024-01-02"), "600000.SH"] == 20.0


def test_local_lake_read_projects_columns_and_preserves_date_index(tmp_path) -> None:
    lake = LocalDataLake(tmp_path)
    lake.add(
        "tushare",
        "daily",
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": ["20240102"],
                "close": [10.0],
                "open": [9.0],
            }
        ),
    )

    projected = lake.read("tushare", "daily", columns=["close"])

    assert projected.index.name == "date"
    assert projected.columns.tolist() == ["close"]
    assert projected["close"].tolist() == [10.0]


def test_local_lake_read_prunes_partitions_by_date_range(tmp_path) -> None:
    lake = LocalDataLake(tmp_path)
    lake.write(
        "tushare",
        "daily",
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": ["20240131"],
                "close": [10.0],
            }
        ),
        partition_column="trade_date",
    )
    lake.write(
        "tushare",
        "daily",
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": ["20240201"],
                "close": [11.0],
            }
        ),
        partition_column="trade_date",
    )
    january = next(
        tmp_path.glob("tushare/daily/year=2024/month=01/snapshots/*/data.parquet")
    )
    january.write_text("corrupt if read", encoding="utf-8")

    data = lake.read("tushare", "daily", start_date="2024-02-01")

    assert data["close"].tolist() == [11.0]


def test_loader_lake_read_applies_field_projection(tmp_path) -> None:
    lake = LocalDataLake(tmp_path)
    registry = DataSourceRegistry()
    registry.register(CountingSource())
    lake.add(
        "fake",
        "prices",
        pd.DataFrame(
            {
                "trade_date": ["20240102"],
                "value": [1.0],
                "other": [2.0],
            }
        ),
    )

    loaded = Loader(registry=registry, lake=lake).source("fake").load(
        "prices",
        fields=("value",),
    )

    assert loaded.metadata["origin"] == "lake"
    assert loaded.data.columns.tolist() == ["value"]


def test_read_panel_field_projects_to_required_columns(tmp_path) -> None:
    lake = CountingLocalDataLake(tmp_path)
    lake.add(
        "tushare",
        "daily",
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": ["20240102"],
                "close": [10.0],
                "open": [9.0],
            }
        ),
    )
    lake.read_kwargs.clear()

    panel = lake.read_panel_field("tushare_daily_close", start_date="2024-01-02")

    assert panel.columns.tolist() == ["000001.SZ"]
    assert lake.read_kwargs[-1]["columns"] == [
        "close",
        "ts_code",
        "date",
        "trade_date",
        "f_ann_date",
    ]


def test_stock_basic_is_row_table_not_date_indexed(tmp_path) -> None:
    lake = LocalDataLake(tmp_path)

    lake.add(
        "tushare",
        "stock_basic",
        pd.DataFrame({"ts_code": ["000001.SZ"], "name": ["Ping An"]}),
    )
    stock_basic = lake.read("tushare", "stock_basic")

    assert stock_basic.index.name is None
    assert stock_basic["ts_code"].tolist() == ["000001.SZ"]
    assert {"create_time", "delete_flag"}.issubset(stock_basic.columns)


def test_stock_basic_updates_source_asset_catalog(tmp_path) -> None:
    lake = LocalDataLake(tmp_path)
    lake.add(
        "tushare",
        "stock_basic",
        pd.DataFrame({"ts_code": ["000001.SZ", "600000.SH"]}),
    )

    assert lake.asset_ids("tushare") == ("tushare_000001.SZ", "tushare_600000.SH")


def test_lake_manager_add_edit_delete(tmp_path) -> None:
    manager = DataLakeManager(LocalDataLake(tmp_path))
    manager.add("tushare", "daily", pd.DataFrame({"value": [1]}))
    manager.edit("tushare", "daily", pd.DataFrame({"value": [3]}))

    assert manager.list_datasets("tushare") == (("tushare", "daily"),)
    assert manager.lake.read("tushare", "daily")["value"].tolist() == [3]

    manager.delete("tushare", "daily")

    assert manager.list_datasets("tushare") == ()


def test_loader_prefers_lake_and_refresh_updates_from_provider(tmp_path) -> None:
    registry = DataSourceRegistry()
    source = CountingSource()
    registry.register(source)
    lake = LocalDataLake(tmp_path)
    lake.add("fake", "prices", pd.DataFrame({"value": [10]}))
    loader = Loader(registry=registry, lake=lake).source("fake")

    cached = loader.load("prices")
    refreshed = loader.load("prices", refresh=True)
    cached_again = loader.load("prices")

    assert cached.metadata["origin"] == "lake"
    assert cached.data["value"].tolist() == [10]
    assert refreshed.metadata["origin"] == "provider"
    assert refreshed.data["value"].tolist() == [1]
    assert cached_again.data["value"].tolist() == [1]
    assert source.calls == 1


def test_manager_update_reads_provider_once(tmp_path) -> None:
    registry = DataSourceRegistry()
    source = CountingSource()
    registry.register(source)
    manager = DataLakeManager(LocalDataLake(tmp_path), registry=registry)

    snapshot = manager.update("fake", DataRequest(dataset="prices"))

    assert snapshot.dataset == "prices"
    assert manager.lake.read("fake", "prices")["value"].tolist() == [1]
    assert source.calls == 1


def test_tushare_all_price_update_reads_day_by_day(tmp_path) -> None:
    registry = DataSourceRegistry()
    source = FakeTushareUpdateSource()
    registry.register(source)
    manager = DataLakeManager(LocalDataLake(tmp_path), registry=registry)

    manager.update_tushare_all(
        "daily",
        start_date="2024-01-01",
        end_date="2024-01-02",
        workers=2,
    )
    daily = manager.lake.read("tushare", "daily")

    assert source.calls["stock_basic"] == []
    assert sorted(source.calls["daily"], key=lambda item: str(item["trade_date"])) == [
        {"trade_date": "20240101"},
        {"trade_date": "20240102"},
    ]
    assert daily.index.name == "date"
    assert daily["ts_code"].tolist() == ["000001.SZ", "000001.SZ"]


def test_tushare_price_update_skips_existing_dates_and_reports_progress(
    tmp_path,
) -> None:
    registry = DataSourceRegistry()
    source = FakeTushareUpdateSource()
    registry.register(source)
    manager = DataLakeManager(LocalDataLake(tmp_path), registry=registry)
    manager.lake.add(
        "tushare",
        "daily",
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": ["20240101"],
                "close": [1.0],
            }
        ),
    )
    events = []

    manager.update_tushare_all(
        "daily",
        start_date="2024-01-01",
        end_date="2024-01-02",
        workers=2,
        progress=events.append,
    )

    assert source.calls["daily"] == [{"trade_date": "20240102"}]
    price_events = [
        event
        for event in events
        if event["table"] == "daily" and event.get("status") == "succeeded"
    ]
    assert price_events == [
        {
            "table": "daily",
            "kind": "price",
            "item": "2024-01-02",
            "completed": price_events[0]["completed"],
            "total": 1,
            "rows_written": 1,
            "snapshot": price_events[0]["snapshot"],
            "status": "succeeded",
            "filters": {"trade_date": "20240102"},
        }
    ]
    assert any(event.get("status") == "started" for event in events)
    assert len(manager.lake.snapshots("tushare", "daily")) == 2


def test_tushare_price_update_skips_existing_date_without_snapshot(tmp_path) -> None:
    registry = DataSourceRegistry()
    source = FakeTushareUpdateSource()
    registry.register(source)
    manager = DataLakeManager(LocalDataLake(tmp_path), registry=registry)
    manager.lake.add(
        "tushare",
        "daily",
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": ["20240101"],
                "close": [1.0],
            }
        ),
    )
    snapshots = manager.lake.snapshots("tushare", "daily")

    refs = manager.update_tushare_all(
        "daily",
        start_date="2024-01-01",
        end_date="2024-01-01",
        workers=2,
    )

    assert refs == ()
    assert source.calls["daily"] == []
    assert manager.lake.snapshots("tushare", "daily") == snapshots


def test_tushare_price_scan_uses_existing_update_records(tmp_path) -> None:
    registry = DataSourceRegistry()
    registry.register(FakeTushareUpdateSource())
    lake = CountingLocalDataLake(tmp_path)
    manager = DataLakeManager(lake, registry=registry)
    lake.add(
        "tushare",
        "trade_cal",
        pd.DataFrame({"cal_date": ["20240101", "20240102"], "is_open": [1, 1]}),
    )
    lake.write(
        "tushare",
        TUSHARE_PRICE_UPDATE_RECORDS,
        pd.DataFrame(
            {
                "source": ["tushare", "tushare"],
                "table": ["daily", "daily"],
                "calendar": ["trade_cal", "trade_cal"],
                "trade_date": ["20240101", "20240102"],
                "exists": [True, False],
                "last_snapshot_id": ["snapshot", ""],
                "last_updated_at": ["2024-01-01T00:00:00+00:00", ""],
            }
        ),
        mode="overwrite",
        update_catalogs=False,
    )
    lake.read_calls.clear()

    report = manager.scan_tushare_updates(
        ["daily"],
        kinds={"daily": "price"},
        start_date="2024-01-01",
        end_date="2024-01-02",
        trading_calendars={
            "daily": TushareTradingCalendarRef(name="trade_cal", table="trade_cal")
        },
    )

    assert report.plans[0].pending_items == ("2024-01-02",)
    assert ("tushare", "daily") not in lake.read_calls


def test_tushare_price_update_marks_record_complete(tmp_path) -> None:
    registry = DataSourceRegistry()
    source = FakeTushareUpdateSource()
    registry.register(source)
    manager = DataLakeManager(LocalDataLake(tmp_path), registry=registry)
    manager.lake.add(
        "tushare",
        "trade_cal",
        pd.DataFrame({"cal_date": ["20240101"], "is_open": [1]}),
    )

    manager.update_tushare_all(
        "daily",
        kind="price",
        start_date="2024-01-01",
        end_date="2024-01-01",
        workers=1,
        trading_calendar=TushareTradingCalendarRef(
            name="trade_cal",
            table="trade_cal",
        ),
    )

    records = manager.lake.read("tushare", TUSHARE_PRICE_UPDATE_RECORDS)
    row = records.loc[records["trade_date"].astype(str) == "20240101"].iloc[0]
    assert bool(row["exists"])
    assert str(row["last_snapshot_id"])


def test_scan_data_lake_creates_price_records_for_configured_empty_table(
    tmp_path,
) -> None:
    registry = DataSourceRegistry()
    registry.register(FakeTushareUpdateSource())
    manager = DataLakeManager(LocalDataLake(tmp_path), registry=registry)
    manager.lake.add(
        "tushare",
        "trade_cal",
        pd.DataFrame({"cal_date": ["20240101", "20240102"], "is_open": [1, 1]}),
    )

    manager.scan_data_lake(
        specs=(
            TushareTableUpdateSpec(
                table="daily",
                kind="price",
                trading_calendar=TushareTradingCalendarRef(
                    name="trade_cal",
                    table="trade_cal",
                ),
            ),
        ),
        start_date="2024-01-01",
        end_date="2024-01-02",
    )

    records = manager.lake.read("tushare", TUSHARE_PRICE_UPDATE_RECORDS)
    assert records["trade_date"].tolist() == ["20240101", "20240102"]
    assert records["exists"].tolist() == [False, False]


def test_scan_data_lake_rebuilds_price_records_from_existing_table(tmp_path) -> None:
    registry = DataSourceRegistry()
    registry.register(FakeTushareUpdateSource())
    manager = DataLakeManager(LocalDataLake(tmp_path), registry=registry)
    manager.lake.add(
        "tushare",
        "trade_cal",
        pd.DataFrame({"cal_date": ["20240101", "20240102"], "is_open": [1, 1]}),
    )
    manager.lake.add(
        "tushare",
        "daily",
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": ["20240101"],
                "close": [1.0],
            }
        ),
    )

    manager.scan_data_lake(
        specs=(
            TushareTableUpdateSpec(
                table="daily",
                kind="price",
                trading_calendar=TushareTradingCalendarRef(
                    name="trade_cal",
                    table="trade_cal",
                ),
            ),
        ),
        start_date="2024-01-01",
        end_date="2024-01-02",
    )

    records = manager.lake.read("tushare", TUSHARE_PRICE_UPDATE_RECORDS)
    by_date = {
        str(row.trade_date): bool(row.exists)
        for row in records.itertuples(index=False)
    }
    assert by_date == {"20240101": True, "20240102": False}


def test_tushare_price_update_writes_new_dates_as_day_partitions(tmp_path) -> None:
    registry = DataSourceRegistry()
    source = FakeTushareUpdateSource()
    registry.register(source)
    manager = DataLakeManager(LocalDataLake(tmp_path), registry=registry)

    manager.update_tushare_all(
        "daily",
        start_date="2024-01-01",
        end_date="2024-01-02",
        workers=2,
    )

    daily_path = tmp_path / "tushare" / "daily" / "year=2024" / "month=01"
    assert daily_path.exists()
    assert (daily_path / "day=01").exists()
    assert (daily_path / "day=02").exists()
    assert manager.lake.read("tushare", "daily")["trade_date"].tolist() == [
        "20240101",
        "20240102",
    ]


def test_tushare_update_scan_reports_missing_price_dates_without_provider_calls(
    tmp_path,
) -> None:
    registry = DataSourceRegistry()
    source = FakeTushareUpdateSource()
    registry.register(source)
    manager = DataLakeManager(LocalDataLake(tmp_path), registry=registry)
    manager.lake.add(
        "tushare",
        "daily",
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": ["20240101"],
                "close": [1.0],
            }
        ),
    )

    report = manager.scan_tushare_updates(
        ["daily"],
        kinds={"daily": "price"},
        start_date="2024-01-01",
        end_date="2024-01-03",
    )

    assert source.calls["daily"] == []
    assert report.plans[0].status == "pending"
    assert report.plans[0].effective_start == pd.Timestamp("2024-01-02").date()
    assert report.plans[0].pending_items == ("2024-01-02", "2024-01-03")
    assert [job.filters for job in report.jobs] == [
        {"trade_date": "20240102"},
        {"trade_date": "20240103"},
    ]


def test_tushare_trading_calendar_update_writes_calendar_table(tmp_path) -> None:
    registry = DataSourceRegistry()
    source = FakeTushareUpdateSource()
    registry.register(source)
    manager = DataLakeManager(LocalDataLake(tmp_path), registry=registry)

    ref = manager.update_tushare_trading_calendar(
        start_date="2024-01-01",
        end_date="2024-01-03",
    )

    trade_cal = manager.lake.read("tushare", "trade_cal")
    assert ref.dataset == "trade_cal"
    assert source.calls["trade_cal"] == [
        {"end_date": "20240103", "start_date": "20240101"}
    ]
    assert trade_cal["cal_date"].tolist() == ["20240101", "20240102", "20240103"]


def test_tushare_price_scan_uses_open_trading_calendar_dates(tmp_path) -> None:
    registry = DataSourceRegistry()
    source = FakeTushareUpdateSource()
    registry.register(source)
    manager = DataLakeManager(LocalDataLake(tmp_path), registry=registry)
    manager.lake.add(
        "tushare",
        "trade_cal",
        pd.DataFrame(
            {
                "cal_date": ["20240101", "20240102", "20240103"],
                "is_open": [1, 0, 1],
            }
        ),
    )

    report = manager.scan_tushare_updates(
        specs=(
            TushareTableUpdateSpec(
                table="daily",
                kind="price",
                trading_calendar=TushareTradingCalendarRef(
                    name="trade_cal",
                    table="trade_cal",
                    date_column="cal_date",
                    open_column="is_open",
                ),
            ),
        ),
        start_date="2024-01-01",
        end_date="2024-01-03",
    )

    assert report.plans[0].pending_items == ("2024-01-01", "2024-01-03")
    assert [job.filters for job in report.jobs] == [
        {"trade_date": "20240101"},
        {"trade_date": "20240103"},
    ]


def test_tushare_update_spec_scan_matches_legacy_map_scan(tmp_path) -> None:
    registry = DataSourceRegistry()
    source = FakeTushareUpdateSource()
    registry.register(source)
    manager = DataLakeManager(LocalDataLake(tmp_path), registry=registry)
    manager.lake.add(
        "tushare",
        "trade_cal",
        pd.DataFrame({"cal_date": ["20240101"], "is_open": [1]}),
    )

    legacy = manager.scan_tushare_updates(
        ["daily"],
        kinds={"daily": "price"},
        trading_calendars={
            "daily": TushareTradingCalendarRef(name="trade_cal", table="trade_cal")
        },
        start_date="2024-01-01",
        end_date="2024-01-01",
    )
    modern = manager.scan_tushare_updates(
        specs=(
            TushareTableUpdateSpec(
                table="daily",
                kind="price",
                trading_calendar=TushareTradingCalendarRef(
                    name="trade_cal",
                    table="trade_cal",
                ),
            ),
        ),
        start_date="2024-01-01",
        end_date="2024-01-01",
    )

    assert modern.jobs == legacy.jobs
    assert modern.plans == legacy.plans


def test_tushare_stock_basic_reads_all_statuses_and_deduplicates(tmp_path) -> None:
    registry = DataSourceRegistry()
    source = FakeTushareUpdateSource()
    registry.register(source)
    manager = DataLakeManager(LocalDataLake(tmp_path), registry=registry)

    manager.update_tushare_stock_basic()

    stock_basic = manager.lake.read("tushare", "stock_basic")
    assert source.calls["stock_basic"] == [
        {"list_status": "L"},
        {"list_status": "D"},
        {"list_status": "P"},
    ]
    assert stock_basic["ts_code"].tolist() == ["000001.SZ", "300001.SZ", "600000.SH"]
    assert manager.lake.asset_ids("tushare") == (
        "tushare_000001.SZ",
        "tushare_300001.SZ",
        "tushare_600000.SH",
    )


def test_tushare_fundamental_update_reads_incremental_asset_jobs(tmp_path) -> None:
    registry = DataSourceRegistry()
    source = FakeTushareUpdateSource()
    registry.register(source)
    manager = DataLakeManager(LocalDataLake(tmp_path), registry=registry)
    manager.lake.add(
        "tushare",
        "stock_basic",
        pd.DataFrame({"ts_code": ["000001.SZ", "600000.SH"]}),
    )
    manager.lake.add(
        "tushare",
        "income",
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "f_ann_date": ["20240102"],
                "revenue": [1.0],
            }
        ),
    )

    manager.update_tushare_all(
        "income",
        kind="fundamental",
        start_date="2024-01-01",
        end_date="2024-01-04",
        workers=2,
    )

    calls = sorted(source.calls["income"], key=lambda item: str(item["ts_code"]))
    assert calls == [
        {"end_date": "20240104", "start_date": "20240102", "ts_code": "000001.SZ"},
        {"end_date": "20240104", "start_date": "20240101", "ts_code": "600000.SH"},
    ]
    income = manager.lake.read("tushare", "income")
    assert income["f_ann_date"].tolist() == [
        "20240101",
        "20240102",
        "20240102",
        "20240103",
    ]


def test_tushare_fundamental_records_cover_all_stock_statuses(tmp_path) -> None:
    registry = DataSourceRegistry()
    source = FakeTushareUpdateSource()
    registry.register(source)
    manager = DataLakeManager(LocalDataLake(tmp_path), registry=registry)

    manager.update_tushare_stock_basic()
    manager.scan_tushare_updates(
        ["income"],
        kinds={"income": "fundamental"},
        start_date="2024-01-01",
        end_date="2024-01-02",
    )

    records = manager.lake.read("tushare", TUSHARE_FUNDAMENTAL_UPDATE_RECORDS)
    assert records["asset_id"].tolist() == [
        "000001.SZ",
        "300001.SZ",
        "600000.SH",
    ]
    assert source.calls["stock_basic"] == [
        {"list_status": "L"},
        {"list_status": "D"},
        {"list_status": "P"},
    ]


def test_tushare_fundamental_scan_uses_record_latest_date(tmp_path) -> None:
    registry = DataSourceRegistry()
    source = FakeTushareUpdateSource()
    registry.register(source)
    lake = CountingLocalDataLake(tmp_path)
    manager = DataLakeManager(lake, registry=registry)
    lake.add("tushare", "stock_basic", pd.DataFrame({"ts_code": ["000001.SZ"]}))
    lake.write(
        "tushare",
        TUSHARE_FUNDAMENTAL_UPDATE_RECORDS,
        pd.DataFrame(
            {
                "source": ["tushare"],
                "table": ["income"],
                "universe": ["stock_basic"],
                "asset_id": ["000001.SZ"],
                "latest_date": ["20240102"],
                "exists": [True],
                "last_snapshot_id": ["snapshot"],
                "last_updated_at": ["2024-01-02T00:00:00+00:00"],
            }
        ),
        mode="overwrite",
        update_catalogs=False,
    )
    lake.read_calls.clear()

    report = manager.scan_tushare_updates(
        ["income"],
        kinds={"income": "fundamental"},
        start_date="2024-01-01",
        end_date="2024-01-04",
    )

    assert report.jobs[0].start_date == pd.Timestamp("2024-01-02").date()
    assert ("tushare", "income") not in lake.read_calls


def test_tushare_fundamental_update_advances_asset_record(tmp_path) -> None:
    registry = DataSourceRegistry()
    source = FakeTushareUpdateSource()
    registry.register(source)
    manager = DataLakeManager(LocalDataLake(tmp_path), registry=registry)
    manager.lake.add("tushare", "stock_basic", pd.DataFrame({"ts_code": ["000001.SZ"]}))

    manager.update_tushare_all(
        "income",
        kind="fundamental",
        start_date="2024-01-01",
        end_date="2024-01-04",
        workers=1,
    )

    records = manager.lake.read("tushare", TUSHARE_FUNDAMENTAL_UPDATE_RECORDS)
    row = records.loc[records["asset_id"].astype(str) == "000001.SZ"].iloc[0]
    assert str(row["latest_date"]) == "20240102"
    assert bool(row["exists"])


def test_scan_data_lake_creates_fundamental_records_for_configured_empty_table(
    tmp_path,
) -> None:
    registry = DataSourceRegistry()
    registry.register(FakeTushareUpdateSource())
    manager = DataLakeManager(LocalDataLake(tmp_path), registry=registry)
    manager.lake.add(
        "tushare",
        "stock_basic",
        pd.DataFrame({"ts_code": ["000001.SZ", "600000.SH"]}),
    )

    manager.scan_data_lake(
        specs=(TushareTableUpdateSpec(table="income", kind="fundamental"),),
        start_date="2024-01-01",
        end_date="2024-01-02",
    )

    records = manager.lake.read("tushare", TUSHARE_FUNDAMENTAL_UPDATE_RECORDS)
    assert records["asset_id"].tolist() == ["000001.SZ", "600000.SH"]
    assert records["exists"].tolist() == [False, False]
    assert records["latest_date"].tolist() == ["", ""]


def test_scan_data_lake_rebuilds_fundamental_records_from_existing_table(
    tmp_path,
) -> None:
    registry = DataSourceRegistry()
    registry.register(FakeTushareUpdateSource())
    manager = DataLakeManager(LocalDataLake(tmp_path), registry=registry)
    manager.lake.add(
        "tushare",
        "stock_basic",
        pd.DataFrame({"ts_code": ["000001.SZ", "600000.SH"]}),
    )
    manager.lake.add(
        "tushare",
        "income",
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "f_ann_date": ["20240102"],
                "revenue": [1.0],
            }
        ),
    )

    manager.scan_data_lake(
        specs=(TushareTableUpdateSpec(table="income", kind="fundamental"),),
        start_date="2024-01-01",
        end_date="2024-01-04",
    )

    records = manager.lake.read("tushare", TUSHARE_FUNDAMENTAL_UPDATE_RECORDS)
    by_asset = {
        str(row.asset_id): str(row.latest_date)
        for row in records.itertuples(index=False)
    }
    assert by_asset == {"000001.SZ": "20240102", "600000.SH": ""}


def test_tushare_execute_preloads_existing_fundamental_table_once(tmp_path) -> None:
    source = FakeTushareUpdateSource()
    registry = DataSourceRegistry()
    registry.register(source)
    lake = CountingLocalDataLake(tmp_path)
    manager = DataLakeManager(lake, registry=registry)
    manager.lake.add(
        "tushare",
        "stock_basic",
        pd.DataFrame({"ts_code": ["000001.SZ", "600000.SH"]}),
    )
    manager.lake.add(
        "tushare",
        "income",
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "f_ann_date": ["20240101"],
                "revenue": [1.0],
            }
        ),
    )
    report = manager.scan_tushare_updates(
        specs=(TushareTableUpdateSpec(table="income", kind="fundamental"),),
        start_date="2024-01-01",
        end_date="2024-01-03",
    )
    lake.read_calls.clear()

    manager.execute_tushare_update_report(report, workers=1)

    assert lake.read_calls.count(("tushare", "income")) == 1


def test_tushare_update_rejects_zero_workers(tmp_path) -> None:
    registry = DataSourceRegistry()
    registry.register(FakeTushareUpdateSource())
    manager = DataLakeManager(LocalDataLake(tmp_path), registry=registry)

    with pytest.raises(ValueError, match="workers"):
        manager.execute_tushare_update_report(
            manager.scan_tushare_updates(
                specs=(TushareTableUpdateSpec(table="stock_basic", kind="general"),),
            ),
            workers=0,
        )


def test_tushare_update_scan_reports_fundamental_effective_start(tmp_path) -> None:
    registry = DataSourceRegistry()
    source = FakeTushareUpdateSource()
    registry.register(source)
    manager = DataLakeManager(LocalDataLake(tmp_path), registry=registry)
    manager.lake.add(
        "tushare",
        "income",
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "f_ann_date": ["20240102"],
                "revenue": [1.0],
            }
        ),
    )
    manager.lake.add(
        "tushare",
        "stock_basic",
        pd.DataFrame({"ts_code": ["000001.SZ", "600000.SH"]}),
    )

    report = manager.scan_tushare_updates(
        ["income"],
        kinds={"income": "fundamental"},
        start_date="2024-01-01",
        end_date="2024-01-04",
    )

    assert source.calls["income"] == []
    assert report.plans[0].effective_start == pd.Timestamp("2024-01-01").date()
    assert report.plans[0].pending_items == ("000001.SZ", "600000.SH")
    jobs_by_asset = {str(job.filters["ts_code"]): job for job in report.jobs}
    assert jobs_by_asset["000001.SZ"].start_date == pd.Timestamp("2024-01-02").date()
    assert jobs_by_asset["600000.SH"].start_date == pd.Timestamp("2024-01-01").date()
    assert all(
        job.end_date == pd.Timestamp("2024-01-04").date()
        for job in report.jobs
    )


def test_tushare_update_scan_reports_fundamental_jobs_without_existing_table(
    tmp_path,
) -> None:
    registry = DataSourceRegistry()
    source = FakeTushareUpdateSource()
    registry.register(source)
    manager = DataLakeManager(LocalDataLake(tmp_path), registry=registry)
    manager.lake.add(
        "tushare",
        "stock_basic",
        pd.DataFrame({"ts_code": ["000001.SZ", "600000.SH"]}),
    )

    report = manager.scan_tushare_updates(
        ["income"],
        kinds={"income": "fundamental"},
        start_date="2024-01-01",
        end_date="2024-01-04",
    )

    assert source.calls["income"] == []
    assert report.plans[0].effective_start == pd.Timestamp("2024-01-01").date()
    assert report.plans[0].estimated_job_count == 2
    assert report.plans[0].pending_items == ("000001.SZ", "600000.SH")
    assert [job.filters for job in report.jobs] == [
        {"ts_code": "000001.SZ"},
        {"ts_code": "600000.SH"},
    ]
    assert all(
        job.start_date == pd.Timestamp("2024-01-01").date()
        for job in report.jobs
    )


def test_tushare_fundamental_scan_uses_configured_universe_table(tmp_path) -> None:
    registry = DataSourceRegistry()
    source = FakeTushareUpdateSource()
    registry.register(source)
    manager = DataLakeManager(LocalDataLake(tmp_path), registry=registry)
    manager.lake.add(
        "tushare",
        "index_basic",
        pd.DataFrame({"ts_code": ["000300.SH", "000905.SH"]}),
    )

    report = manager.scan_tushare_updates(
        ["index_member"],
        kinds={"index_member": "fundamental"},
        start_date="2024-01-01",
        end_date="2024-01-04",
        universes={
            "index_member": TushareUniverseRef(
                name="index_basic",
                table="index_basic",
                code_column="ts_code",
            )
        },
    )

    assert report.plans[0].pending_items == ("000300.SH", "000905.SH")
    assert [job.filters for job in report.jobs] == [
        {"ts_code": "000300.SH"},
        {"ts_code": "000905.SH"},
    ]


def test_tushare_update_scan_uses_asset_catalog_for_new_fundamental_table(
    tmp_path,
) -> None:
    registry = DataSourceRegistry()
    source = FakeTushareUpdateSource()
    registry.register(source)
    manager = DataLakeManager(LocalDataLake(tmp_path), registry=registry)
    manager.lake.add(
        "tushare",
        "daily",
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ", "600000.SH"],
                "trade_date": ["20240102", "20240102"],
                "close": [1.0, 2.0],
            }
        ),
    )

    report = manager.scan_tushare_updates(
        ["income"],
        kinds={"income": "fundamental"},
        start_date="2024-01-01",
        end_date="2024-01-04",
    )

    assert report.plans[0].estimated_job_count == 2
    assert report.plans[0].pending_items == ("000001.SZ", "600000.SH")
    assert [job.filters for job in report.jobs] == [
        {"ts_code": "000001.SZ"},
        {"ts_code": "600000.SH"},
    ]


def test_tushare_fundamental_update_filters_boundary_duplicates(tmp_path) -> None:
    registry = DataSourceRegistry()
    source = FakeTushareUpdateSource()
    registry.register(source)
    manager = DataLakeManager(LocalDataLake(tmp_path), registry=registry)
    manager.lake.add(
        "tushare",
        "income",
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "f_ann_date": ["20240102"],
                "revenue": [1.0],
            }
        ),
    )
    manager.lake.add(
        "tushare",
        "stock_basic",
        pd.DataFrame({"ts_code": ["000001.SZ"]}),
    )

    manager.update_tushare_all(
        "income",
        kind="fundamental",
        start_date="2024-01-01",
        end_date="2024-01-02",
        workers=2,
    )

    income = manager.lake.read("tushare", "income")
    assert income["f_ann_date"].tolist() == ["20240102"]
    assert income["revenue"].tolist() == [1.0]


def test_tushare_vip_fundamental_update_reads_by_season(tmp_path) -> None:
    registry = DataSourceRegistry()
    source = FakeTushareUpdateSource()
    registry.register(source)
    manager = DataLakeManager(LocalDataLake(tmp_path), registry=registry)

    manager.update_tushare_all(
        "income_vip",
        start_date="2024-01-01",
        end_date="2024-09-30",
        workers=2,
    )

    assert sorted(source.calls["income_vip"], key=lambda item: str(item["period"])) == [
        {"period": "20240331"},
        {"period": "20240630"},
        {"period": "20240930"},
    ]


def test_tushare_vip_fundamental_update_is_incremental_by_season(tmp_path) -> None:
    registry = DataSourceRegistry()
    source = FakeTushareUpdateSource()
    registry.register(source)
    manager = DataLakeManager(LocalDataLake(tmp_path), registry=registry)
    manager.lake.add(
        "tushare",
        "income_vip",
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "f_ann_date": ["20240630"],
                "revenue": [1.0],
            }
        ),
    )
    manager.lake.add(
        "tushare",
        "stock_basic",
        pd.DataFrame({"ts_code": ["000001.SZ"]}),
    )

    manager.update_tushare_all(
        "income_vip",
        start_date="2024-01-01",
        end_date="2024-12-31",
        workers=2,
    )

    assert sorted(source.calls["income_vip"], key=lambda item: str(item["period"])) == [
        {"period": "20240930"},
        {"period": "20241231"},
    ]


def test_tushare_update_scan_reports_missing_vip_quarters(tmp_path) -> None:
    registry = DataSourceRegistry()
    source = FakeTushareUpdateSource()
    registry.register(source)
    manager = DataLakeManager(LocalDataLake(tmp_path), registry=registry)
    manager.lake.add(
        "tushare",
        "income_vip",
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "f_ann_date": ["20240630"],
                "revenue": [1.0],
            }
        ),
    )

    report = manager.scan_tushare_updates(
        ["income_vip"],
        kinds={"income_vip": "fundamental_vip"},
        start_date="2024-01-01",
        end_date="2024-12-31",
    )

    assert source.calls["income_vip"] == []
    assert report.plans[0].pending_items == ("2024-09-30", "2024-12-31")
    assert [job.filters for job in report.jobs] == [
        {"period": "20240930"},
        {"period": "20241231"},
    ]


def test_tushare_update_report_execution_uses_confirmed_jobs(tmp_path) -> None:
    registry = DataSourceRegistry()
    source = FakeTushareUpdateSource()
    registry.register(source)
    manager = DataLakeManager(LocalDataLake(tmp_path), registry=registry)
    report = manager.scan_tushare_updates(
        ["daily"],
        kinds={"daily": "price"},
        start_date="2024-01-01",
        end_date="2024-01-01",
    )

    manager.execute_tushare_update_report(report, workers=2)
    manager.execute_tushare_update_report(report, workers=2)

    assert source.calls["daily"] == [
        {"trade_date": "20240101"},
        {"trade_date": "20240101"},
    ]


def test_tushare_update_report_continues_after_failed_job(tmp_path) -> None:
    class PartiallyFailingSource(FakeTushareUpdateSource):
        def read(self, request: DataRequest) -> pd.DataFrame:
            if (
                request.dataset == "daily"
                and request.filters.get("trade_date") == "20240101"
            ):
                self.calls.setdefault(request.dataset, []).append(dict(request.filters))
                raise RuntimeError("API access limit exceeded")
            return super().read(request)

    registry = DataSourceRegistry()
    source = PartiallyFailingSource()
    registry.register(source)
    manager = DataLakeManager(LocalDataLake(tmp_path), registry=registry)
    report = manager.scan_tushare_updates(
        ["daily"],
        kinds={"daily": "price"},
        start_date="2024-01-01",
        end_date="2024-01-02",
    )
    events: list[dict[str, object]] = []

    refs = manager.execute_tushare_update_report(
        report,
        workers=1,
        progress=events.append,
    )

    assert len(refs) == 1
    assert manager.lake.read("tushare", "daily")["trade_date"].tolist() == ["20240102"]
    failed_events = [event for event in events if event.get("status") == "failed"]
    assert failed_events == [
        {
            "table": "daily",
            "kind": "price",
            "item": "2024-01-01",
            "completed": 1,
            "total": 2,
            "rows_written": 0,
            "snapshot": None,
            "status": "failed",
            "error": "API access limit exceeded",
            "filters": {"trade_date": "20240101"},
        }
    ]
    assert any(event.get("status") == "succeeded" for event in events)


def test_tushare_vip_fundamental_update_writes_quarter_partitions(tmp_path) -> None:
    registry = DataSourceRegistry()
    source = FakeTushareUpdateSource()
    registry.register(source)
    manager = DataLakeManager(LocalDataLake(tmp_path), registry=registry)

    manager.update_tushare_all(
        "income_vip",
        start_date="2024-01-01",
        end_date="2024-06-30",
        workers=2,
    )

    assert (tmp_path / "tushare" / "income_vip" / "year=2024" / "quarter=1").exists()
    assert (tmp_path / "tushare" / "income_vip" / "year=2024" / "quarter=2").exists()
    assert manager.lake.read("tushare", "income_vip")["f_ann_date"].tolist() == [
        "20240331",
        "20240331",
        "20240630",
        "20240630",
    ]


class CountingSource:
    name = "fake"

    def __init__(self) -> None:
        self.calls = 0

    def read(self, request: DataRequest) -> pd.DataFrame:
        self.calls += 1
        return pd.DataFrame({"value": [self.calls]})

    def exists(self, dataset: str) -> bool:
        return dataset == "prices"

    def describe(self, dataset: str):
        return {"dataset": dataset}


class CountingLocalDataLake(LocalDataLake):
    def __init__(self, root) -> None:
        super().__init__(root)
        self.read_calls: list[tuple[str, str]] = []
        self.read_kwargs: list[dict[str, object]] = []

    def read(self, source: str, dataset: str, **kwargs) -> pd.DataFrame:
        self.read_calls.append((source, dataset))
        self.read_kwargs.append(dict(kwargs))
        return super().read(source, dataset, **kwargs)


class FakeTushareUpdateSource:
    name = "tushare"

    def __init__(self) -> None:
        self.calls: dict[str, list[dict[str, object]]] = {
            "stock_basic": [],
            "trade_cal": [],
            "index_basic": [],
            "daily": [],
            "income": [],
            "income_vip": [],
            "index_member": [],
        }

    def read(self, request: DataRequest) -> pd.DataFrame:
        call = dict(request.filters)
        if request.start_date is not None:
            call["start_date"] = pd.Timestamp(request.start_date).strftime("%Y%m%d")
        if request.end_date is not None:
            call["end_date"] = pd.Timestamp(request.end_date).strftime("%Y%m%d")
        self.calls.setdefault(request.dataset, []).append(call)
        if request.dataset == "stock_basic":
            status = cast(str, request.filters.get("list_status"))
            codes = {
                "L": ["000001.SZ", "600000.SH"],
                "D": ["300001.SZ", "000001.SZ"],
                "P": ["600000.SH"],
            }.get(status, ["000001.SZ", "600000.SH"])
            return pd.DataFrame({"ts_code": codes})
        if request.dataset == "trade_cal":
            return pd.DataFrame(
                {
                    "cal_date": ["20240101", "20240102", "20240103"],
                    "is_open": [1, 0, 1],
                }
            )
        if request.dataset == "index_basic":
            return pd.DataFrame({"ts_code": ["000300.SH", "000905.SH"]})
        if request.dataset == "daily":
            trade_date = str(request.filters["trade_date"])
            return pd.DataFrame(
                {
                    "ts_code": ["000001.SZ"],
                    "trade_date": [trade_date],
                    "close": [1.0],
                }
            )
        if request.dataset == "income":
            start = pd.Timestamp(request.start_date)
            next_day = start + pd.Timedelta(days=1)
            ts_code = str(request.filters["ts_code"])
            data = pd.DataFrame(
                {
                    "ts_code": [ts_code, ts_code],
                    "f_ann_date": [
                        start.strftime("%Y%m%d"),
                        next_day.strftime("%Y%m%d"),
                    ],
                    "revenue": [2.0, 3.0],
                }
            )
            if request.end_date is not None:
                end = pd.Timestamp(request.end_date).strftime("%Y%m%d")
                data = data.loc[data["f_ann_date"] <= end]
            return data
        if request.dataset == "index_member":
            ts_code = str(request.filters["ts_code"])
            return pd.DataFrame({"ts_code": [ts_code], "f_ann_date": ["20240101"]})
        if request.dataset == "income_vip":
            period = str(request.filters["period"])
            return pd.DataFrame(
                {
                    "ts_code": ["000001.SZ", "600000.SH"],
                    "f_ann_date": [period, period],
                    "revenue": [2.0, 3.0],
                }
            )
        return pd.DataFrame()

    def exists(self, dataset: str) -> bool:
        return dataset in self.calls

    def describe(self, dataset: str):
        return {"dataset": dataset}
