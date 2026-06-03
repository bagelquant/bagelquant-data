from __future__ import annotations

import pandas as pd

from bagelquant_data.datasource import DataRequest, DataSourceRegistry
from bagelquant_data.lake import (
    DataLakeManager,
    LocalDataLake,
    PartitionSpec,
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
    assert "tushare_daily_close" in lake.data_item_ids("tushare")


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

    assert source.calls["stock_basic"] == [
        {"list_status": "L"},
        {"list_status": "D"},
        {"list_status": "P"},
    ]
    assert sorted(source.calls["daily"], key=lambda item: str(item["trade_date"])) == [
        {"trade_date": "20240101"},
        {"trade_date": "20240102"},
    ]
    assert daily.index.name == "date"
    assert daily["ts_code"].tolist() == ["000001.SZ", "000001.SZ"]


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


def test_tushare_fundamental_update_reads_incremental_id_by_id(tmp_path) -> None:
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

    manager.update_tushare_all(
        "income",
        kind="fundamental",
        start_date="2024-01-01",
        end_date="2024-01-04",
        workers=2,
    )

    calls = sorted(source.calls["income"], key=lambda item: str(item["ts_code"]))
    assert calls == [
        {"end_date": "20240104", "start_date": "20240103", "ts_code": "000001.SZ"},
        {"end_date": "20240104", "start_date": "20240101", "ts_code": "300001.SZ"},
        {"end_date": "20240104", "start_date": "20240101", "ts_code": "600000.SH"},
    ]


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


class FakeTushareUpdateSource:
    name = "tushare"

    def __init__(self) -> None:
        self.calls: dict[str, list[dict[str, object]]] = {
            "stock_basic": [],
            "daily": [],
            "income": [],
            "income_vip": [],
        }

    def read(self, request: DataRequest) -> pd.DataFrame:
        call = dict(request.filters)
        if request.start_date is not None:
            call["start_date"] = pd.Timestamp(request.start_date).strftime("%Y%m%d")
        if request.end_date is not None:
            call["end_date"] = pd.Timestamp(request.end_date).strftime("%Y%m%d")
        self.calls.setdefault(request.dataset, []).append(call)
        if request.dataset == "stock_basic":
            status = request.filters.get("list_status")
            codes = {
                "L": ["000001.SZ", "600000.SH"],
                "D": ["300001.SZ", "000001.SZ"],
                "P": ["600000.SH"],
            }.get(status, ["000001.SZ", "600000.SH"])
            return pd.DataFrame({"ts_code": codes})
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
            return pd.DataFrame(
                {
                    "ts_code": [request.filters["ts_code"]],
                    "f_ann_date": [request.start_date],
                    "revenue": [2.0],
                }
            )
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
