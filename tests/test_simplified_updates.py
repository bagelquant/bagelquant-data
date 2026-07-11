from __future__ import annotations

from typing import cast

import polars as pl

from bagelquant_data import DataLake, DatasetSpec
from bagelquant_data.core import ASSET_BUCKET_COUNT
from bagelquant_data.core.hashing import stable_bucket


class StaticSource:
    name = "custom"

    def __init__(self, responses: dict[str, pl.DataFrame]) -> None:
        self.responses = responses
        self.requests: list[dict[str, object]] = []

    def fetch(self, dataset: str, request: dict[str, object]) -> pl.DataFrame:
        self.requests.append(dict(request))
        if dataset == "daily":
            return pl.DataFrame({"trade_date": [str(request["date"]).replace("-", "")], "ts_code": ["000001.SZ"], "close": [10.0]})
        if dataset == "fundamental":
            return pl.DataFrame({"ann_date": [str(request.get("start", "2025-01-01")).replace("-", "")], "ts_code": [str(request["id"])], "value": [1.0]})
        return self.responses[dataset]


class FanoutSource:
    name = "custom"

    def __init__(self, failed_status: str | None = None) -> None:
        self.failed_status = failed_status
        self.requests: list[dict[str, object]] = []

    def fetch(self, dataset: str, request: dict[str, object]) -> pl.DataFrame:
        self.requests.append(dict(request))
        status = str(request["list_status"])
        if status == self.failed_status:
            raise RuntimeError(f"failed status: {status}")
        return pl.DataFrame({"status": [status]})


def test_general_update_merges_dataset_and_runtime_params(tmp_path) -> None:
    source = StaticSource({"stock_basic": pl.DataFrame({"code": ["A"]})})
    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(source)
    lake.admin.datasets.register(
        DatasetSpec(
            "stock_basic",
            "general",
            source_api_params={"exchange": "SSE"},
            source_api_param_sets=({"list_status": "L"},),
        )
    )

    lake.update.dataset(
        "stock_basic",
        source="custom",
        params={"exchange": "SZSE", "list_status": "P"},
        progress=False,
    )
    source.responses["stock_basic"] = pl.DataFrame({"code": ["B"]})
    lake.update.dataset("stock_basic", source="custom", progress=False)

    frame = cast(pl.DataFrame, lake.query.query_general("stock_basic", source="custom").collect())
    assert frame["code"].to_list() == ["B"]
    assert source.requests[0] == {"exchange": "SZSE", "list_status": "P"}


def test_general_update_expands_parameter_sets_and_keeps_literal_default_lists(tmp_path) -> None:
    source = FanoutSource()
    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(source)
    lake.admin.datasets.register(
        DatasetSpec(
            "stock_basic",
            "general",
            source_api_params={"exchange": "SSE", "ts_code": ["000001.SZ", "000002.SZ"]},
            source_api_param_sets=({"list_status": ["L", "D", "P"]},),
        )
    )

    lake.update.dataset("stock_basic", source="custom", progress=False)

    assert source.requests == [
        {"exchange": "SSE", "ts_code": ["000001.SZ", "000002.SZ"], "list_status": "L"},
        {"exchange": "SSE", "ts_code": ["000001.SZ", "000002.SZ"], "list_status": "D"},
        {"exchange": "SSE", "ts_code": ["000001.SZ", "000002.SZ"], "list_status": "P"},
    ]
    assert lake.query.query_general("stock_basic", source="custom").collect()["status"].sort().to_list() == ["D", "L", "P"]


def test_parameter_set_cartesian_product_and_runtime_override(tmp_path) -> None:
    source = FanoutSource()
    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(source)
    lake.admin.datasets.register(
        DatasetSpec(
            "stock_basic",
            "general",
            source_api_param_sets=(
                {"list_status": ["L", "D"], "exchange": ["SSE", "SZSE"]},
                {"list_status": "P"},
            ),
        )
    )

    lake.update.dataset("stock_basic", source="custom", progress=False)

    assert source.requests == [
        {"list_status": "L", "exchange": "SSE"},
        {"list_status": "L", "exchange": "SZSE"},
        {"list_status": "D", "exchange": "SSE"},
        {"list_status": "D", "exchange": "SZSE"},
        {"list_status": "P"},
    ]


def test_general_update_retains_existing_data_when_parameter_set_call_fails(tmp_path) -> None:
    source = FanoutSource(failed_status="D")
    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(source)
    spec = DatasetSpec("stock_basic", "general", source_api_param_sets=({"list_status": ["L", "D"]},))
    lake.ingest(spec, pl.DataFrame({"status": ["old"]}))

    report = lake.update.dataset(
        "stock_basic",
        source="custom",
        max_retries=1,
        retry_backoff_seconds=0,
        progress=False,
    )

    assert report.status == "failed"
    assert lake.query.query_general("stock_basic", source="custom").collect()["status"].to_list() == ["old"]


def test_by_daily_fetches_missing_calendar_dates_and_writes_year_month(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    source = StaticSource({})
    lake.admin.sources.register(source)
    lake.ingest(DatasetSpec("trade_cal", "general"), pl.DataFrame({"time": ["20250102", "20250103", "20250104"], "is_open": [1, 1, 0]}))
    lake.ingest(
        DatasetSpec("daily", "by_daily", calendar="trade_cal", source_api_params={"date": "wrong", "exchange": "SSE"}),
        pl.DataFrame({"trade_date": ["20250102"], "ts_code": ["000001.SZ"], "close": [9.0]}),
    )

    lake.update.dataset("daily", source="custom", today="2025-01-04", params={"exchange": "SZSE"}, progress=False)

    assert source.requests == [{"date": "2025-01-03", "exchange": "SZSE"}]
    assert lake.admin.status.partitions("daily", source="custom")[0]["partition_path"] == "year=2025/month=01/data.parquet"


def test_by_daily_uses_configured_date_parameter(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    source = StaticSource({"st": pl.DataFrame({"trade_date": ["20250102"], "ts_code": ["000001.SZ"]})})
    lake.admin.sources.register(source)
    lake.ingest(DatasetSpec("trade_cal", "general"), pl.DataFrame({"time": ["20250102"], "is_open": [1]}))
    lake.admin.datasets.register(
        DatasetSpec(
            "st",
            "by_daily",
            calendar="trade_cal",
            date_param="pub_date",
            source_api_params={"pub_date": "wrong"},
        )
    )

    lake.update.dataset("st", source="custom", today="2025-01-02", progress=False)

    assert source.requests == [{"pub_date": "2025-01-02"}]


def test_by_asset_uses_asset_list_and_fixed_batch_paths(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    source = StaticSource({})
    lake.admin.sources.register(source)
    lake.ingest(DatasetSpec("stock_basic", "general"), pl.DataFrame({"ts_code": ["000001.SZ", "000002.SZ"]}))
    lake.ingest(
        DatasetSpec(
            "fundamental",
            "by_asset",
            asset_list="stock_basic",
            source_api_params={"id": "wrong", "start": "wrong", "end": "wrong", "limit": 10},
        ),
        pl.DataFrame({"ann_date": ["20250102"], "ts_code": ["000001.SZ"], "value": [0.5]}),
    )

    lake.update.dataset(
        "fundamental",
        source="custom",
        start="2025-01-01",
        today="2025-01-04",
        params={"limit": 25},
        progress=False,
    )

    assert source.requests == [
        {"id": "000001.SZ", "start": "2025-01-03", "end": "2025-01-04", "limit": 25},
        {"id": "000002.SZ", "start": "2025-01-01", "end": "2025-01-04", "limit": 25},
    ]
    expected = {
        f"year=2025/batch={stable_bucket('000001.SZ', ASSET_BUCKET_COUNT):02d}/data.parquet",
        f"year=2025/batch={stable_bucket('000002.SZ', ASSET_BUCKET_COUNT):02d}/data.parquet",
    }
    assert {row["partition_path"] for row in lake.admin.status.partitions("fundamental", source="custom")} == expected
