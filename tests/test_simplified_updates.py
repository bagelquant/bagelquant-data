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


def test_general_update_replaces_dataset_and_uses_runtime_params(tmp_path) -> None:
    source = StaticSource({"stock_basic": pl.DataFrame({"code": ["A"]})})
    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(source)
    lake.admin.datasets.register(DatasetSpec("stock_basic", "general"))

    lake.update.dataset("stock_basic", source="custom", params={"exchange": "SSE"}, progress=False)
    source.responses["stock_basic"] = pl.DataFrame({"code": ["B"]})
    lake.update.dataset("stock_basic", source="custom", progress=False)

    frame = cast(pl.DataFrame, lake.query.query_general("stock_basic", source="custom").collect())
    assert frame["code"].to_list() == ["B"]
    assert source.requests[0] == {"exchange": "SSE"}


def test_by_daily_fetches_missing_calendar_dates_and_writes_year_month(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    source = StaticSource({})
    lake.admin.sources.register(source)
    lake.ingest(DatasetSpec("trade_cal", "general"), pl.DataFrame({"time": ["20250102", "20250103", "20250104"], "is_open": [1, 1, 0]}))
    lake.ingest(DatasetSpec("daily", "by_daily", calendar="trade_cal"), pl.DataFrame({"trade_date": ["20250102"], "ts_code": ["000001.SZ"], "close": [9.0]}))

    lake.update.dataset("daily", source="custom", today="2025-01-04", progress=False)

    assert source.requests == [{"date": "2025-01-03"}]
    assert lake.admin.status.partitions("daily", source="custom")[0]["partition_path"] == "year=2025/month=01/data.parquet"


def test_by_asset_uses_asset_list_and_fixed_batch_paths(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    source = StaticSource({})
    lake.admin.sources.register(source)
    lake.ingest(DatasetSpec("stock_basic", "general"), pl.DataFrame({"ts_code": ["000001.SZ", "000002.SZ"]}))
    lake.ingest(DatasetSpec("fundamental", "by_asset", asset_list="stock_basic"), pl.DataFrame({"ann_date": ["20250102"], "ts_code": ["000001.SZ"], "value": [0.5]}))

    lake.update.dataset("fundamental", source="custom", start="2025-01-01", today="2025-01-04", progress=False)

    assert source.requests == [
        {"id": "000001.SZ", "start": "2025-01-03", "end": "2025-01-04"},
        {"id": "000002.SZ", "start": "2025-01-01", "end": "2025-01-04"},
    ]
    expected = {
        f"year=2025/batch={stable_bucket('000001.SZ', ASSET_BUCKET_COUNT):02d}/data.parquet",
        f"year=2025/batch={stable_bucket('000002.SZ', ASSET_BUCKET_COUNT):02d}/data.parquet",
    }
    assert {row["partition_path"] for row in lake.admin.status.partitions("fundamental", source="custom")} == expected
