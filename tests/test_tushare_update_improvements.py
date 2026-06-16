from __future__ import annotations

from threading import Lock
from typing import Any

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec, TushareSource
from bagelquant_data.core.exceptions import ConfigurationError
from bagelquant_data.core.request import RequestContext
from scripts.update_lake import _date_after, _effective_start


def stock_basic_spec() -> DatasetSpec:
    return DatasetSpec(
        name="stock_basic",
        source="tushare",
        source_dataset="stock_basic",
        category="reference",
        field_mapping={"ts_code": "ts_code"},
        required_columns=(),
        partition_strategy="single_file",
        update_mode="snapshot_replace",
        reference=True,
    )


def trade_cal_spec() -> DatasetSpec:
    return DatasetSpec(
        name="trade_cal",
        source="tushare",
        source_dataset="trade_cal",
        category="reference",
        field_mapping={"cal_date": "cal_date"},
        required_columns=(),
        partition_strategy="single_file",
        update_mode="snapshot_replace",
        reference=True,
    )


def daily_spec() -> DatasetSpec:
    return DatasetSpec(
        name="daily",
        source="tushare",
        source_dataset="daily",
        category="market",
        field_mapping={"ts_code": "ts_code", "trade_date": "trade_date"},
        required_columns=("asset_id", "time"),
        primary_key=("asset_id", "time"),
        asset_column="ts_code",
        time_column="trade_date",
        partition_strategy="year_month",
        deduplication="primary_key_last",
    )


def income_spec(**overrides: Any) -> DatasetSpec:
    values = {
        "name": "income",
        "source": "tushare",
        "source_dataset": "income",
        "category": "financial_statement",
        "field_mapping": {"ts_code": "ts_code", "f_ann_date": "f_ann_date", "end_date": "end_date"},
        "required_columns": ("asset_id", "time", "period"),
        "business_key": ("asset_id", "period", "report_type", "comp_type"),
        "asset_column": "ts_code",
        "time_column": "f_ann_date",
        "period_column": "end_date",
        "request_planner": "by_asset",
        "partition_strategy": "year_bucket",
        "deduplication": "exact_record_hash",
        "update_mode": "replace_asset",
        "point_in_time": True,
    }
    values.update(overrides)
    return DatasetSpec(**values)


def forecast_spec(**overrides: Any) -> DatasetSpec:
    values = {
        "name": "forecast",
        "source": "tushare",
        "source_dataset": "forecast",
        "category": "financial_event",
        "field_mapping": {"ts_code": "ts_code", "ann_date": "ann_date", "end_date": "end_date"},
        "required_columns": ("asset_id", "time", "period"),
        "asset_column": "ts_code",
        "time_column": "ann_date",
        "period_column": "end_date",
        "request_planner": "by_asset",
        "partition_strategy": "year_bucket",
        "deduplication": "exact_record_hash",
        "update_mode": "replace_asset",
        "point_in_time": True,
    }
    values.update(overrides)
    return DatasetSpec(**values)


def reference_page_spec() -> DatasetSpec:
    return DatasetSpec(
        name="paged",
        source="tushare",
        source_dataset="paged",
        category="reference",
        field_mapping={},
        required_columns=(),
        partition_strategy="single_file",
        update_mode="snapshot_replace",
        reference=True,
        request_options={
            "pagination": "offset",
            "page_size": 2,
            "limit_param": "limit",
            "offset_param": "offset",
            "offset_start": 0,
        },
    )


class FakeTushareSource:
    name = "tushare"

    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self._lock = Lock()

    def configure(self, **options: Any) -> None:
        self.options = options

    def plan_requests(self, dataset: DatasetSpec, context: Any) -> list[dict[str, Any]]:
        if dataset.category == "market":
            return [{"trade_date": trade_date} for trade_date in context.options.get("trade_dates", [])]
        if dataset.request_planner == "by_asset":
            return [{"ts_code": asset} for asset in context.assets or []]
        request: dict[str, Any] = {}
        if context.start is not None:
            request["start_date"] = context.start
        if context.end is not None:
            request["end_date"] = context.end
        return [request]

    def fetch(self, source_dataset: str, request: dict[str, Any]) -> pl.DataFrame:
        with self._lock:
            self.requests.append(dict(request))
        if source_dataset == "daily":
            assert set(request) == {"trade_date"}
            return pl.DataFrame({"ts_code": ["000001.SZ"], "trade_date": [request["trade_date"]], "close": [10.0]})
        return pl.DataFrame(
            {
                "ts_code": [request["ts_code"]],
                "f_ann_date": ["20240331"],
                "end_date": ["20231231"],
                "report_type": ["1"],
                "comp_type": ["1"],
                "n_income_attr_p": [1.0],
            }
        )


class FlakySource(FakeTushareSource):
    def __init__(self, fail_always: bool = False) -> None:
        super().__init__()
        self.fail_always = fail_always
        self.calls = 0

    def fetch(self, source_dataset: str, request: dict[str, Any]) -> pl.DataFrame:
        self.calls += 1
        if self.fail_always or self.calls == 1:
            raise RuntimeError("temporary tushare limit")
        return super().fetch(source_dataset, request)


class PagedSource(FakeTushareSource):
    def fetch(self, source_dataset: str, request: dict[str, Any]) -> pl.DataFrame:
        with self._lock:
            self.requests.append(dict(request))
        offset = int(request["offset"])
        if offset == 0:
            return pl.DataFrame({"symbol": ["A", "B"], "price": [1.0, 2.0]})
        if offset == 2:
            return pl.DataFrame({"symbol": ["C"], "price": [3.0]})
        return pl.DataFrame({"symbol": [], "price": []})


class DailyIncrementSource(FakeTushareSource):
    def fetch(self, source_dataset: str, request: dict[str, Any]) -> pl.DataFrame:
        with self._lock:
            self.requests.append(dict(request))
        return pl.DataFrame(
            {
                "ts_code": ["000001.SZ", "000002.SZ"],
                "trade_date": ["20240102", "20240103"],
                "close": [12.0, 20.0],
            }
        )


class DailyByRequestSource(FakeTushareSource):
    def fetch(self, source_dataset: str, request: dict[str, Any]) -> pl.DataFrame:
        with self._lock:
            self.requests.append(dict(request))
        return pl.DataFrame(
            {
                "ts_code": [f"{request['trade_date']}.SZ"],
                "trade_date": [request["trade_date"]],
                "close": [float(len(self.requests))],
            }
        )


class PartialIncomeSource(FakeTushareSource):
    def fetch(self, source_dataset: str, request: dict[str, Any]) -> pl.DataFrame:
        with self._lock:
            self.requests.append(dict(request))
        if request["ts_code"] == "600000.SH":
            raise RuntimeError("limit")
        return pl.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "f_ann_date": ["20240331"],
                "end_date": ["20231231"],
                "report_type": ["1"],
                "comp_type": ["1"],
                "n_income_attr_p": [2.0],
            }
        )


class EmptyDailySource(FakeTushareSource):
    def fetch(self, source_dataset: str, request: dict[str, Any]) -> pl.DataFrame:
        with self._lock:
            self.requests.append(dict(request))
        return pl.DataFrame(
            {
                "ts_code": pl.Series([], dtype=pl.String),
                "trade_date": pl.Series([], dtype=pl.String),
                "close": pl.Series([], dtype=pl.Float64),
            }
        )


class EmptyStringDailySource(FakeTushareSource):
    def fetch(self, source_dataset: str, request: dict[str, Any]) -> pl.DataFrame:
        with self._lock:
            self.requests.append(dict(request))
        return pl.DataFrame(
            {
                "ts_code": pl.Series([], dtype=pl.String),
                "trade_date": pl.Series([], dtype=pl.String),
                "close": pl.Series([], dtype=pl.String),
            }
        )


class DailyWithEmptyStringSource(FakeTushareSource):
    def fetch(self, source_dataset: str, request: dict[str, Any]) -> pl.DataFrame:
        with self._lock:
            self.requests.append(dict(request))
        if request["trade_date"] == "2024-01-03":
            return pl.DataFrame(
                {
                    "ts_code": pl.Series([], dtype=pl.String),
                    "trade_date": pl.Series([], dtype=pl.String),
                    "close": pl.Series([], dtype=pl.String),
                }
            )
        return pl.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": [request["trade_date"]],
                "close": [12.0],
            }
        )


class IncomeByAssetSource(FakeTushareSource):
    def fetch(self, source_dataset: str, request: dict[str, Any]) -> pl.DataFrame:
        with self._lock:
            self.requests.append(dict(request))
        if request["ts_code"] == "000002.SZ":
            raise RuntimeError("limit")
        return pl.DataFrame(
            {
                "ts_code": [request["ts_code"]],
                "f_ann_date": ["20240331"],
                "end_date": ["20231231"],
                "report_type": ["1"],
                "comp_type": ["1"],
                "n_income_attr_p": [float(len(self.requests))],
            }
        )


def seed_trade_cal(lake: DataLake, dates: list[str], is_open: list[int] | None = None) -> None:
    values: dict[str, Any] = {"cal_date": dates}
    if is_open is not None:
        values["is_open"] = is_open
    lake.ingest_frame(trade_cal_spec(), pl.DataFrame(values))


def test_configure_tushare_persists_and_redacts_token(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    lake.sources.register(TushareSource())
    lake.sources.configure_tushare("secret-token")

    reopened = DataLake.open(tmp_path)
    source = TushareSource()
    reopened.sources.register(source)

    assert source._token == "secret-token"
    assert reopened.sources.list()[0]["options"]["token"] == "<redacted>"


def test_market_update_does_not_send_ts_code(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    source = FakeTushareSource()
    lake.sources.register(source)
    seed_trade_cal(lake, ["20240102"], [1])
    lake.datasets.add(daily_spec())

    report = lake.update.dataset("daily", source="tushare", start="2024-01-02", end="2024-01-02", progress=False)

    assert report.status == "success"
    assert source.requests == [{"trade_date": "2024-01-02"}]


def test_market_update_excludes_closed_trade_cal_dates(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    source = FakeTushareSource()
    lake.sources.register(source)
    seed_trade_cal(lake, ["20240102", "20240103", "20240104"], [1, 0, 1])
    lake.datasets.add(daily_spec())

    report = lake.update.dataset("daily", source="tushare", start="2024-01-02", end="2024-01-04", progress=False)

    assert report.status == "success"
    assert source.requests == [{"trade_date": "2024-01-02"}, {"trade_date": "2024-01-04"}]


def test_market_update_requires_trade_cal(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    lake.sources.register(FakeTushareSource())
    lake.datasets.add(daily_spec())

    with pytest.raises(ConfigurationError, match="trade_cal"):
        lake.update.dataset("daily", source="tushare", start="2024-01-02", end="2024-01-04", progress=False)


def test_tushare_market_planning_uses_trade_dates_only() -> None:
    source = TushareSource(client=object())
    requests = list(
        source.plan_requests(
            daily_spec(),
            RequestContext(
                source="tushare",
                dataset="daily",
                start="2024-01-01",
                end="2024-01-31",
                options={
                    "workers": 8,
                    "progress": False,
                    "max_retries": 3,
                    "trade_dates": ["2024-01-02", "2024-01-03"],
                },
            ),
        )
    )

    assert requests == [{"trade_date": "2024-01-02"}, {"trade_date": "2024-01-03"}]


def test_financial_update_sends_ts_code_for_explicit_assets(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    source = FakeTushareSource()
    lake.sources.register(source)
    lake.datasets.add(income_spec())

    report = lake.update.dataset("income", source="tushare", assets=["000001.SZ", "600000.SH"], progress=False)

    assert report.status == "success"
    assert [{"ts_code": "000001.SZ"}, {"ts_code": "600000.SH"}] == sorted(
        source.requests, key=lambda item: item["ts_code"]
    )


def test_financial_update_derives_assets_from_stock_basic(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    source = FakeTushareSource()
    lake.sources.register(source)
    lake.ingest_frame(stock_basic_spec(), pl.DataFrame({"ts_code": ["000001.SZ", "600000.SH"]}))
    lake.datasets.add(income_spec())

    report = lake.update.dataset("income", source="tushare", progress=False)

    assert report.status == "success"
    assert sorted(request["ts_code"] for request in source.requests) == ["000001.SZ", "600000.SH"]


def test_financial_update_without_assets_requires_stock_basic(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    lake.sources.register(FakeTushareSource())
    lake.datasets.add(income_spec())

    with pytest.raises(ConfigurationError, match="stock_basic"):
        lake.update.dataset("income", source="tushare", progress=False)


def test_update_rejects_removed_noop_options(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    lake.sources.register(FakeTushareSource())
    lake.datasets.add(stock_basic_spec())

    with pytest.raises(ConfigurationError, match="force"):
        lake.update.dataset("stock_basic", source="tushare", force=True)


def test_retry_succeeds_after_temporary_failure(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    source = FlakySource()
    lake.sources.register(source)
    seed_trade_cal(lake, ["20240102"], [1])
    lake.datasets.add(daily_spec())

    report = lake.update.dataset(
        "daily",
        source="tushare",
        start="2024-01-02",
        end="2024-01-02",
        progress=False,
        max_retries=2,
        retry_backoff_seconds=0,
    )

    assert report.status == "success"
    assert report.request_count == 1
    assert report.success_count == 1
    assert source.calls == 2


def test_all_failed_dataset_records_failed_run(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    source = FlakySource(fail_always=True)
    lake.sources.register(source)
    seed_trade_cal(lake, ["20240102"], [1])
    lake.datasets.add(daily_spec())

    report = lake.update.dataset(
        "daily",
        source="tushare",
        start="2024-01-02",
        end="2024-01-02",
        progress=False,
        max_retries=2,
        retry_backoff_seconds=0,
    )

    assert report.status == "failed"
    assert report.failure_count == 1
    assert lake.status.runs(limit=1)[0]["status"] == "failed"


def test_offset_pagination_fetches_until_short_page(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    source = PagedSource()
    lake.sources.register(source)
    lake.datasets.add(reference_page_spec())

    report = lake.update.dataset("paged", source="tushare", progress=False)

    assert report.status == "success"
    assert report.request_count == 2
    assert [request["offset"] for request in source.requests] == [0, 2]
    page_frame = lake.query.reference("paged", source="tushare", collect=True)
    if isinstance(page_frame, pl.LazyFrame):
        page_frame = page_frame.collect()
    assert page_frame.height == 3


def test_update_lake_effective_start_uses_manifest_maximum_time(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    lake.ingest_frame(
        daily_spec(),
        pl.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20240102"], "close": [10.0]}),
    )

    assert _effective_start(lake, "daily", source="tushare", fallback_start="2000-01-01") == "2024-01-02"
    assert _date_after("2024-01-03", "2024-01-02")


def test_update_lake_effective_start_falls_back_without_manifest(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    lake.datasets.add(daily_spec())

    assert _effective_start(lake, "daily", source="tushare", fallback_start="2000-01-01") == "2000-01-01"


def test_update_lake_effective_start_uses_financial_statement_dataset_latest(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    lake.ingest_frame(
        income_spec(),
        pl.DataFrame(
            {
                "ts_code": ["000001.SZ", "600000.SH"],
                "f_ann_date": ["20240331", "20240501"],
                "end_date": ["20231231", "20240331"],
                "report_type": ["1", "1"],
                "comp_type": ["1", "1"],
                "n_income_attr_p": [1.0, 2.0],
            }
        ),
    )

    assert _effective_start(lake, "income", source="tushare", fallback_start="2000-01-01") == "2024-05-01"


def test_update_lake_effective_start_uses_financial_event_dataset_latest(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    lake.ingest_frame(
        forecast_spec(),
        pl.DataFrame(
            {
                "ts_code": ["000001.SZ", "600000.SH"],
                "ann_date": ["20240401", "20240615"],
                "end_date": ["20240331", "20240630"],
            }
        ),
    )

    assert _effective_start(lake, "forecast", source="tushare", fallback_start="2000-01-01") == "2024-06-15"


def test_upsert_incremental_update_preserves_existing_partition_rows(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    lake.ingest_frame(
        daily_spec(),
        pl.DataFrame(
            {
                "ts_code": ["000001.SZ", "000001.SZ"],
                "trade_date": ["20240101", "20240102"],
                "close": [10.0, 11.0],
            }
        ),
    )
    source = DailyIncrementSource()
    lake.sources.register(source)
    seed_trade_cal(lake, ["20240102", "20240103"], [1, 1])

    report = lake.update.dataset("daily", source="tushare", start="2024-01-02", end="2024-01-03", progress=False)

    assert report.status == "success"
    frame = lake.query.raw("daily", source="tushare", columns=["asset_id", "time", "close"]).collect()
    rows = sorted((row["asset_id"], str(row["time"]), row["close"]) for row in frame.to_dicts())
    assert rows == [
        ("000001.SZ", "2024-01-01", 10.0),
        ("000001.SZ", "2024-01-02", 12.0),
        ("000002.SZ", "2024-01-03", 20.0),
    ]


def test_empty_string_market_page_does_not_widen_numeric_batch(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    lake.ingest_frame(
        daily_spec(),
        pl.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20240101"], "close": [10.0]}),
    )
    source = DailyWithEmptyStringSource()
    lake.sources.register(source)
    seed_trade_cal(lake, ["20240102", "20240103"], [1, 1])

    report = lake.update.dataset("daily", source="tushare", start="2024-01-02", end="2024-01-03", progress=False)

    assert report.status == "success"
    assert report.request_count == 2
    assert report.success_count == 2
    assert report.rows_downloaded == 1
    frame = lake.query.raw("daily", source="tushare", columns=["asset_id", "time", "close"]).collect()
    assert frame.schema["close"] == pl.Float64
    rows = sorted((row["asset_id"], str(row["time"]), row["close"]) for row in frame.to_dicts())
    assert rows == [
        ("000001.SZ", "2024-01-01", 10.0),
        ("000001.SZ", "2024-01-02", 12.0),
    ]


def test_market_update_commits_one_run_across_month_batches(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    source = DailyByRequestSource()
    lake.sources.register(source)
    seed_trade_cal(lake, ["20240131", "20240201"], [1, 1])
    lake.datasets.add(daily_spec())

    report = lake.update.dataset("daily", source="tushare", start="2024-01-31", end="2024-02-01", progress=False)

    assert report.status == "success"
    assert report.request_count == 2
    assert report.rows_downloaded == 2
    assert sum(1 for run in lake.status.runs(limit=10) if run["dataset"] == "daily") == 1
    assert lake.query.raw("daily", source="tushare").collect().height == 2


def test_financial_update_batches_by_batch_size_and_records_one_run(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    source = IncomeByAssetSource()
    lake.sources.register(source)
    lake.datasets.add(income_spec())

    report = lake.update.dataset(
        "income",
        source="tushare",
        assets=["000001.SZ", "000002.SZ", "000003.SZ"],
        start="2024-03-31",
        end="2024-03-31",
        batch_size=1,
        progress=False,
        max_retries=1,
    )

    assert report.status == "partial"
    assert report.request_count == 3
    assert report.success_count == 2
    assert report.failure_count == 1
    assert sum(1 for run in lake.status.runs(limit=10) if run["dataset"] == "income") == 1
    frame = lake.query.raw("income", source="tushare", columns=["asset_id"]).collect()
    assert sorted(frame.get_column("asset_id").to_list()) == ["000001.SZ", "000003.SZ"]


def test_replace_asset_partial_update_preserves_failed_asset_rows(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    lake.ingest_frame(
        income_spec(),
        pl.DataFrame(
            {
                "ts_code": ["000001.SZ", "600000.SH"],
                "f_ann_date": ["20240331", "20240331"],
                "end_date": ["20231231", "20231231"],
                "report_type": ["1", "1"],
                "comp_type": ["1", "1"],
                "n_income_attr_p": [1.0, 6.0],
            }
        ),
    )
    source = PartialIncomeSource()
    lake.sources.register(source)

    report = lake.update.dataset(
        "income",
        source="tushare",
        assets=["000001.SZ", "600000.SH"],
        start="2024-03-31",
        end="2024-03-31",
        progress=False,
        max_retries=1,
    )

    assert report.status == "partial"
    frame = lake.query.raw("income", source="tushare", columns=["asset_id", "n_income_attr_p"]).collect()
    values = {row["asset_id"]: row["n_income_attr_p"] for row in frame.to_dicts()}
    assert values == {"000001.SZ": 2.0, "600000.SH": 6.0}


def test_empty_successful_update_is_noop_for_existing_data(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    lake.ingest_frame(
        daily_spec(),
        pl.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20240102"], "close": [10.0]}),
    )
    source = EmptyDailySource()
    lake.sources.register(source)
    seed_trade_cal(lake, ["20240103"], [1])

    report = lake.update.dataset("daily", source="tushare", start="2024-01-03", end="2024-01-03", progress=False)

    assert report.status == "success"
    assert report.rows_downloaded == 0
    assert lake.query.raw("daily", source="tushare").collect().height == 1


def test_empty_string_successful_update_is_noop_for_existing_data(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    lake.ingest_frame(
        daily_spec(),
        pl.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20240102"], "close": [10.0]}),
    )
    source = EmptyStringDailySource()
    lake.sources.register(source)
    seed_trade_cal(lake, ["20240103"], [1])

    report = lake.update.dataset("daily", source="tushare", start="2024-01-03", end="2024-01-03", progress=False)

    assert report.status == "success"
    assert report.request_count == 1
    assert report.success_count == 1
    assert report.rows_downloaded == 0
    frame = lake.query.raw("daily", source="tushare").collect()
    assert frame.height == 1
    assert frame.schema["close"] == pl.Float64
