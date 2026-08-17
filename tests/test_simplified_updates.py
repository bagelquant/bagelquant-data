from __future__ import annotations

from datetime import date, timedelta
from threading import Event, Lock
from typing import Any, cast

import polars as pl

from bagelquant_data import DataLake, DatasetSpec
from bagelquant_data.core import ASSET_BUCKET_COUNT
from bagelquant_data.core.hashing import stable_bucket
from bagelquant_data.management.lake import _manifest_map, _partition_changes


def test_partition_change_coordinates_cover_deleted_boundary_rows() -> None:
    key = ("daily", "year=2025/month=01/data.parquet")
    before: dict[tuple[str, str], dict[str, Any]] = {
        key: {
            "content_hash": "before",
            "min_time": "2025-01-02",
            "max_time": "2025-01-31",
        }
    }
    after: dict[tuple[str, str], dict[str, Any]] = {
        key: {
            "content_hash": "after",
            "min_time": "2025-01-03",
            "max_time": "2025-01-30",
        }
    }

    change = _partition_changes(before, after)[0]

    assert change.min_time == "2025-01-02"
    assert change.max_time == "2025-01-31"


def test_update_manifest_snapshot_reads_only_selected_datasets() -> None:
    calls: list[tuple[str, str]] = []

    class Metadata:
        def manifest(self, source: str, dataset: str):  # noqa: ANN201
            calls.append((source, dataset))
            return [
                {
                    "dataset": dataset,
                    "partition_path": f"{dataset}.parquet",
                    "content_hash": dataset,
                }
            ]

    result = _manifest_map(
        cast(Any, Metadata()),
        "custom",
        ("daily", "income"),
    )

    assert calls == [("custom", "daily"), ("custom", "income")]
    assert set(result) == {
        ("daily", "daily.parquet"),
        ("income", "income.parquet"),
    }


class StaticSource:
    name = "custom"

    def __init__(self, responses: dict[str, pl.DataFrame]) -> None:
        self.responses = responses
        self.requests: list[dict[str, object]] = []

    def fetch(self, dataset: str, request: dict[str, object]) -> pl.DataFrame:
        self.requests.append(dict(request))
        if dataset == "daily":
            return pl.DataFrame(
                {
                    "trade_date": [str(request["date"]).replace("-", "")],
                    "ts_code": ["000001.SZ"],
                    "close": [10.0],
                }
            )
        if dataset == "fundamental":
            return pl.DataFrame(
                {
                    "ann_date": [
                        str(request.get("start", "2025-01-01")).replace("-", "")
                    ],
                    "ts_code": [str(request["id"])],
                    "value": [1.0],
                }
            )
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


class ConcurrentDailySource:
    name = "custom"

    def __init__(self, failures: set[str] | None = None, release_at: int = 1) -> None:
        self.failures = failures or set()
        self.release_at = release_at
        self.requests: list[tuple[str, dict[str, object]]] = []
        self.active = 0
        self.maximum_active = 0
        self.lock = Lock()
        self.release = Event()

    def fetch(self, dataset: str, request: dict[str, object]) -> pl.DataFrame:
        with self.lock:
            self.requests.append((dataset, dict(request)))
            self.active += 1
            self.maximum_active = max(self.maximum_active, self.active)
            if self.active >= self.release_at:
                self.release.set()
        self.release.wait(timeout=2)
        try:
            value = str(request["date"])
            if value in self.failures:
                raise RuntimeError(f"failed date: {value}")
            return pl.DataFrame(
                {
                    "trade_date": [value.replace("-", "")],
                    "ts_code": ["000001.SZ"],
                    "dataset": [dataset],
                }
            )
        finally:
            with self.lock:
                self.active -= 1


class PaginatedDailySource:
    name = "custom"

    def __init__(self) -> None:
        self.fail_second_page = True
        self.requests: list[dict[str, object]] = []

    def fetch(self, dataset: str, request: dict[str, object]) -> pl.DataFrame:
        self.requests.append(dict(request))
        offset = int(str(request["offset"]))
        if offset == 2 and self.fail_second_page:
            raise RuntimeError("page failed")
        count = 2 if offset == 0 else 1
        return pl.DataFrame(
            {
                "trade_date": [str(request["date"]).replace("-", "")] * count,
                "ts_code": [f"{offset + index:06d}.SZ" for index in range(count)],
            }
        )


class AdaptiveAssetSource:
    name = "custom"

    def __init__(self, rows: list[tuple[str, str]]) -> None:
        self.rows = rows
        self.requests: list[dict[str, object]] = []

    def fetch(self, dataset: str, request: dict[str, object]) -> pl.DataFrame:
        self.requests.append(dict(request))
        lower = date.fromisoformat(str(request["start"]))
        upper = date.fromisoformat(str(request["end"]))
        selected = [
            (announcement, period)
            for announcement, period in self.rows
            if lower <= date.fromisoformat(announcement) <= upper
        ]
        truncated = selected[-2:]
        return pl.DataFrame(
            {
                "ann_date": [value[0].replace("-", "") for value in truncated],
                "ts_code": [str(request["id"])] * len(truncated),
                "end_date": [value[1].replace("-", "") for value in truncated],
            }
        )


class DailyRangeSource:
    name = "custom"

    def __init__(
        self,
        rows: dict[str, tuple[str, ...]],
        *,
        truncate_at: int | None = None,
    ) -> None:
        self.rows = rows
        self.truncate_at = truncate_at
        self.fail = False
        self.requests: list[dict[str, object]] = []

    def fetch(self, dataset: str, request: dict[str, object]) -> pl.DataFrame:
        self.requests.append(dict(request))
        if self.fail:
            raise RuntimeError("range provider failed")
        if "date" in request:
            dates = [str(request["date"])]
        else:
            lower = date.fromisoformat(str(request["start_date"]))
            upper = date.fromisoformat(str(request["end_date"]))
            dates = [
                value
                for value in self.rows
                if lower <= date.fromisoformat(value) <= upper
            ]
        values = [
            (scope_day, str(request.get("index_code", asset_id)))
            for scope_day in dates
            for asset_id in self.rows.get(scope_day, ())
        ]
        if self.truncate_at is not None:
            values = values[-self.truncate_at :]
        return pl.DataFrame(
            {
                "trade_date": [value[0].replace("-", "") for value in values],
                "ts_code": [value[1] for value in values],
            },
            schema={"trade_date": pl.String, "ts_code": pl.String},
        )


def _daily_range_lake(
    tmp_path, source: DailyRangeSource, sessions: list[date]
) -> DataLake:
    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(source)
    lake.ingest(
        DatasetSpec("trade_cal", "general"),
        pl.DataFrame(
            {
                "time": [value.isoformat() for value in sessions],
                "is_open": [1] * len(sessions),
            }
        ),
    )
    lake.admin.datasets.register(
        DatasetSpec(
            "daily",
            "by_daily",
            calendar="trade_cal",
            field_mappings={"trade_date": "time", "ts_code": "asset_id"},
        )
    )
    return lake


def _daily_range_options() -> dict[str, Any]:
    return {
        "daily_range_backfill": {
            "start_param": "start_date",
            "end_param": "end_date",
            "row_limit": 1_000,
            "max_scopes": 1_024,
            "max_pages": 4_096,
        }
    }


def _adaptive_asset_lake(tmp_path, source: AdaptiveAssetSource) -> DataLake:
    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(source)
    lake.ingest(
        DatasetSpec(
            "stock_basic",
            "general",
            field_mappings={"ts_code": "asset_id"},
        ),
        pl.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "list_date": ["20200101"],
                "delist_date": [None],
            }
        ),
    )
    lake.admin.datasets.register(
        DatasetSpec(
            "financial",
            "by_asset",
            asset_list="stock_basic",
            request_date_field="ann_date",
            primary_key_extra=("end_date",),
            field_mappings={"ann_date": "time", "ts_code": "asset_id"},
        )
    )
    return lake


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
    )
    source.responses["stock_basic"] = pl.DataFrame({"code": ["B"]})
    lake.update.dataset("stock_basic", source="custom")

    frame = cast(
        pl.DataFrame, lake.query.query_general("stock_basic", source="custom").collect()
    )
    assert frame["code"].to_list() == ["B"]
    assert source.requests[0] == {"exchange": "SZSE", "list_status": "P"}


def test_general_update_expands_parameter_sets_and_keeps_literal_default_lists(
    tmp_path,
) -> None:
    source = FanoutSource()
    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(source)
    lake.admin.datasets.register(
        DatasetSpec(
            "stock_basic",
            "general",
            source_api_params={
                "exchange": "SSE",
                "ts_code": ["000001.SZ", "000002.SZ"],
            },
            source_api_param_sets=({"list_status": ["L", "D", "P"]},),
        )
    )

    lake.update.dataset("stock_basic", source="custom")

    assert sorted(source.requests, key=lambda request: str(request["list_status"])) == sorted([
        {"exchange": "SSE", "ts_code": ["000001.SZ", "000002.SZ"], "list_status": "L"},
        {"exchange": "SSE", "ts_code": ["000001.SZ", "000002.SZ"], "list_status": "D"},
        {"exchange": "SSE", "ts_code": ["000001.SZ", "000002.SZ"], "list_status": "P"},
    ], key=lambda request: str(request["list_status"]))
    assert lake.query.query_general("stock_basic", source="custom").collect()[
        "status"
    ].sort().to_list() == ["D", "L", "P"]


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

    lake.update.dataset("stock_basic", source="custom", workers=1)

    assert source.requests == [
        {"list_status": "L", "exchange": "SSE"},
        {"list_status": "L", "exchange": "SZSE"},
        {"list_status": "D", "exchange": "SSE"},
        {"list_status": "D", "exchange": "SZSE"},
        {"list_status": "P"},
    ]


def test_general_update_retains_existing_data_when_parameter_set_call_fails(
    tmp_path,
) -> None:
    source = FanoutSource(failed_status="D")
    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(source)
    spec = DatasetSpec(
        "stock_basic", "general", source_api_param_sets=({"list_status": ["L", "D"]},)
    )
    lake.ingest(spec, pl.DataFrame({"status": ["old"]}))

    report = lake.update.dataset(
        "stock_basic",
        source="custom",
        max_retries=1,
        retry_backoff_seconds=0,
    )

    assert report.status == "failed"
    assert lake.query.query_general("stock_basic", source="custom").collect()[
        "status"
    ].to_list() == ["old"]


def test_by_daily_fetches_missing_calendar_dates_and_writes_year_month(
    tmp_path,
) -> None:
    lake = DataLake.open(tmp_path)
    source = StaticSource({})
    lake.admin.sources.register(source)
    lake.ingest(
        DatasetSpec("trade_cal", "general"),
        pl.DataFrame(
            {"time": ["20250102", "20250103", "20250104"], "is_open": [1, 1, 0]}
        ),
    )
    lake.ingest(
        DatasetSpec(
            "daily",
            "by_daily",
            calendar="trade_cal",
            source_api_params={"date": "wrong", "exchange": "SSE"},
            field_mappings={"trade_date": "time", "ts_code": "asset_id"},
        ),
        pl.DataFrame(
            {"trade_date": ["20250102"], "ts_code": ["000001.SZ"], "close": [9.0]}
        ),
    )

    lake.update.dataset(
        "daily",
        source="custom",
        today="2025-01-04",
        params={"exchange": "SZSE"},
    )

    assert source.requests == [
        {"date": "2025-01-02", "exchange": "SZSE"},
        {"date": "2025-01-03", "exchange": "SZSE"},
    ]
    assert (
        lake.admin.status.partitions("daily", source="custom")[0]["partition_path"]
        == "year=2025/month=01/data.parquet"
    )


def test_by_daily_uses_configured_date_parameter(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    source = StaticSource(
        {"st": pl.DataFrame({"trade_date": ["20250102"], "ts_code": ["000001.SZ"]})}
    )
    lake.admin.sources.register(source)
    lake.ingest(
        DatasetSpec("trade_cal", "general"),
        pl.DataFrame({"time": ["20250102"], "is_open": [1]}),
    )
    lake.admin.datasets.register(
        DatasetSpec(
            "st",
            "by_daily",
            calendar="trade_cal",
            date_param="pub_date",
            source_api_params={"pub_date": "wrong"},
            field_mappings={"trade_date": "time", "ts_code": "asset_id"},
        )
    )

    lake.update.dataset("st", source="custom", today="2025-01-02")

    assert source.requests == [{"pub_date": "2025-01-02"}]


def test_by_asset_uses_asset_list_and_fixed_batch_paths(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    source = StaticSource({})
    lake.admin.sources.register(source)
    lake.ingest(
        DatasetSpec("stock_basic", "general", field_mappings={"ts_code": "asset_id"}),
        pl.DataFrame({"ts_code": ["000001.SZ", "000002.SZ"]}),
    )
    lake.ingest(
        DatasetSpec(
            "fundamental",
            "by_asset",
            asset_list="stock_basic",
            source_api_params={
                "id": "wrong",
                "start": "wrong",
                "end": "wrong",
                "limit": 10,
            },
            field_mappings={"ann_date": "time", "ts_code": "asset_id"},
        ),
        pl.DataFrame(
            {"ann_date": ["20250102"], "ts_code": ["000001.SZ"], "value": [0.5]}
        ),
    )

    lake.update.dataset(
        "fundamental",
        source="custom",
        start="2025-01-01",
        today="2025-01-04",
        params={"limit": 25},
    )

    assert source.requests == [
        {"id": "000001.SZ", "start": "2025-01-01", "end": "2025-01-04", "limit": 25},
        {"id": "000002.SZ", "start": "2025-01-01", "end": "2025-01-04", "limit": 25},
    ]
    expected = {
        f"year=2025/bucket={stable_bucket('000001.SZ', ASSET_BUCKET_COUNT):02d}/data.parquet",
        f"year=2025/bucket={stable_bucket('000002.SZ', ASSET_BUCKET_COUNT):02d}/data.parquet",
    }
    assert {
        row["partition_path"]
        for row in lake.admin.status.partitions("fundamental", source="custom")
    } == expected


def test_by_daily_ledger_checks_every_untracked_date(
    tmp_path,
) -> None:
    lake = DataLake.open(tmp_path)
    source = StaticSource({})
    lake.admin.sources.register(source)
    lake.ingest(
        DatasetSpec("trade_cal", "general"),
        pl.DataFrame(
            {"time": ["20250102", "20250103", "20250104"], "is_open": [1, 1, 1]}
        ),
    )
    lake.ingest(
        DatasetSpec(
            "daily",
            "by_daily",
            calendar="trade_cal",
            field_mappings={"trade_date": "time", "ts_code": "asset_id"},
        ),
        pl.DataFrame(
            {
                "trade_date": ["20250102", "20250104"],
                "ts_code": ["000001.SZ", "000001.SZ"],
                "close": [9.0, 10.0],
            }
        ),
    )

    lake.update.dataset(
        "daily", source="custom", start="20250101", end="20250104"
    )

    assert sorted(source.requests, key=lambda request: str(request["date"])) == [
        {"date": "2025-01-02"},
        {"date": "2025-01-03"},
        {"date": "2025-01-04"},
    ]


def test_empty_incremental_dataset_accepts_compact_fallback_dates(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    source = StaticSource({})
    lake.admin.sources.register(source)
    lake.ingest(
        DatasetSpec("trade_cal", "general"),
        pl.DataFrame({"time": ["19991231", "20000101"], "is_open": [1, 1]}),
    )
    lake.admin.datasets.register(
        DatasetSpec(
            "daily",
            "by_daily",
            calendar="trade_cal",
            field_mappings={"trade_date": "time", "ts_code": "asset_id"},
        )
    )

    lake.update.dataset(
        "daily", source="custom", start="19991231", end="19991231"
    )

    assert source.requests == [{"date": "1999-12-31"}]


def test_explicit_dataset_list_includes_general_refresh(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    source = StaticSource({"stock_basic": pl.DataFrame({"code": ["A"]})})
    lake.admin.sources.register(source)
    lake.ingest(
        DatasetSpec("trade_cal", "general"),
        pl.DataFrame({"time": ["20250102"], "is_open": [1]}),
    )
    lake.admin.datasets.register(DatasetSpec("stock_basic", "general"))
    lake.admin.datasets.register(
        DatasetSpec(
            "daily",
            "by_daily",
            calendar="trade_cal",
            field_mappings={"trade_date": "time", "ts_code": "asset_id"},
        )
    )

    report = lake.update.datasets(
        ["stock_basic", "daily"],
        source="custom",
        end="20250102",
    )

    assert report.datasets == ("stock_basic", "daily")
    assert source.requests == [{}, {"date": "2025-01-02"}]


def test_retry_wait_is_fixed_and_attempt_count_is_three(tmp_path, monkeypatch) -> None:
    source = FanoutSource(failed_status="L")
    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(source)
    lake.admin.datasets.register(
        DatasetSpec(
            "stock_basic", "general", source_api_param_sets=({"list_status": "L"},)
        )
    )
    waits: list[float] = []
    monkeypatch.setattr("bagelquant_data.pipeline.update.time.sleep", waits.append)

    report = lake.update.dataset(
        "stock_basic",
        source="custom",
        retry_backoff_seconds=60,
    )

    assert report.failure_count == 1
    assert len(source.requests) == 3
    assert waits == [60.0, 60.0]


def test_datasets_run_sequentially_with_workers_inside_each_dataset(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    source = ConcurrentDailySource(release_at=4)
    lake.admin.sources.register(source)
    lake.ingest(
        DatasetSpec("trade_cal", "general"),
        pl.DataFrame(
            {
                "time": ["20250102", "20250103", "20250104", "20250105"],
                "is_open": [1, 1, 1, 1],
            }
        ),
    )
    for name in ("daily", "daily_basic"):
        lake.admin.datasets.register(
            DatasetSpec(
                name,
                "by_daily",
                calendar="trade_cal",
                field_mappings={"trade_date": "time", "ts_code": "asset_id"},
            )
        )

    lake.update.datasets(
        ["daily", "daily_basic"],
        source="custom",
        end="2025-01-05",
        workers=4,
    )

    assert source.maximum_active == 4
    assert {dataset for dataset, _ in source.requests} == {"daily", "daily_basic"}
    requested_datasets = [dataset for dataset, _ in source.requests]
    boundary = requested_datasets.index("daily_basic")
    assert set(requested_datasets[:boundary]) == {"daily"}
    assert set(requested_datasets[boundary:]) == {"daily_basic"}


def test_failed_daily_job_is_retried_before_new_jobs(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    source = ConcurrentDailySource(failures={"2025-01-02"})
    lake.admin.sources.register(source)
    calendar = DatasetSpec("trade_cal", "general")
    lake.ingest(
        calendar,
        pl.DataFrame({"time": ["20250102", "20250103"], "is_open": [1, 1]}),
    )
    lake.admin.datasets.register(
        DatasetSpec(
            "daily",
            "by_daily",
            calendar="trade_cal",
            field_mappings={"trade_date": "time", "ts_code": "asset_id"},
        )
    )

    first = lake.update.dataset(
        "daily",
        source="custom",
        end="2025-01-03",
        workers=2,
        max_retries=1,
    )
    assert first.status == "partial"
    assert first.remaining_scope_count == 1
    assert lake.admin.status.update_scopes(
        dataset="daily", source="custom", status="failed"
    )

    source.failures.clear()
    source.requests.clear()
    lake.ingest(
        calendar,
        pl.DataFrame(
            {"time": ["20250102", "20250103", "20250104"], "is_open": [1, 1, 1]}
        ),
    )
    second = lake.update.dataset(
        "daily",
        source="custom",
        end="2025-01-04",
        workers=2,
        max_retries=1,
    )

    assert second.status == "success"
    assert [request["date"] for _, request in source.requests] == [
        "2025-01-02",
        "2025-01-04",
    ]
    assert (
        lake.admin.status.update_scopes(
            dataset="daily", source="custom", status="failed"
        )
        == []
    )


def test_persistent_failed_job_does_not_block_new_daily_work(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    source = ConcurrentDailySource(failures={"2025-01-02"})
    lake.admin.sources.register(source)
    calendar = DatasetSpec("trade_cal", "general")
    lake.ingest(
        calendar,
        pl.DataFrame({"time": ["20250102", "20250103"], "is_open": [1, 1]}),
    )
    lake.admin.datasets.register(
        DatasetSpec(
            "daily",
            "by_daily",
            calendar="trade_cal",
            field_mappings={"trade_date": "time", "ts_code": "asset_id"},
        )
    )
    lake.update.dataset(
        "daily",
        source="custom",
        end="2025-01-03",
        max_retries=1,
    )
    lake.ingest(
        calendar,
        pl.DataFrame(
            {"time": ["20250102", "20250103", "20250104"], "is_open": [1, 1, 1]}
        ),
    )
    source.requests.clear()

    report = lake.update.dataset(
        "daily",
        source="custom",
        end="2025-01-04",
        max_retries=1,
    )

    assert report.status == "partial"
    assert [request["date"] for _, request in source.requests] == [
        "2025-01-02",
        "2025-01-04",
    ]
    assert report.remaining_scope_count == 1
    assert lake.query.query("daily", source="custom").collect()[
        "time"
    ].max() == __import__("datetime").date(2025, 1, 4)


def test_paginated_failure_retries_the_whole_logical_job(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    source = PaginatedDailySource()
    lake.admin.sources.register(source)
    lake.ingest(
        DatasetSpec("trade_cal", "general"),
        pl.DataFrame({"time": ["20250102"], "is_open": [1]}),
    )
    lake.admin.datasets.register(
        DatasetSpec(
            "daily",
            "by_daily",
            calendar="trade_cal",
            field_mappings={"trade_date": "time", "ts_code": "asset_id"},
        )
    )
    options = {
        "source": "custom",
        "end": "2025-01-02",
        "source_options": {"pagination": "offset", "page_size": 2},
        "max_retries": 1,
    }

    first = lake.update.dataset("daily", **options)
    assert first.status == "failed"
    assert lake.admin.status.files("daily", source="custom") == []
    failed = lake.admin.status.update_scopes(
        dataset="daily", source="custom", status="failed"
    )
    assert failed[0]["scope_key"] == "2025-01-02"

    source.fail_second_page = False
    source.requests.clear()
    second = lake.update.dataset("daily", **options)

    assert second.status == "success"
    assert [request["offset"] for request in source.requests] == [0, 2]
    assert lake.query.query("daily", source="custom").collect().height == 3
    assert (
        lake.admin.status.update_scopes(
            dataset="daily", source="custom", status="failed"
        )
        == []
    )


def test_adaptive_date_range_discards_saturated_parents_and_commits_all_leaves(
    tmp_path,
) -> None:
    source = AdaptiveAssetSource(
        [
            ("2020-01-01", "2019-12-31"),
            ("2020-01-02", "2019-09-30"),
            ("2020-01-03", "2019-06-30"),
            ("2020-01-04", "2019-03-31"),
        ]
    )
    lake = _adaptive_asset_lake(tmp_path, source)

    report = lake.update.dataset(
        "financial",
        source="custom",
        start="2020-01-01",
        end="2020-01-04",
        source_options={
            "pagination": "adaptive_date_range",
            "row_limit": 2,
        },
    )

    assert report.status == "success"
    result = lake.query.query("financial", source="custom").collect().sort("time")
    assert result.height == 4
    assert result.get_column("time").to_list() == [
        date(2020, 1, 1),
        date(2020, 1, 2),
        date(2020, 1, 3),
        date(2020, 1, 4),
    ]
    calls = lake.metadata._rows(
        "select request_key,row_count,status from api_calls "
        "where dataset='financial' order by rowid"
    )
    assert calls[0]["row_count"] == 2
    assert len(calls) == 7
    assert all(row["status"] == "success" for row in calls)


def test_adaptive_date_range_rejects_a_saturated_minimum_window(tmp_path) -> None:
    source = AdaptiveAssetSource(
        [
            ("2020-01-01", "2019-12-31"),
            ("2020-01-01", "2019-09-30"),
        ]
    )
    lake = _adaptive_asset_lake(tmp_path, source)

    report = lake.update.dataset(
        "financial",
        source="custom",
        start="2020-01-01",
        end="2020-01-01",
        source_options={
            "pagination": "adaptive_date_range",
            "row_limit": 2,
        },
    )

    assert report.status == "failed"
    assert lake.admin.status.files("financial", source="custom") == []
    invalid = lake.admin.status.update_scopes(
        dataset="financial", source="custom", status="invalid"
    )
    assert len(invalid) == 1
    assert "still returned 2 rows" in str(invalid[0]["last_error"])


def test_daily_initial_range_maps_mixed_results_to_durable_scopes(tmp_path) -> None:
    sessions = [date(2025, 1, 1) + timedelta(days=index) for index in range(25)]
    rows = {
        value.isoformat(): (() if index < 2 else (f"{index:06d}.SZ",))
        for index, value in enumerate(sessions)
    }
    source = DailyRangeSource(rows)
    lake = _daily_range_lake(tmp_path, source, sessions)

    first = lake.update.dataset(
        "daily",
        source="custom",
        start=sessions[0],
        end=sessions[-1],
        source_options=_daily_range_options(),
    )

    assert first.status == "success"
    assert first.request_count == 1
    assert first.success_count == 23
    assert first.empty_count == 2
    assert source.requests == [
        {
            "start_date": sessions[0].isoformat(),
            "end_date": sessions[-1].isoformat(),
        }
    ]
    scopes = lake.admin.status.update_scopes(dataset="daily", source="custom")
    assert [row["status"] for row in scopes[:2]] == ["empty", "empty"]
    assert all(row["status"] == "success" for row in scopes[2:])
    assert all(row["commit_run_id"] == first.run_id for row in scopes[2:])
    checks = lake.admin.status.provider_scope_checks(
        dataset="daily", source="custom"
    )
    assert [row["checked_through"] for row in checks[:2]] == [
        sessions[0].isoformat(),
        sessions[1].isoformat(),
    ]
    calls = lake.metadata._rows(
        "select request_kind,scope_id,request_params from api_calls "
        "where dataset='daily' order by rowid"
    )
    assert len(calls) == 1
    assert calls[0]["request_kind"] == "initial_range_backfill"
    assert calls[0]["scope_id"] is None

    source.requests.clear()
    second = lake.update.dataset(
        "daily",
        source="custom",
        start=sessions[0],
        end=sessions[-1],
        source_options=_daily_range_options(),
    )

    assert second.request_count == 0
    assert source.requests == []


def test_daily_initial_range_requests_only_new_pending_tail(tmp_path) -> None:
    sessions = [date(2025, 1, 1) + timedelta(days=index) for index in range(6)]
    rows = {value.isoformat(): (f"{index:06d}.SZ",) for index, value in enumerate(sessions)}
    source = DailyRangeSource(rows)
    lake = _daily_range_lake(tmp_path, source, sessions)
    lake.update.dataset(
        "daily", source="custom", start=sessions[0], end=sessions[2]
    )
    source.requests.clear()

    report = lake.update.dataset(
        "daily",
        source="custom",
        start=sessions[0],
        end=sessions[-1],
        source_options=_daily_range_options(),
    )

    assert report.request_count == 1
    assert report.success_count == 3
    assert source.requests == [
        {
            "start_date": sessions[3].isoformat(),
            "end_date": sessions[-1].isoformat(),
        }
    ]


def test_daily_initial_range_groups_each_variant_independently(tmp_path) -> None:
    sessions = [date(2025, 1, 3), date(2025, 1, 6), date(2025, 1, 7)]
    rows = {value.isoformat(): ("ignored",) for value in sessions}
    source = DailyRangeSource(rows)
    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(source)
    lake.ingest(
        DatasetSpec("trade_cal", "general"),
        pl.DataFrame(
            {
                "time": [value.isoformat() for value in sessions],
                "is_open": [1] * len(sessions),
            }
        ),
    )
    lake.admin.datasets.register(
        DatasetSpec(
            "daily",
            "by_daily",
            calendar="trade_cal",
            source_api_param_sets=({"index_code": ["IDX-A", "IDX-B"]},),
            field_mappings={"trade_date": "time", "ts_code": "asset_id"},
        )
    )

    report = lake.update.dataset(
        "daily",
        source="custom",
        start=sessions[0],
        end=sessions[-1],
        source_options=_daily_range_options(),
    )

    assert report.request_count == 2
    assert report.success_count == 6
    assert {
        (request["index_code"], request["start_date"], request["end_date"])
        for request in source.requests
    } == {
        ("IDX-A", sessions[0].isoformat(), sessions[-1].isoformat()),
        ("IDX-B", sessions[0].isoformat(), sessions[-1].isoformat()),
    }


def test_daily_initial_range_discards_saturated_parents(tmp_path) -> None:
    sessions = [date(2025, 1, 1) + timedelta(days=index) for index in range(4)]
    rows = {value.isoformat(): (f"{index:06d}.SZ",) for index, value in enumerate(sessions)}
    source = DailyRangeSource(rows, truncate_at=2)
    lake = _daily_range_lake(tmp_path, source, sessions)
    options = _daily_range_options()
    options["daily_range_backfill"]["row_limit"] = 2

    report = lake.update.dataset(
        "daily",
        source="custom",
        start=sessions[0],
        end=sessions[-1],
        source_options=options,
    )

    assert report.status == "success"
    assert report.request_count == 7
    assert report.success_count == 4
    assert lake.query.query("daily", source="custom").collect().height == 4
    calls = lake.metadata._rows(
        "select row_count,status,request_kind from api_calls "
        "where dataset='daily' order by rowid"
    )
    assert len(calls) == 7
    assert all(row["request_kind"] == "initial_range_backfill" for row in calls)
    assert all(row["status"] == "success" for row in calls)


def test_daily_initial_range_saturated_leaf_invalidates_whole_group(tmp_path) -> None:
    sessions = [date(2025, 1, 1), date(2025, 1, 2)]
    rows = {
        sessions[0].isoformat(): ("000001.SZ", "000002.SZ"),
        sessions[1].isoformat(): (),
    }
    source = DailyRangeSource(rows, truncate_at=2)
    lake = _daily_range_lake(tmp_path, source, sessions)
    options = _daily_range_options()
    options["daily_range_backfill"]["row_limit"] = 2

    report = lake.update.dataset(
        "daily",
        source="custom",
        start=sessions[0],
        end=sessions[-1],
        source_options=options,
    )

    assert report.status == "failed"
    assert lake.admin.status.files("daily", source="custom") == []
    scopes = lake.admin.status.update_scopes(dataset="daily", source="custom")
    assert [row["status"] for row in scopes] == ["invalid", "invalid"]
    assert all("still returned 2 rows" in str(row["last_error"]) for row in scopes)


def test_daily_initial_range_resumes_after_cooperative_cancel(tmp_path) -> None:
    sessions = [date(2025, 1, 1) + timedelta(days=index) for index in range(3)]
    rows = {value.isoformat(): (f"{index:06d}.SZ",) for index, value in enumerate(sessions)}
    source = DailyRangeSource(rows)
    lake = _daily_range_lake(tmp_path, source, sessions)

    canceled = lake.update.dataset(
        "daily",
        source="custom",
        start=sessions[0],
        end=sessions[-1],
        source_options=_daily_range_options(),
        cancel_requested=lambda: True,
    )

    assert canceled.status == "cancelled"
    assert source.requests == []
    assert all(
        row["status"] == "pending"
        for row in lake.admin.status.update_scopes(
            dataset="daily", source="custom"
        )
    )

    resumed = lake.update.dataset(
        "daily",
        source="custom",
        start=sessions[0],
        end=sessions[-1],
        source_options=_daily_range_options(),
    )

    assert resumed.status == "success"
    assert resumed.request_count == 1
    assert source.requests == [
        {
            "start_date": sessions[0].isoformat(),
            "end_date": sessions[-1].isoformat(),
        }
    ]


def test_daily_range_transport_failure_retries_as_individual_days(tmp_path) -> None:
    sessions = [date(2025, 1, 1) + timedelta(days=index) for index in range(3)]
    rows = {value.isoformat(): (f"{index:06d}.SZ",) for index, value in enumerate(sessions)}
    source = DailyRangeSource(rows)
    source.fail = True
    lake = _daily_range_lake(tmp_path, source, sessions)

    failed = lake.update.dataset(
        "daily",
        source="custom",
        start=sessions[0],
        end=sessions[-1],
        source_options=_daily_range_options(),
        max_retries=1,
    )

    assert failed.status == "failed"
    assert failed.failure_count == 3
    source.fail = False
    source.requests.clear()

    retried = lake.update.dataset(
        "daily",
        source="custom",
        start=sessions[0],
        end=sessions[-1],
        source_options=_daily_range_options(),
    )

    assert retried.status == "success"
    assert retried.request_count == 3
    assert [request["date"] for request in source.requests] == [
        value.isoformat() for value in sessions
    ]
