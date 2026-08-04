from __future__ import annotations

from typing import Any, cast
from threading import Event, Lock

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

    lake.update.dataset("stock_basic", source="custom", progress=False)

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

    lake.update.dataset("stock_basic", source="custom", workers=1, progress=False)

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
        progress=False,
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
        progress=False,
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

    lake.update.dataset("st", source="custom", today="2025-01-02", progress=False)

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
        progress=False,
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
        "daily", source="custom", start="20250101", end="20250104", progress=False
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
        "daily", source="custom", start="19991231", end="19991231", progress=False
    )

    assert source.requests == [{"date": "1999-12-31"}]


def test_batch_update_confirmation_filters_jobs_and_quit_is_safe(
    tmp_path, monkeypatch, capsys
) -> None:
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

    answers = iter(["invalid", "4"])
    monkeypatch.setattr("builtins.input", lambda _: next(answers))
    report = lake.update.datasets(
        ["stock_basic", "daily"],
        source="custom",
        end="20250102",
        progress=False,
    )

    assert report.datasets == ("stock_basic",)
    assert source.requests == [{}]
    assert "Invalid selection" in capsys.readouterr().out

    source.requests.clear()
    monkeypatch.setattr("builtins.input", lambda _: "5")
    report = lake.update.datasets(
        ["stock_basic", "daily"],
        source="custom",
        end="20250102",
        progress=False,
    )
    assert report.datasets == ()
    assert source.requests == []


def test_noninteractive_selection_includes_explicit_general_refresh(tmp_path) -> None:
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
        confirm=False,
        progress=False,
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
        progress=False,
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
        confirm=False,
        workers=4,
        progress=False,
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
        progress=False,
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
        progress=False,
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
        progress=False,
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
        progress=False,
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
        "progress": False,
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
