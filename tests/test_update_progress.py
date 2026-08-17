from __future__ import annotations

from datetime import date, timedelta

import polars as pl

from bagelquant_data import DataLake, DatasetSpec, UpdateProgress


class ProgressSource:
    name = "custom"

    def __init__(self, *, fail: bool = False, paginated: bool = False) -> None:
        self.fail = fail
        self.paginated = paginated

    def fetch(self, dataset: str, request: dict[str, object]) -> pl.DataFrame:
        if self.fail:
            raise RuntimeError("provider failed")
        offset = int(str(request.get("offset", 0)))
        if self.paginated and offset == 0:
            codes = ["000001.SZ", "000002.SZ"]
        elif self.paginated and offset == 2:
            codes = ["000003.SZ"]
        elif self.paginated:
            codes = []
        else:
            codes = ["000001.SZ"]
        return pl.DataFrame(
            {
                "trade_date": [str(request["date"]).replace("-", "")] * len(codes),
                "ts_code": codes,
            }
        )


class RangeProgressSource:
    name = "custom"

    def fetch(self, dataset: str, request: dict[str, object]) -> pl.DataFrame:
        lower = date.fromisoformat(str(request["start_date"]))
        upper = date.fromisoformat(str(request["end_date"]))
        values = []
        current = lower
        while current <= upper:
            values.append(current)
            current += timedelta(days=1)
        return pl.DataFrame(
            {
                "trade_date": [value.strftime("%Y%m%d") for value in values],
                "ts_code": [f"{index:06d}.SZ" for index in range(len(values))],
            }
        )


def _lake(tmp_path, source: ProgressSource, *datasets: str) -> DataLake:
    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(source)
    lake.ingest(
        DatasetSpec("trade_cal", "general"),
        pl.DataFrame({"time": ["20250102"], "is_open": [1]}),
    )
    for dataset in datasets:
        lake.admin.datasets.register(
            DatasetSpec(
                dataset,
                "by_daily",
                calendar="trade_cal",
                field_mappings={"trade_date": "time", "ts_code": "asset_id"},
            )
        )
    return lake


def test_progress_callback_reports_ledger_phases_and_completion(tmp_path) -> None:
    lake = _lake(tmp_path, ProgressSource(), "daily")
    events: list[UpdateProgress] = []

    report = lake.update.dataset(
        "daily",
        source="custom",
        end="2025-01-02",
        progress_callback=events.append,
    )

    assert report.status == "success"
    assert events[0].phase == "sync"
    assert events[1].phase == "claim"
    assert any(event.phase == "fetch" and event.completed == 1 for event in events)
    assert any(event.phase == "commit" for event in events)
    assert events[-1].phase == "complete"
    assert events[-1].status == "success"
    assert events[-1].success_count == 1


def test_progress_callback_counts_paginated_request_as_one_scope(tmp_path) -> None:
    lake = _lake(tmp_path, ProgressSource(paginated=True), "daily")
    events: list[UpdateProgress] = []

    lake.update.dataset(
        "daily",
        source="custom",
        end="2025-01-02",
        source_options={"pagination": "offset", "page_size": 2},
        progress_callback=events.append,
    )

    assert events[0].total == 1
    assert any(event.completed == 1 and event.total == 1 for event in events)
    assert events[-1].total == 1


def test_range_backfill_progress_counts_logical_daily_scopes(tmp_path) -> None:
    source = RangeProgressSource()
    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(source)
    lake.ingest(
        DatasetSpec("trade_cal", "general"),
        pl.DataFrame(
            {
                "time": ["20250102", "20250103", "20250104"],
                "is_open": [1, 1, 1],
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
    events: list[UpdateProgress] = []

    lake.update.dataset(
        "daily",
        source="custom",
        end="2025-01-04",
        source_options={
            "daily_range_backfill": {
                "start_param": "start_date",
                "end_param": "end_date",
                "row_limit": 1_000,
                "max_scopes": 10,
                "max_pages": 100,
            }
        },
        progress_callback=events.append,
    )

    assert events[0].total == 3
    assert any(event.phase == "fetch" and event.completed == 3 for event in events)
    assert events[-1].total == 3
    assert events[-1].success_count == 3


def test_progress_callback_reports_multiple_datasets_and_failure(tmp_path) -> None:
    source = ProgressSource()
    lake = _lake(tmp_path, source, "daily", "daily_basic")
    events: list[UpdateProgress] = []

    report = lake.update.datasets(
        ["daily", "daily_basic"],
        source="custom",
        end="2025-01-02",
        progress_callback=events.append,
    )

    assert report.datasets == ("daily", "daily_basic")
    completed = {event.dataset for event in events if event.phase == "complete"}
    assert completed == {"daily", "daily_basic"}

    failed_lake = _lake(tmp_path / "failed", ProgressSource(fail=True), "daily")
    failed_events: list[UpdateProgress] = []
    failed = failed_lake.update.dataset(
        "daily",
        source="custom",
        end="2025-01-02",
        max_retries=1,
        progress_callback=failed_events.append,
    )

    assert failed.status == "failed"
    assert failed_events[-1].status == "failed"
    assert failed_events[-1].failure_count == 1
