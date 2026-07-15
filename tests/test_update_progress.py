from __future__ import annotations

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


def test_progress_callback_reports_plan_advance_and_completion(tmp_path) -> None:
    lake = _lake(tmp_path, ProgressSource(), "daily")
    events: list[UpdateProgress] = []

    report = lake.update.dataset(
        "daily",
        source="custom",
        end="2025-01-02",
        progress=False,
        progress_callback=events.append,
    )

    assert report.status == "success"
    assert events[0] == UpdateProgress("daily", "planned", 0, 1, 0, 0, 0, "running")
    assert any(event.phase == "new" and event.completed == 1 for event in events)
    assert events[-1].phase == "complete"
    assert events[-1].status == "success"
    assert events[-1].success_count == 1


def test_progress_callback_expands_paginated_total(tmp_path) -> None:
    lake = _lake(tmp_path, ProgressSource(paginated=True), "daily")
    events: list[UpdateProgress] = []

    lake.update.dataset(
        "daily",
        source="custom",
        end="2025-01-02",
        source_options={"pagination": "offset", "page_size": 2},
        progress=False,
        progress_callback=events.append,
    )

    assert events[0].total == 1
    assert any(event.completed == 2 and event.total == 2 for event in events)
    assert events[-1].total == 2


def test_progress_callback_reports_multiple_datasets_and_failure(tmp_path) -> None:
    source = ProgressSource()
    lake = _lake(tmp_path, source, "daily", "daily_basic")
    events: list[UpdateProgress] = []

    report = lake.update.datasets(
        ["daily", "daily_basic"],
        source="custom",
        end="2025-01-02",
        confirm=False,
        progress=False,
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
        progress=False,
        progress_callback=failed_events.append,
    )

    assert failed.status == "failed"
    assert failed_events[-1].status == "failed"
    assert failed_events[-1].failure_count == 1
