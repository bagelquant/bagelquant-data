from __future__ import annotations

from dataclasses import replace
from datetime import date

import polars as pl
import pytest

from bagelquant_data import (
    DataLake,
    DatasetSpec,
    StaleUpdatePlanError,
)


class AuditSource:
    name = "custom"

    def __init__(self, *, empty: bool = False) -> None:
        self.empty = empty
        self.requests: list[tuple[str, dict[str, object]]] = []

    def fetch(self, dataset: str, request: dict[str, object]) -> pl.DataFrame:
        self.requests.append((dataset, dict(request)))
        if self.empty:
            return pl.DataFrame(schema={"trade_date": pl.String, "ts_code": pl.String})
        if dataset == "daily":
            return pl.DataFrame(
                {
                    "trade_date": [str(request["date"]).replace("-", "")],
                    "ts_code": ["000001.SZ"],
                    "close": [10.0],
                }
            )
        raise KeyError(dataset)


def _daily_lake(tmp_path, *, source: AuditSource | None = None) -> DataLake:
    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(source or AuditSource())
    lake.ingest(
        DatasetSpec("trade_cal", "general"),
        pl.DataFrame(
            {
                "time": ["20250102", "20250103", "20250104"],
                "is_open": [1, 1, 0],
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


def test_full_daily_plan_finds_historical_gap(tmp_path) -> None:
    lake = _daily_lake(tmp_path)
    lake.ingest(
        lake.admin.datasets.get("daily", source="custom"),
        pl.DataFrame(
            {"trade_date": ["20250103"], "ts_code": ["000001.SZ"], "close": [9.0]}
        ),
    )

    plan = lake.update.plan(
        ["daily"], source="custom", start="2025-01-02", end="2025-01-04",
        audit="full",
    )

    assert plan.summaries[0].expected == 2
    assert plan.summaries[0].present == 1
    assert plan.summaries[0].missing == 1
    assert plan.datasets[0].requests == ({"date": "2025-01-02"},)


def test_state_fingerprint_ignores_identical_reregistration(tmp_path) -> None:
    lake = _daily_lake(tmp_path)
    spec = lake.admin.datasets.get("daily", source="custom")
    first = lake.update.state_fingerprint(source="custom")

    lake.admin.datasets.register(spec)
    repeated = lake.update.state_fingerprint(source="custom")
    lake.admin.datasets.register(
        replace(spec, source_api_params={"adjust": "qfq"})
    )

    assert repeated == first
    assert lake.update.state_fingerprint(source="custom") != first


def test_successful_empty_response_becomes_verified_coverage(tmp_path) -> None:
    source = AuditSource(empty=True)
    lake = _daily_lake(tmp_path, source=source)
    plan = lake.update.plan(
        ["daily"], source="custom", start="2025-01-02", end="2025-01-02",
        audit="full",
    )

    report = lake.update.execute(plan, progress=False)
    repeated = lake.update.plan(
        ["daily"], source="custom", start="2025-01-02", end="2025-01-02",
        audit="full",
    )

    assert report.runs[0].status == "success"
    assert repeated.summaries[0].verified_empty == 1
    assert repeated.summaries[0].missing == 0
    assert repeated.datasets[0].requests == ()


def test_full_asset_plan_uses_listing_active_years(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(AuditSource())
    lake.ingest(
        DatasetSpec(
            "stock_basic", "general", field_mappings={"ts_code": "asset_id"}
        ),
        pl.DataFrame(
            {
                "ts_code": ["A", "B"],
                "list_date": ["20240601", "20250101"],
                "delist_date": ["20250630", None],
            }
        ),
    )
    lake.admin.datasets.register(
        DatasetSpec(
            "income", "by_asset", asset_list="stock_basic",
            field_mappings={"ann_date": "time", "ts_code": "asset_id"},
        )
    )

    plan = lake.update.plan(
        ["income"], source="custom", start="2024-01-01", end="2025-12-31",
        audit="full",
    )

    assert plan.summaries[0].expected == 3
    assert {(row["id"], row["start"], row["end"]) for row in plan.datasets[0].requests} == {
        ("A", "2024-06-01", "2024-12-31"),
        ("A", "2025-01-01", "2025-06-30"),
        ("B", "2025-01-01", "2025-12-31"),
    }


def test_execute_rejects_stale_plan(tmp_path) -> None:
    lake = _daily_lake(tmp_path)
    plan = lake.update.plan(
        ["daily"], source="custom", start="2025-01-02", end="2025-01-02"
    )
    lake.ingest(DatasetSpec("other", "general"), pl.DataFrame({"value": [1]}))

    with pytest.raises(StaleUpdatePlanError, match="changed after preview"):
        lake.update.execute(plan, progress=False)


def test_state_fingerprint_matches_plan_and_changes_with_lake_state(tmp_path) -> None:
    lake = _daily_lake(tmp_path)
    before = lake.update.state_fingerprint(source="custom")
    plan = lake.update.plan(
        ["daily"], source="custom", start="2025-01-02", end="2025-01-02"
    )

    assert before == plan.state_fingerprint

    lake.ingest(DatasetSpec("other", "general"), pl.DataFrame({"value": [1]}))

    assert lake.update.state_fingerprint(source="custom") != before


def test_execute_reports_changed_partition_hashes(tmp_path) -> None:
    lake = _daily_lake(tmp_path)
    plan = lake.update.plan(
        ["daily"], source="custom", start="2025-01-02", end="2025-01-02"
    )

    report = lake.update.execute(plan, progress=False)

    assert len(report.changed_partitions) == 1
    change = report.changed_partitions[0]
    assert change.dataset == "daily"
    assert change.before_hash is None
    assert change.after_hash


def test_plan_exposes_exact_pending_retries(tmp_path) -> None:
    lake = _daily_lake(tmp_path)
    retry = {"date": "2025-01-02"}
    lake.metadata.record_failed_update_job(
        job_key="retry-daily",
        source="custom",
        dataset="daily",
        update_type="by_daily",
        request_params=retry,
        asset_id=None,
        error_message="temporary provider error",
    )

    plan = lake.update.plan(
        ["daily"], source="custom", start="2025-01-02", end="2025-01-02"
    )

    assert plan.datasets[0].pending_retries == (retry,)
    assert plan.summaries[0].retry == 1
    assert plan.summaries[0].audit_status == "incomplete"


def test_nonempty_current_day_response_is_not_provisional(tmp_path) -> None:
    current = date.today()
    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(AuditSource())
    lake.ingest(
        DatasetSpec("trade_cal", "general"),
        pl.DataFrame({"time": [current.strftime("%Y%m%d")], "is_open": [1]}),
    )
    lake.admin.datasets.register(
        DatasetSpec(
            "daily",
            "by_daily",
            calendar="trade_cal",
            field_mappings={"trade_date": "time", "ts_code": "asset_id"},
        )
    )
    plan = lake.update.plan(
        ["daily"], source="custom", start=current, end=current, audit="full"
    )

    lake.update.execute(plan, progress=False)
    coverage = lake.metadata.coverage("custom", "daily")

    assert len(coverage) == 1
    assert coverage[0]["row_count"] == 1
    assert coverage[0]["provisional"] == 0
