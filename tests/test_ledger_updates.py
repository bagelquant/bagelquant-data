from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

import bagelquant_data
from bagelquant_data import DataLake, DatasetSpec
from bagelquant_data.storage.atomic import atomic_write_parquet


class LedgerSource:
    name = "custom"

    def __init__(self, *, empty: bool = False, wrong_date: bool = False) -> None:
        self.empty = empty
        self.wrong_date = wrong_date
        self.requests: list[tuple[str, dict[str, object]]] = []

    def fetch(self, dataset: str, request: dict[str, object]) -> pl.DataFrame:
        self.requests.append((dataset, dict(request)))
        if self.empty:
            return pl.DataFrame()
        if dataset == "daily":
            value = (
                "20250103" if self.wrong_date else str(request["date"]).replace("-", "")
            )
            return pl.DataFrame(
                {"trade_date": [value], "ts_code": ["000001.SZ"], "close": [10.0]}
            )
        return pl.DataFrame(
            {
                "ann_date": [str(request["end"]).replace("-", "")],
                "ts_code": [str(request["id"])],
                "value": [1.0],
            }
        )


def _daily_lake(tmp_path, source: LedgerSource) -> DataLake:
    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(source)
    lake.ingest(
        DatasetSpec("trade_cal", "general"),
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
    return lake


def test_daily_ledger_synchronizes_and_commits_before_success(tmp_path) -> None:
    source = LedgerSource()
    lake = _daily_lake(tmp_path, source)

    report = lake.update.dataset(
        "daily", source="custom", start="2025-01-02", end="2025-01-03", progress=False
    )

    assert report.status == "success"
    rows = lake.admin.status.update_scopes(dataset="daily", source="custom")
    assert [(row["scope_key"], row["status"]) for row in rows] == [
        ("2025-01-02", "success"),
        ("2025-01-03", "success"),
    ]
    assert all(row["commit_run_id"] == report.run_id for row in rows)
    assert lake.admin.status.dataset("daily", source="custom")["row_count"] == 2


def test_wrong_daily_date_is_invalid_and_requires_reset(tmp_path) -> None:
    lake = _daily_lake(tmp_path, LedgerSource(wrong_date=True))

    report = lake.update.dataset(
        "daily", source="custom", start="2025-01-02", end="2025-01-02", progress=False
    )

    assert report.status == "failed"
    row = lake.admin.status.update_scopes(dataset="daily", source="custom")[0]
    assert row["status"] == "invalid"
    assert lake.admin.status.reset_update_scopes([int(row["id"])]) == 1
    assert (
        lake.admin.status.update_scopes(dataset="daily", source="custom")[0]["status"]
        == "pending"
    )


def test_asset_empty_advances_checked_through_without_data(tmp_path) -> None:
    source = LedgerSource(empty=True)
    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(source)
    lake.ingest(
        DatasetSpec("stock_basic", "general", field_mappings={"ts_code": "asset_id"}),
        pl.DataFrame({"ts_code": ["A"], "list_date": ["20250101"]}),
    )
    lake.admin.datasets.register(
        DatasetSpec(
            "income",
            "by_asset",
            asset_list="stock_basic",
            field_mappings={"ann_date": "time", "ts_code": "asset_id"},
        )
    )

    lake.update.dataset(
        "income", source="custom", start="2025-01-01", end="2025-01-31", progress=False
    )
    row = lake.admin.status.update_scopes(dataset="income", source="custom")[0]
    assert row["status"] == "empty"
    assert row["checked_through"] == "2025-01-31"
    source.requests.clear()
    lake.update.dataset(
        "income", source="custom", start="2025-01-01", end="2025-01-31", progress=False
    )
    assert source.requests == []


def test_commit_failure_cannot_publish_buffered_daily_success(
    tmp_path, monkeypatch
) -> None:
    lake = _daily_lake(tmp_path, LedgerSource())

    def fail_commit(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        raise PermissionError("locked partition")

    monkeypatch.setattr(lake._pipeline, "commit_frame", fail_commit)
    with pytest.raises(PermissionError, match="locked partition"):
        lake.update.dataset(
            "daily",
            source="custom",
            start="2025-01-02",
            end="2025-01-03",
            progress=False,
        )

    rows = lake.admin.status.update_scopes(dataset="daily", source="custom")
    assert {row["status"] for row in rows} == {"failed"}
    assert not any(row["commit_run_id"] for row in rows)
    assert lake.admin.status.runs(1)[0]["status"] == "failed"


def test_removed_audit_public_surface(tmp_path) -> None:
    assert not hasattr(bagelquant_data, "UpdatePlan")
    assert not hasattr(bagelquant_data, "CoverageSummary")
    lake = DataLake.open(tmp_path)
    assert not hasattr(lake.update, "plan")
    assert not hasattr(lake.update, "execute")
    assert not hasattr(lake.update, "state_fingerprint")


def test_bootstrap_seeds_physical_daily_dates_and_keeps_assets_pending(
    tmp_path,
) -> None:
    lake = DataLake.open(tmp_path)
    lake.ingest(
        DatasetSpec("trade_cal", "general"),
        pl.DataFrame({"time": ["20250102", "20250103"], "is_open": [1, 1]}),
    )
    lake.ingest(
        DatasetSpec(
            "daily",
            "by_daily",
            calendar="trade_cal",
            field_mappings={"trade_date": "time", "ts_code": "asset_id"},
        ),
        pl.DataFrame({"trade_date": ["20250102"], "ts_code": ["A"]}),
    )
    lake.ingest(
        DatasetSpec("stock_basic", "general", field_mappings={"ts_code": "asset_id"}),
        pl.DataFrame({"ts_code": ["A"], "list_date": ["20250101"]}),
    )
    lake.admin.datasets.register(
        DatasetSpec(
            "income",
            "by_asset",
            asset_list="stock_basic",
            field_mappings={"ann_date": "time", "ts_code": "asset_id"},
        )
    )
    with lake.metadata.connect() as db:
        db.execute("delete from metadata_state where key='update_state_version'")
        db.execute("create table pending_update_jobs(job_key text)")
        db.execute("create table update_coverage(scope_key text)")
        db.execute("create table audit_watermarks(dataset text)")

    preview = lake.update.bootstrap_update_state(start="2025-01-01", end="2025-01-03")
    assert preview["mode"] == "dry-run"
    assert not lake.metadata.update_state_ready()

    result = lake.update.bootstrap_update_state(
        start="2025-01-01", end="2025-01-03", apply=True
    )
    assert result["seeded_daily_scopes"] == 1
    assert result["backup"] and Path(result["backup"]).is_file()
    daily = lake.admin.status.update_scopes(dataset="daily", source="custom")
    assert [(row["scope_key"], row["status"]) for row in daily] == [
        ("2025-01-02", "success"),
        ("2025-01-03", "pending"),
    ]
    asset = lake.admin.status.update_scopes(dataset="income", source="custom")[0]
    assert asset["status"] == "pending"
    assert asset["checked_through"] is None
    with lake.metadata.connect() as db:
        tables = {
            row[0]
            for row in db.execute("select name from sqlite_master where type='table'")
        }
    assert not {"pending_update_jobs", "update_coverage", "audit_watermarks"} & tables


def test_atomic_parquet_replace_retries_transient_permission_errors(
    tmp_path, monkeypatch
) -> None:
    import bagelquant_data.storage.atomic as atomic

    real_replace = atomic.os.replace
    attempts = 0

    def flaky_replace(source, target):  # noqa: ANN001, ANN202
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise PermissionError("scanner lock")
        real_replace(source, target)

    monkeypatch.setattr(atomic.os, "replace", flaky_replace)
    monkeypatch.setattr(atomic.time, "sleep", lambda _: None)
    path = tmp_path / "data.parquet"
    atomic_write_parquet(pl.DataFrame({"value": [1]}), path)

    assert attempts == 3
    assert pl.read_parquet(path).to_dicts() == [{"value": 1}]
