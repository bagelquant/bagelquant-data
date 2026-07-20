from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import sqlite3

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


def test_asset_empty_records_provider_check_without_local_success(tmp_path) -> None:
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

    report = lake.update.dataset(
        "income", source="custom", start="2025-01-01", end="2025-01-31", progress=False
    )
    row = lake.admin.status.update_scopes(dataset="income", source="custom")[0]
    assert report.status == "success"
    assert report.success_count == 0
    assert report.empty_count == 1
    assert report.rows_committed == 0
    assert row["status"] == "pending"
    assert row["checked_through"] is None
    assert row["data_max_time"] is None
    assert row["last_success_at"] is None
    checks = lake.admin.status.provider_scope_checks(
        dataset="income", source="custom"
    )
    assert len(checks) == 1
    assert checks[0]["checked_through"] == "2025-01-31"
    assert checks[0]["last_result"] == "empty"
    stored_run = next(
        run for run in lake.admin.status.runs() if run["run_id"] == report.run_id
    )
    assert stored_run["success_count"] == 0
    assert stored_run["empty_count"] == 1

    source.requests.clear()
    lake.update.dataset(
        "income", source="custom", start="2025-01-01", end="2025-01-31", progress=False
    )
    assert source.requests == []

    assert lake.admin.status.reset_dataset_update_coverage(
        ["income"], source="custom"
    ) == 1
    assert lake.admin.status.provider_scope_checks(
        dataset="income", source="custom"
    ) == []
    lake.update.dataset(
        "income", source="custom", start="2025-01-01", end="2025-01-31", progress=False
    )
    assert len(source.requests) == 1


def test_dense_historical_empty_retries_then_fails_without_coverage(tmp_path) -> None:
    source = LedgerSource(empty=True)
    lake = DataLake.open(tmp_path)
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
            historical_empty_is_error=True,
        )
    )

    with pytest.raises(RuntimeError, match="unexpected empty response"):
        lake.update.dataset(
            "daily",
            source="custom",
            start="2025-01-02",
            end="2025-01-02",
            max_retries=2,
            retry_backoff_seconds=0,
            progress=False,
        )

    scope = lake.admin.status.update_scopes(dataset="daily", source="custom")[0]
    assert scope["status"] == "failed"
    assert scope["data_max_time"] is None
    assert scope["last_success_at"] is None
    assert lake.admin.status.provider_scope_checks(
        dataset="daily", source="custom"
    ) == []
    assert len(source.requests) == 2


def test_dense_current_day_empty_is_provisional_provider_check(tmp_path) -> None:
    today = date.today()
    source = LedgerSource(empty=True)
    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(source)
    lake.ingest(
        DatasetSpec("trade_cal", "general"),
        pl.DataFrame({"time": [today.isoformat()], "is_open": [1]}),
    )
    lake.admin.datasets.register(
        DatasetSpec(
            "daily",
            "by_daily",
            calendar="trade_cal",
            field_mappings={"trade_date": "time", "ts_code": "asset_id"},
            historical_empty_is_error=True,
        )
    )

    report = lake.update.dataset(
        "daily",
        source="custom",
        start=today,
        end=today,
        progress=False,
    )

    assert report.status == "success"
    assert report.success_count == 0
    assert report.empty_count == 1
    scope = lake.admin.status.update_scopes(dataset="daily", source="custom")[0]
    assert scope["status"] == "pending"
    check = lake.admin.status.provider_scope_checks(
        dataset="daily", source="custom"
    )[0]
    assert check["checked_through"] == today.isoformat()
    assert check["recheck_after"] == (today + timedelta(days=1)).isoformat()


def test_existing_empty_coverage_migrates_to_separate_provider_check(tmp_path) -> None:
    lake = _daily_lake(tmp_path, LedgerSource())
    lake.update.dataset(
        "daily", source="custom", start="2025-01-02", end="2025-01-02", progress=False
    )
    with lake.metadata.connect() as db:
        db.execute("delete from provider_scope_checks")
        db.execute("delete from metadata_state where key='provider_check_version'")
        db.execute(
            "update update_scopes set status='empty',checked_through='2025-01-02',"
            "data_max_time=null,last_success_at=null"
        )

    reopened = DataLake.open(tmp_path)

    scope = reopened.admin.status.update_scopes(dataset="daily", source="custom")[0]
    assert scope["status"] == "pending"
    assert scope["checked_through"] is None
    assert scope["data_max_time"] is None
    check = reopened.admin.status.provider_scope_checks(
        dataset="daily", source="custom"
    )[0]
    assert check["checked_through"] == "2025-01-02"
    assert check["last_result"] == "empty"


def test_dataset_coverage_reset_preserves_committed_data_and_audit(tmp_path) -> None:
    source = LedgerSource()
    lake = _daily_lake(tmp_path, source)
    report = lake.update.dataset(
        "daily", source="custom", start="2025-01-02", end="2025-01-03", progress=False
    )
    before = lake.query.query("daily", source="custom").collect()

    reset = lake.admin.status.reset_dataset_update_coverage(
        ["daily"], source="custom"
    )

    assert reset == 2
    scopes = lake.admin.status.update_scopes(dataset="daily", source="custom")
    assert all(scope["status"] == "pending" for scope in scopes)
    assert all(scope["checked_through"] is None for scope in scopes)
    assert all(scope["data_max_time"] is not None for scope in scopes)
    assert lake.admin.status.provider_scope_checks(
        dataset="daily", source="custom"
    ) == []
    assert lake.query.query("daily", source="custom").collect().equals(before)
    assert any(run["run_id"] == report.run_id for run in lake.admin.status.runs())


def test_dataset_coverage_reset_rejects_active_lease(tmp_path) -> None:
    lake = _daily_lake(tmp_path, LedgerSource())
    lake.update.dataset(
        "daily", source="custom", start="2025-01-02", end="2025-01-03", progress=False
    )
    lake.metadata.acquire_update_leases([("custom", "daily", "active-run")])

    with pytest.raises(RuntimeError, match="Dataset update is active"):
        lake.admin.status.reset_dataset_update_coverage(
            ["daily"], source="custom"
        )


def test_asset_request_date_field_is_distinct_from_pit_time(tmp_path) -> None:
    class FinancialSource:
        name = "custom"

        def fetch(self, dataset: str, request: dict[str, object]) -> pl.DataFrame:
            return pl.DataFrame(
                {
                    "ann_date": ["20250131"],
                    "f_ann_date": ["20241231"],
                    "ts_code": [str(request["id"])],
                    "value": [1.0],
                }
            )

    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(FinancialSource())
    lake.ingest(
        DatasetSpec("stock_basic", "general", field_mappings={"ts_code": "asset_id"}),
        pl.DataFrame({"ts_code": ["A"], "list_date": ["20250101"]}),
    )
    lake.admin.datasets.register(
        DatasetSpec(
            "income",
            "by_asset",
            asset_list="stock_basic",
            request_date_field="ann_date",
            field_mappings={"f_ann_date": "time", "ts_code": "asset_id"},
        )
    )

    report = lake.update.dataset(
        "income", source="custom", start="2025-01-01", end="2025-01-31", progress=False
    )

    assert report.status == "success"
    frame = lake.query.query("income", source="custom").collect()
    assert frame["time"].item().isoformat() == "2024-12-31"
    scope = lake.admin.status.update_scopes(dataset="income", source="custom")[0]
    assert scope["checked_through"] == "2024-12-31"
    assert scope["data_max_time"] == "2024-12-31"
    provider_check = lake.admin.status.provider_scope_checks(
        dataset="income", source="custom"
    )[0]
    assert provider_check["checked_through"] == "2025-01-31"


def test_clear_dataset_data_preserves_registration_and_audit(tmp_path) -> None:
    source = LedgerSource()
    lake = _daily_lake(tmp_path, source)
    lake.update.dataset(
        "daily", source="custom", start="2025-01-02", end="2025-01-03", progress=False
    )
    before_runs = lake.admin.status.runs(20)

    result = lake.admin.datasets.clear_dataset_data(
        "daily", source="custom", confirm=True
    )

    assert result["partitions"] > 0
    assert result["rows"] == 2
    assert lake.admin.datasets.get("daily", source="custom").name == "daily"
    assert lake.admin.status.files("daily", source="custom") == []
    assert lake.admin.status.update_scopes(dataset="daily", source="custom") == []
    assert lake.admin.status.runs(20) == before_runs
    assert not lake.paths.dataset_root("custom", "daily").exists()


def test_clear_dataset_data_requires_confirmation_and_rejects_escape(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    lake.admin.datasets.register(DatasetSpec("../escape", "general"))

    with pytest.raises(Exception, match="confirm=True"):
        lake.admin.datasets.clear_dataset_data("../escape", source="custom")
    with pytest.raises(Exception, match="escapes lake root"):
        lake.admin.datasets.clear_dataset_data(
            "../escape", source="custom", confirm=True
        )


def test_clear_dataset_data_restores_files_on_metadata_failure(
    tmp_path, monkeypatch
) -> None:
    lake = _daily_lake(tmp_path, LedgerSource())
    lake.update.dataset(
        "daily", source="custom", start="2025-01-02", end="2025-01-03", progress=False
    )
    root = lake.paths.dataset_root("custom", "daily")
    files = sorted(path.relative_to(root) for path in root.rglob("*.parquet"))

    def fail(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        raise sqlite3.OperationalError("metadata unavailable")

    monkeypatch.setattr(lake.metadata, "clear_dataset_data", fail)
    with pytest.raises(sqlite3.OperationalError, match="metadata unavailable"):
        lake.admin.datasets.clear_dataset_data(
            "daily", source="custom", confirm=True
        )

    assert sorted(path.relative_to(root) for path in root.rglob("*.parquet")) == files


def test_deep_manifest_validation_detects_orphans_and_mismatches(tmp_path) -> None:
    lake = _daily_lake(tmp_path, LedgerSource())
    lake.update.dataset(
        "daily", source="custom", start="2025-01-02", end="2025-01-03", progress=False
    )
    healthy = lake.admin.status.validate_manifest(
        "daily", source="custom", deep=True
    )
    assert healthy["valid"]
    assert healthy["files_scanned"] == healthy["manifest_files"]

    root = lake.paths.dataset_root("custom", "daily")
    orphan = root / "year=1999" / "orphan.parquet"
    orphan.parent.mkdir(parents=True)
    pl.DataFrame({"time": ["1999-01-01"], "asset_id": ["X"]}).write_parquet(orphan)
    with_orphan = lake.admin.status.validate_manifest(
        "daily", source="custom", deep=True
    )
    assert with_orphan["orphaned_files"] == ["year=1999/orphan.parquet"]

    manifested = next(
        path for path in root.rglob("*.parquet") if path != orphan
    )
    pl.DataFrame(
        {"time": ["2025-01-02"], "asset_id": ["000001.SZ"], "close": [99.0]}
    ).write_parquet(manifested)
    mismatched = lake.admin.status.validate_manifest(
        "daily", source="custom", deep=True
    )
    assert any(issue["kind"] == "mismatch" for issue in mismatched["issues"])


def test_fast_manifest_validation_does_not_read_parquet(tmp_path, monkeypatch) -> None:
    lake = _daily_lake(tmp_path, LedgerSource())
    lake.update.dataset(
        "daily", source="custom", start="2025-01-02", end="2025-01-02", progress=False
    )

    def fail(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        raise AssertionError("fast health must not read parquet")

    monkeypatch.setattr(pl, "read_parquet", fail)
    result = lake.admin.status.validate_manifest(
        "daily", source="custom", deep=False
    )

    assert result["valid"]
    assert result["files_scanned"] == 0


def test_deep_manifest_validation_isolates_unreadable_file(tmp_path) -> None:
    lake = _daily_lake(tmp_path, LedgerSource())
    lake.update.dataset(
        "daily", source="custom", start="2025-01-02", end="2025-01-02", progress=False
    )
    path = next(lake.paths.dataset_root("custom", "daily").rglob("*.parquet"))
    path.write_bytes(b"not parquet")

    result = lake.admin.status.validate_manifest(
        "daily", source="custom", deep=True
    )

    assert not result["valid"]
    assert any(issue["kind"] == "unreadable" for issue in result["issues"])


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
