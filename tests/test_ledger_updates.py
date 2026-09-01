from __future__ import annotations

from datetime import date, timedelta
import sqlite3

import polars as pl
import pytest

import bagelquant_data
from bagelquant_data import DataLake, DatasetSpec
from bagelquant_data.core import ConfigurationError
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
        "daily", source="custom", start="2025-01-02", end="2025-01-03"
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
        "daily", source="custom", start="2025-01-02", end="2025-01-02"
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
        "income", source="custom", start="2025-01-01", end="2025-01-31"
    )
    row = lake.admin.status.update_scopes(dataset="income", source="custom")[0]
    assert report.status == "no_data"
    assert report.success_count == 0
    assert report.empty_count == 1
    assert report.rows_committed == 0
    assert row["status"] == "empty"
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
        "income", source="custom", start="2025-01-01", end="2025-01-31"
    )
    assert source.requests == []

    lake.update.dataset(
        "income", source="custom", start="2025-01-01", end="2025-02-01"
    )
    assert len(source.requests) == 1
    assert source.requests[0][1]["start"] == "2025-02-01"
    assert source.requests[0][1]["end"] == "2025-02-01"
    source.requests.clear()

    assert lake.admin.status.reset_update_scopes(
        [int(row["id"])], clear_watermark=True
    ) == 1
    assert lake.admin.status.provider_scope_checks(
        dataset="income", source="custom"
    ) == []
    lake.update.dataset(
        "income", source="custom", start="2025-01-01", end="2025-01-31"
    )
    assert len(source.requests) == 1


def test_asset_forward_update_rechecks_recent_days_for_late_rows(tmp_path) -> None:
    class LateFinancialSource:
        name = "custom"

        def __init__(self) -> None:
            self.requests: list[tuple[str, dict[str, object]]] = []
            self.publish_late = False

        def fetch(self, dataset: str, request: dict[str, object]) -> pl.DataFrame:
            self.requests.append((dataset, dict(request)))
            if not self.publish_late:
                return pl.DataFrame()
            return pl.DataFrame(
                {
                    "ann_date": ["20250131"],
                    "ts_code": [str(request["id"])],
                    "value": [1.0],
                }
            )

    source = LateFinancialSource()
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
        "income",
        source="custom",
        start="2025-01-01",
        end="2025-01-31",
        source_options={"asset_recent_recheck_days": 3},
    )
    source.publish_late = True
    source.requests.clear()

    report = lake.update.dataset(
        "income",
        source="custom",
        start="2025-01-01",
        end="2025-02-01",
        source_options={"asset_recent_recheck_days": 3},
    )

    assert report.status == "success"
    assert source.requests[0][1]["start"] == "2025-01-30"
    assert source.requests[0][1]["end"] == "2025-02-01"
    assert lake.query.query("income", source="custom").collect()["time"].item() == date(
        2025, 1, 31
    )


def test_empty_recheck_preserves_existing_committed_coverage(tmp_path) -> None:
    source = LedgerSource()
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
    first = lake.update.dataset(
        "income", source="custom", start="2025-01-01", end="2025-01-31"
    )
    committed = lake.admin.status.update_scopes(dataset="income", source="custom")[0]
    preserved = {
        key: committed[key]
        for key in ("data_max_time", "last_success_at", "row_count", "commit_run_id")
    }
    with lake.metadata.connect() as db:
        db.execute(
            "update provider_scope_checks set recheck_after='2000-01-01'"
        )
    source.empty = True
    source.requests.clear()

    second = lake.update.dataset(
        "income", source="custom", start="2025-01-01", end="2025-01-31"
    )

    row = lake.admin.status.update_scopes(dataset="income", source="custom")[0]
    assert first.status == "success"
    assert second.status == "no_data"
    assert row["status"] == "empty"
    assert {key: row[key] for key in preserved} == preserved
    assert len(source.requests) == 1
    api_call = lake.metadata._rows(
        "select status,result_kind from api_calls order by finished_at desc limit 1"
    )[0]
    assert api_call == {"status": "success", "result_kind": "empty"}


def test_cooperative_interruption_persists_completed_empties_and_resumes(tmp_path) -> None:
    source = LedgerSource(empty=True)
    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(source)
    assets = ["A", "B", "C", "D", "E"]
    lake.ingest(
        DatasetSpec("stock_basic", "general", field_mappings={"ts_code": "asset_id"}),
        pl.DataFrame(
            {"ts_code": assets, "list_date": ["20250101"] * len(assets)}
        ),
    )
    lake.admin.datasets.register(
        DatasetSpec(
            "balancesheet",
            "by_asset",
            asset_list="stock_basic",
            field_mappings={"ann_date": "time", "ts_code": "asset_id"},
        )
    )

    interrupted = lake.update.dataset(
        "balancesheet",
        source="custom",
        start="2025-01-01",
        end="2025-01-31",
        workers=1,
        max_in_flight=1,
        cancel_requested=lambda: len(source.requests) >= 1,
    )

    scopes = lake.admin.status.update_scopes(
        dataset="balancesheet", source="custom"
    )
    assert interrupted.status == "cancelled"
    assert interrupted.empty_count == 1
    assert sum(row["status"] == "empty" for row in scopes) == 1
    assert sum(row["status"] == "pending" for row in scopes) == 4
    assert all(row["status"] != "running" for row in scopes)
    assert lake.metadata.active_update_leases() == []

    source.requests.clear()
    resumed = lake.update.dataset(
        "balancesheet",
        source="custom",
        start="2025-01-01",
        end="2025-01-31",
        workers=1,
        max_in_flight=1,
    )

    assert resumed.status == "no_data"
    assert resumed.empty_count == 4
    assert len(source.requests) == 4
    assert all(
        row["status"] == "empty"
        for row in lake.admin.status.update_scopes(
            dataset="balancesheet", source="custom"
        )
    )


def test_recent_historical_empty_is_rechecked_and_remains_empty(tmp_path) -> None:
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
        )
    )

    report = lake.update.dataset(
        "daily",
        source="custom",
        start="2025-01-02",
        end="2025-01-02",
        max_retries=2,
        retry_backoff_seconds=0,
    )

    scope = lake.admin.status.update_scopes(dataset="daily", source="custom")[0]
    assert report.status == "no_data"
    assert scope["status"] == "empty"
    assert scope["data_max_time"] is None
    assert scope["last_success_at"] is None
    assert (
        lake.admin.status.provider_scope_checks(
            dataset="daily", source="custom"
        )[0]["recheck_after"]
        is None
    )
    assert len(source.requests) == 1

    source.requests.clear()
    lake.update.dataset(
        "daily",
        source="custom",
        start="2025-01-02",
        end="2025-01-02",
    )
    assert [request[1]["date"] for request in source.requests] == ["2025-01-02"]
    assert lake.admin.status.update_scopes(
        dataset="daily", source="custom"
    )[0]["status"] == "empty"
    api_call = lake.metadata._rows(
        "select request_kind,result_kind from api_calls order by rowid desc limit 1"
    )[0]
    assert api_call == {"request_kind": "empty_recheck", "result_kind": "empty"}


def test_dense_current_day_empty_is_terminal_provider_check(tmp_path) -> None:
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
        )
    )

    report = lake.update.dataset(
        "daily",
        source="custom",
        start=today,
        end=today,
    )

    assert report.status == "no_data"
    assert report.success_count == 0
    assert report.empty_count == 1
    scope = lake.admin.status.update_scopes(dataset="daily", source="custom")[0]
    assert scope["status"] == "empty"
    check = lake.admin.status.provider_scope_checks(
        dataset="daily", source="custom"
    )[0]
    assert check["checked_through"] == today.isoformat()
    assert check["recheck_after"] is None


def test_daily_update_rechecks_only_empty_scopes_in_last_twenty_sessions(
    tmp_path,
) -> None:
    class SelectiveSource(LedgerSource):
        def __init__(self) -> None:
            super().__init__()
            self.empty_dates: set[str] = set()

        def fetch(self, dataset: str, request: dict[str, object]) -> pl.DataFrame:
            value = str(request["date"])
            if value in self.empty_dates:
                self.requests.append((dataset, dict(request)))
                return pl.DataFrame()
            return super().fetch(dataset, request)

    source = SelectiveSource()
    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(source)
    sessions = [date(2025, 1, 1) + timedelta(days=index) for index in range(25)]
    lake.ingest(
        DatasetSpec("trade_cal", "general"),
        pl.DataFrame({"time": sessions, "is_open": [1] * len(sessions)}),
    )
    lake.admin.datasets.register(
        DatasetSpec(
            "daily",
            "by_daily",
            calendar="trade_cal",
            field_mappings={"trade_date": "time", "ts_code": "asset_id"},
        )
    )
    source.empty_dates = {value.isoformat() for value in sessions}
    lake.update.dataset(
        "daily",
        source="custom",
        start=sessions[0],
        end=sessions[-1],
        workers=4,
    )

    source.empty_dates.clear()
    source.requests.clear()
    report = lake.update.dataset(
        "daily",
        source="custom",
        start=sessions[0],
        end=sessions[-1],
        workers=4,
    )

    requested = {str(request["date"]) for _, request in source.requests}
    assert requested == {value.isoformat() for value in sessions[-20:]}
    assert report.success_count == 20
    scopes = lake.admin.status.update_scopes(dataset="daily", source="custom")
    assert [row["status"] for row in scopes[:5]] == ["empty"] * 5
    assert [row["status"] for row in scopes[5:]] == ["success"] * 20


def test_empty_rechecks_commit_before_incremental_requests_start(tmp_path) -> None:
    class PhaseSource(LedgerSource):
        def __init__(self) -> None:
            super().__init__(empty=True)
            self.lake: DataLake | None = None

        def fetch(self, dataset: str, request: dict[str, object]) -> pl.DataFrame:
            request_date = str(request["date"])
            if request_date == "2025-01-03":
                assert self.lake is not None
                repaired = self.lake.admin.status.update_scopes(
                    dataset="daily", source="custom"
                )
                assert next(
                    row for row in repaired if row["scope_key"] == "2025-01-02"
                )["status"] == "success"
            return super().fetch(dataset, request)

    source = PhaseSource()
    lake = DataLake.open(tmp_path)
    source.lake = lake
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
    lake.update.dataset(
        "daily", source="custom", start="2025-01-02", end="2025-01-02"
    )
    lake.ingest(
        DatasetSpec("trade_cal", "general"),
        pl.DataFrame({"time": ["20250102", "20250103"], "is_open": [1, 1]}),
    )
    source.empty = False
    source.requests.clear()

    lake.update.dataset(
        "daily",
        source="custom",
        start="2025-01-02",
        end="2025-01-03",
        workers=4,
    )

    assert [request["date"] for _, request in source.requests] == [
        "2025-01-02",
        "2025-01-03",
    ]


def test_incompatible_old_schema_is_rejected_without_migration(tmp_path) -> None:
    path = tmp_path / "metadata" / "lake.db"
    path.parent.mkdir(parents=True)
    with sqlite3.connect(path) as db:
        db.execute("create table metadata_state (key text primary key, value text)")
        db.execute(
            "insert into metadata_state(key,value) values ('schema_version','1')"
        )

    with pytest.raises(ConfigurationError, match="automatic migration is intentionally disabled"):
        DataLake.open(tmp_path)


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
        "income", source="custom", start="2025-01-01", end="2025-01-31"
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
        "daily", source="custom", start="2025-01-02", end="2025-01-03"
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
        "daily", source="custom", start="2025-01-02", end="2025-01-03"
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
        "daily", source="custom", start="2025-01-02", end="2025-01-03"
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
        "daily", source="custom", start="2025-01-02", end="2025-01-02"
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
        "daily", source="custom", start="2025-01-02", end="2025-01-02"
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


def test_atomic_parquet_write_supports_long_paths(tmp_path) -> None:
    directory = tmp_path
    while len(str(directory / "data.parquet")) < 242:
        directory /= "p"
    path = directory / "data.parquet"

    atomic_write_parquet(pl.DataFrame({"value": [1]}), path)

    assert pl.read_parquet(path).to_dicts() == [{"value": 1}]
