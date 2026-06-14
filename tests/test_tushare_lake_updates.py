from __future__ import annotations

from datetime import date

import pandas as pd
import polars as pl

from bagelquant_data.datasource import DataSourceRegistry, TushareDataSource
from bagelquant_data.lake import (
    DataLakeManager,
    LocalDataLake,
    TushareTableUpdateSpec,
)


def manager_with_client(tmp_path, client) -> DataLakeManager:
    registry = DataSourceRegistry()
    registry.register(TushareDataSource(token="token", client=client))
    return DataLakeManager(LocalDataLake(tmp_path), registry=registry)


def seed_refs(manager: DataLakeManager) -> None:
    manager.lake.write(
        "tushare",
        "trade_cal",
        pl.DataFrame(
            {
                "cal_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
                "is_open": [1, 0, 1],
            }
        ),
        mode="overwrite",
    )
    manager.lake.write(
        "tushare",
        "stock_basic",
        pl.DataFrame({"ts_code": ["000001.SZ", "000002.SZ"]}),
        mode="overwrite",
    )


def test_scan_price_updates_resume_from_successful_call_log(tmp_path) -> None:
    manager = manager_with_client(tmp_path, object())
    seed_refs(manager)
    manager.lake.write(
        "tushare",
        "__api_call_log",
        pl.DataFrame(
            [
                {
                    "called_at": "2024-01-01T00:00:00+00:00",
                    "api_name": "daily",
                    "table": "daily",
                    "kind": "price",
                    "item_key": "trade_date",
                    "item_value": "20240101",
                    "request_start_date": "2024-01-01",
                    "request_end_date": "2024-01-01",
                    "data_min_time": "2024-01-01",
                    "data_max_time": "2024-01-01",
                    "rows": 1,
                    "status": "success",
                    "error": "",
                    "duration_ms": 1,
                    "request_hash": "a",
                    "snapshot_id": "snapshot",
                    "params_json": "{}",
                    "fields_json": "[]",
                },
                {
                    "called_at": "2024-01-03T00:00:00+00:00",
                    "api_name": "daily",
                    "table": "daily",
                    "kind": "price",
                    "item_key": "trade_date",
                    "item_value": "20240103",
                    "request_start_date": "2024-01-03",
                    "request_end_date": "2024-01-03",
                    "data_min_time": None,
                    "data_max_time": None,
                    "rows": 0,
                    "status": "failed",
                    "error": "boom",
                    "duration_ms": 1,
                    "request_hash": "b",
                    "snapshot_id": "",
                    "params_json": "{}",
                    "fields_json": "[]",
                },
            ]
        ),
        mode="overwrite",
    )

    report = manager.scan_tushare_updates(
        (TushareTableUpdateSpec(table="daily", kind="price"),),
        start_date="2024-01-01",
        end_date="2024-01-03",
    )

    assert [job.item_value for job in report.jobs] == ["20240103"]


def test_scan_fundamental_updates_resume_per_asset(tmp_path) -> None:
    manager = manager_with_client(tmp_path, object())
    seed_refs(manager)
    manager.lake.write(
        "tushare",
        "__api_call_log",
        pl.DataFrame(
            {
                "called_at": ["2024-01-10T00:00:00+00:00"],
                "api_name": ["income"],
                "table": ["income"],
                "kind": ["fundamental"],
                "item_key": ["ts_code"],
                "item_value": ["000001.SZ"],
                "request_start_date": ["2024-01-01"],
                "request_end_date": ["2024-01-10"],
                "data_min_time": ["2024-01-01"],
                "data_max_time": ["2024-01-10"],
                "rows": [1],
                "status": ["empty"],
                "error": [""],
                "duration_ms": [1],
                "request_hash": ["a"],
                "snapshot_id": [""],
                "params_json": ["{}"],
                "fields_json": ["[]"],
            }
        ),
        mode="overwrite",
    )

    report = manager.scan_tushare_updates(
        (TushareTableUpdateSpec(table="income", kind="fundamental"),),
        start_date="2024-01-01",
        end_date="2024-01-31",
    )

    starts = {job.item_value: job.start_date for job in report.jobs}
    assert starts["000001.SZ"] is not None
    assert starts["000002.SZ"] is not None
    assert starts["000001.SZ"].isoformat() == "2024-01-10"
    assert starts["000002.SZ"].isoformat() == "2024-01-01"


def test_scan_fundamental_without_refs_is_pending_when_no_local_data(tmp_path) -> None:
    manager = manager_with_client(tmp_path, object())

    report = manager.scan_tushare_updates(
        (TushareTableUpdateSpec(table="income", kind="fundamental"),),
        start_date="2024-01-01",
        end_date="2024-01-31",
    )

    assert report.plans[0].status == "pending"
    assert report.jobs[0].item == "table=income"
    assert report.jobs[0].start_date is not None
    assert report.jobs[0].start_date.isoformat() == "2024-01-01"


def test_scan_fundamental_migrates_legacy_log_state_for_up_to_date(
    tmp_path,
) -> None:
    manager = manager_with_client(tmp_path, object())
    seed_refs(manager)
    manager.lake.write(
        "tushare",
        "__api_call_log",
        pl.DataFrame(
            [
                {
                    "called_at": "2024-01-31T00:00:00+00:00",
                    "api_name": "income",
                    "table": "income",
                    "kind": "fundamental",
                    "item_key": "ts_code",
                    "item_value": "000001.SZ",
                    "request_start_date": "2024-01-01",
                    "request_end_date": "2024-01-31",
                    "data_min_time": "2024-01-01",
                    "data_max_time": "2024-01-31",
                    "rows": 1,
                    "status": "success",
                    "error": "",
                    "duration_ms": 1,
                    "request_hash": "a",
                    "snapshot_id": "snapshot-a",
                    "params_json": "{}",
                    "fields_json": "[]",
                },
                {
                    "called_at": "2024-01-31T00:00:01+00:00",
                    "api_name": "income",
                    "table": "income",
                    "kind": "fundamental",
                    "item_key": "ts_code",
                    "item_value": "000002.SZ",
                    "request_start_date": "2024-01-01",
                    "request_end_date": "2024-01-31",
                    "data_min_time": "2024-01-01",
                    "data_max_time": "2024-01-31",
                    "rows": 1,
                    "status": "empty",
                    "error": "",
                    "duration_ms": 1,
                    "request_hash": "b",
                    "snapshot_id": "",
                    "params_json": "{}",
                    "fields_json": "[]",
                },
            ]
        ),
        mode="overwrite",
    )

    report = manager.scan_tushare_updates(
        (TushareTableUpdateSpec(table="income", kind="fundamental"),),
        start_date="2024-01-01",
        end_date="2024-01-31",
    )

    assert report.plans[0].status == "up_to_date"
    assert report.jobs == ()
    state = manager.ensure_tushare_call_state()
    assert state["latest_date"].to_list() == [date(2024, 1, 31), date(2024, 1, 31)]


def test_preview_fundamental_uses_state_latest_without_local_table_scan(
    tmp_path,
    monkeypatch,
) -> None:
    manager = manager_with_client(tmp_path, object())
    seed_refs(manager)
    manager.lake.write(
        "tushare",
        "__api_call_state",
        pl.DataFrame(
            {
                "table": ["income"],
                "kind": ["fundamental"],
                "item_key": ["table"],
                "item_value": ["income"],
                "latest_date": ["2024-01-31"],
                "last_update_date": ["2024-01-31"],
                "status": ["success"],
                "rows": [2],
                "snapshot_id": ["snapshot"],
                "request_hash": ["a"],
                "updated_at": ["2024-01-31T00:00:00+00:00"],
            }
        ),
        mode="overwrite",
    )

    def fail_per_asset_scan(*_args, **_kwargs):
        raise AssertionError("preview should not scan latest dates per asset")

    monkeypatch.setattr(
        DataLakeManager,
        "_latest_local_dates_by_asset",
        fail_per_asset_scan,
    )

    report = manager.preview_tushare_updates(
        (TushareTableUpdateSpec(table="income", kind="fundamental"),),
        start_date="2024-01-01",
        end_date="2024-02-29",
    )

    assert report.jobs == ()
    assert report.plans[0].status == "pending"
    assert report.plans[0].last_update_date is not None
    assert report.plans[0].last_update_date.isoformat() == "2024-01-31"
    assert report.plans[0].effective_start is not None
    assert report.plans[0].effective_start.isoformat() == "2024-01-31"
    assert report.plans[0].estimated_job_count == 2


def test_preview_uses_call_state_without_reading_full_call_log(
    tmp_path,
    monkeypatch,
) -> None:
    manager = manager_with_client(tmp_path, object())
    seed_refs(manager)
    manager.lake.write(
        "tushare",
        "__api_call_state",
        pl.DataFrame(
            {
                "table": ["daily"],
                "kind": ["price"],
                "item_key": ["trade_date"],
                "item_value": ["20240101"],
                "latest_date": ["2024-01-01"],
                "last_update_date": ["2024-01-01"],
                "status": ["success"],
                "rows": [1],
                "snapshot_id": ["snapshot"],
                "request_hash": ["a"],
                "updated_at": ["2024-01-01T00:00:00+00:00"],
            }
        ),
        mode="overwrite",
    )

    def fail_log_read(*_args, **_kwargs):
        raise AssertionError("preview should not read the full API call log")

    monkeypatch.setattr(manager, "tushare_api_call_log", fail_log_read)

    report = manager.preview_tushare_updates(
        (TushareTableUpdateSpec(table="daily", kind="price"),),
        start_date="2024-01-01",
        end_date="2024-01-03",
    )

    assert [job.item_value for job in report.jobs] == ["20240103"]


class Client:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def daily(self, **kwargs):
        self.calls.append(kwargs)
        if kwargs["trade_date"] == "20240103":
            return pd.DataFrame()
        return pd.DataFrame(
            {
                "trade_date": [kwargs["trade_date"]],
                "ts_code": ["000001.SZ"],
                "close": [10.0],
            }
        )


class IncomeClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def income(self, **kwargs):
        self.calls.append(kwargs)
        return pd.DataFrame(
            {
                "f_ann_date": [kwargs["start_date"]],
                "end_date": ["20231231"],
                "ts_code": [kwargs["ts_code"]],
                "revenue": [1.0],
            }
        )


class StockBasicClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def stock_basic(self, **kwargs):
        self.calls.append(kwargs)
        status = kwargs["list_status"]
        return pd.DataFrame(
            {
                "ts_code": [f"00000{len(self.calls)}.SZ"],
                "name": [status],
                "list_status": [status],
            }
        )


class TradeCalClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def trade_cal(self, **kwargs):
        self.calls.append(kwargs)
        return pd.DataFrame(
            {
                "cal_date": ["20000103"],
                "is_open": [1],
            }
        )


def test_update_tushare_trading_calendar_logs_string_request_dates(tmp_path) -> None:
    client = TradeCalClient()
    manager = manager_with_client(tmp_path, client)

    ref = manager.update_tushare_trading_calendar(start_date="2000-01-01")

    log = manager.tushare_api_call_log()
    data = manager.lake.read("tushare", "trade_cal")
    assert ref.dataset == "trade_cal"
    assert client.calls == [{"start_date": "20000101"}]
    assert str(log["request_start_date"].to_list()[0]) == "2000-01-01"
    assert str(data["time"].to_list()[0]) == "2000-01-03"


def test_update_tushare_stock_basic_fetches_all_list_statuses(tmp_path) -> None:
    client = StockBasicClient()
    manager = manager_with_client(tmp_path, client)

    ref = manager.update_tushare_stock_basic()

    data = manager.lake.read("tushare", "stock_basic")
    log = manager.tushare_api_call_log()
    assert ref.dataset == "stock_basic"
    assert [call["list_status"] for call in client.calls] == ["L", "D", "P"]
    assert sorted(data["list_status"].to_list()) == ["D", "L", "P"]
    assert log.filter(log["table"] == "stock_basic").height == 3


def test_execute_logs_every_tushare_api_call(tmp_path) -> None:
    client = Client()
    manager = manager_with_client(tmp_path, client)
    seed_refs(manager)
    report = manager.scan_tushare_updates(
        (TushareTableUpdateSpec(table="daily", kind="price"),),
        start_date="2024-01-01",
        end_date="2024-01-03",
    )
    events: list[dict[str, object]] = []

    refs = manager.execute_tushare_update_report(
        report,
        progress=events.append,
        continue_on_error=True,
    )

    log = manager.tushare_api_call_log()
    state = manager.ensure_tushare_call_state()
    assert len(client.calls) == 2
    assert log["status"].to_list() == ["success", "empty"]
    assert state["item_value"].to_list() == ["20240101", "20240103"]
    assert state["latest_date"].to_list() == [date(2024, 1, 1), date(2024, 1, 3)]
    update_date = log["update_date"].to_list()[0]
    assert (
        tmp_path
        / "tushare"
        / "__api_call_log"
        / f"year={update_date.year:04d}"
        / f"month={update_date.month:02d}"
        / f"day={update_date.day:02d}"
    ).exists()
    assert len(refs) == 1
    assert events[-1]["completed"] == 2
    assert events[-1]["total"] == 2


def test_tushare_price_update_writes_day_partition(tmp_path) -> None:
    client = Client()
    manager = manager_with_client(tmp_path, client)
    seed_refs(manager)
    report = manager.scan_tushare_updates(
        (TushareTableUpdateSpec(table="daily", kind="price"),),
        start_date="2024-01-01",
        end_date="2024-01-01",
    )

    refs = manager.execute_tushare_update_report(report)

    assert report.jobs[0].partition_column == "time"
    assert report.jobs[0].partition_granularity == "day"
    assert (
        tmp_path
        / "tushare"
        / "daily"
        / "year=2024"
        / "month=01"
        / "day=01"
        / "snapshots"
        / refs[0].snapshot_id
        / "data.parquet"
    ).exists()


def test_tushare_fundamental_update_writes_announcement_year_partition(
    tmp_path,
) -> None:
    client = IncomeClient()
    manager = manager_with_client(tmp_path, client)
    seed_refs(manager)
    report = manager.scan_tushare_updates(
        (TushareTableUpdateSpec(table="income", kind="fundamental"),),
        start_date="2024-04-30",
        end_date="2024-04-30",
    )

    refs = manager.execute_tushare_update_report(report)

    assert report.jobs[0].partition_column == "time"
    assert report.jobs[0].partition_granularity == "year"
    assert (
        tmp_path
        / "tushare"
        / "income"
        / "year=2024"
        / "snapshots"
        / refs[0].snapshot_id
        / "data.parquet"
    ).exists()
    data = manager.lake.read("tushare", "income")
    assert data.columns == ["time", "end_date", "asset_id", "revenue"]


def test_tushare_update_table_registry_builds_specs(tmp_path) -> None:
    manager = manager_with_client(tmp_path, object())

    manager.register_tushare_update_table("daily")
    manager.register_tushare_update_table("income")

    tables = manager.tushare_update_tables()
    specs = manager.tushare_update_specs()
    assert tables["table"].to_list() == ["daily", "income"]
    assert [(spec.table, spec.kind) for spec in specs] == [
        ("daily", "price"),
        ("income", "fundamental"),
    ]

    manager.remove_tushare_update_table("daily")

    assert manager.tushare_update_tables()["table"].to_list() == ["income"]
    assert [spec.table for spec in manager.tushare_update_specs()] == ["income"]


def test_scan_orders_price_jobs_before_fundamental_jobs(tmp_path) -> None:
    manager = manager_with_client(tmp_path, object())
    seed_refs(manager)

    report = manager.scan_tushare_updates(
        (
            TushareTableUpdateSpec(table="income", kind="fundamental"),
            TushareTableUpdateSpec(table="daily", kind="price"),
        ),
        start_date="2024-01-01",
        end_date="2024-01-01",
    )

    assert [plan.table for plan in report.plans] == ["daily", "income"]
    assert report.jobs[0].table == "daily"
    assert {job.table for job in report.jobs[1:]} == {"income"}
