from __future__ import annotations

from datetime import UTC, date, datetime

from bagelquant_data.gui.config import (
    GuiConfig,
    SourceConfig,
    TableConfig,
    TradingCalendarConfig,
    UniverseConfig,
)
from bagelquant_data.gui.orchestration import (
    build_update_report,
    enabled_update_tables,
    run_all_table_updates,
    run_reference_updates,
    run_table_update,
    run_update_report,
    token_available,
    token_from_config,
    update_binding_errors,
)
from bagelquant_data.lake import TushareUpdatePlan, TushareUpdateReport


def test_token_available_checks_env_without_exposing_value() -> None:
    assert token_available(environ={"TUSHARE_TOKEN": "secret"})
    assert not token_available(environ={})


def test_token_from_config_prefers_persisted_source_token() -> None:
    config = GuiConfig(
        sources=[SourceConfig(name="tushare", token="configured-token")]
    )

    assert token_from_config(config) == "configured-token"


def test_run_table_update_delegates_to_tushare_all_update() -> None:
    manager = FakeManager()
    table = TableConfig(
        source="tushare",
        name="income_vip",
        kind="fundamental_vip",
    )

    snapshots = run_table_update(
        manager,  # type: ignore[arg-type]
        table,
        start_date="2024-01-01",
        end_date="2024-12-31",
        workers=8,
    )

    assert snapshots == ("snapshot",)
    assert manager.calls == [
        {
            "table": "income_vip",
            "kind": "fundamental_vip",
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "workers": 8,
        }
    ]


def test_run_table_update_handles_general_stock_basic() -> None:
    manager = FakeManager()
    table = TableConfig(source="tushare", name="stock_basic", kind="general")

    snapshots = run_table_update(manager, table)  # type: ignore[arg-type]

    assert snapshots == ("stock-basic",)
    assert manager.stock_basic_calls == 1


def test_run_all_table_updates_uses_enabled_tables_in_source_order() -> None:
    manager = FakeManager()
    config = GuiConfig(
        update_start_date="2020-01-01",
        update_end_date="2024-12-31",
        update_workers=6,
        sources=[
            SourceConfig(
                name="tushare",
                universes=[
                    UniverseConfig(
                        source="tushare",
                        table="stock_basic",
                    )
                ],
                trading_calendars=[
                    TradingCalendarConfig(
                        source="tushare",
                        table="trade_cal",
                    )
                ],
                tables=[
                    TableConfig(
                        source="tushare",
                        name="daily",
                        kind="price",
                        universe_for_update="stock_basic",
                        trading_calendar="trade_cal",
                    ),
                    TableConfig(
                        source="tushare",
                        name="disabled",
                        kind="price",
                        enabled=False,
                    ),
                ],
            )
        ],
    )

    snapshots = run_all_table_updates(manager, config)  # type: ignore[arg-type]

    assert enabled_update_tables(config)[0].name == "daily"
    assert snapshots == ("stock-basic", "snapshot")
    assert manager.scan_calls == [
        {
            "specs": [("daily", "price")],
            "start_date": "2020-01-01",
            "end_date": "2024-12-31",
            "trading_calendars": {"daily": "trade_cal"},
        }
    ]
    assert manager.report_workers == 6


def test_build_update_report_uses_enabled_tables() -> None:
    manager = FakeManager()
    config = GuiConfig(
        update_start_date="2020-01-01",
        update_end_date="2024-12-31",
        sources=[
            SourceConfig(
                name="tushare",
                universes=[
                    UniverseConfig(
                        source="tushare",
                        table="stock_basic",
                    )
                ],
                trading_calendars=[
                    TradingCalendarConfig(
                        source="tushare",
                        table="trade_cal",
                    )
                ],
                tables=[
                    TableConfig(
                        source="tushare",
                        name="daily",
                        kind="price",
                        universe_for_update="stock_basic",
                        trading_calendar="trade_cal",
                    ),
                ],
            )
        ],
    )

    report = build_update_report(manager, config)  # type: ignore[arg-type]

    assert report is manager.report
    assert manager.scan_calls[-1]["specs"] == [("daily", "price")]


def test_single_trading_calendar_is_used_as_default() -> None:
    manager = FakeManager()
    config = GuiConfig(
        update_start_date="2020-01-01",
        update_end_date="2024-12-31",
        sources=[
            SourceConfig(
                name="tushare",
                universes=[
                    UniverseConfig(source="tushare", table="stock_basic"),
                ],
                trading_calendars=[
                    TradingCalendarConfig(source="tushare", table="trade_cal"),
                ],
                tables=[
                    TableConfig(
                        source="tushare",
                        name="daily",
                        kind="price",
                        universe_for_update="stock_basic",
                    )
                ],
            )
        ],
    )

    assert update_binding_errors(config) == ()

    build_update_report(manager, config)  # type: ignore[arg-type]

    assert manager.scan_calls[-1]["trading_calendars"] == {"daily": "trade_cal"}


def test_multiple_trading_calendars_require_table_binding() -> None:
    config = GuiConfig(
        sources=[
            SourceConfig(
                name="tushare",
                universes=[
                    UniverseConfig(source="tushare", table="stock_basic"),
                ],
                trading_calendars=[
                    TradingCalendarConfig(source="tushare", table="trade_cal"),
                    TradingCalendarConfig(source="tushare", table="fut_trade_cal"),
                ],
                tables=[
                    TableConfig(
                        source="tushare",
                        name="daily",
                        kind="price",
                        universe_for_update="stock_basic",
                    )
                ],
            )
        ],
    )

    assert update_binding_errors(config) == (
        "tushare/daily is missing an enabled trading calendar",
    )


def test_missing_non_general_bindings_are_reported() -> None:
    config = GuiConfig(
        sources=[
            SourceConfig(
                name="tushare",
                tables=[TableConfig(source="tushare", name="daily", kind="price")],
            )
        ]
    )

    assert update_binding_errors(config) == (
        "tushare/daily is missing an enabled universe",
        "tushare/daily is missing an enabled trading calendar",
    )


def test_run_reference_updates_updates_universes_and_calendars() -> None:
    manager = FakeManager()
    config = GuiConfig(
        update_start_date="2020-01-01",
        update_end_date="2024-12-31",
        sources=[
            SourceConfig(
                name="tushare",
                universes=[
                    UniverseConfig(
                        source="tushare",
                        table="stock_basic",
                    )
                ],
                trading_calendars=[
                    TradingCalendarConfig(
                        source="tushare",
                        table="trade_cal",
                    )
                ],
            )
        ],
    )

    snapshots = run_reference_updates(manager, config)  # type: ignore[arg-type]

    assert snapshots == ("stock-basic", "trade-cal")
    assert manager.reference_calls == [
        ("universe", "stock_basic"),
        ("calendar", "trade_cal", "2020-01-01", "2024-12-31", {}),
    ]


def test_run_update_report_executes_confirmed_report() -> None:
    manager = FakeManager()

    snapshots = run_update_report(
        manager,  # type: ignore[arg-type]
        manager.report,
        workers=3,
    )

    assert snapshots == ("stock-basic", "snapshot")
    assert manager.executed_reports == [manager.report]


def test_run_table_update_passes_progress_callback() -> None:
    manager = FakeManager()
    table = TableConfig(source="tushare", name="daily", kind="price")

    def progress(_event):
        pass

    run_table_update(manager, table, progress=progress)  # type: ignore[arg-type]

    assert manager.calls[-1]["progress"] is progress


def test_gui_config_defaults_update_workers_to_eight() -> None:
    assert GuiConfig().update_workers == 8


class FakeManager:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self.scan_calls: list[dict[str, object]] = []
        self.executed_reports: list[TushareUpdateReport] = []
        self.reference_calls: list[tuple[object, ...]] = []
        self.stock_basic_calls = 0
        self.report_workers = 0
        self.report = TushareUpdateReport(
            generated_at=datetime.now(UTC),
            source="tushare",
            requested_start=date(2020, 1, 1),
            requested_end=date(2024, 12, 31),
            plans=(
                TushareUpdatePlan(
                    table="daily",
                    kind="price",
                    requested_start=date(2020, 1, 1),
                    requested_end=date(2024, 12, 31),
                    effective_start=date(2024, 1, 1),
                    pending_items=("2024-01-01",),
                    reason="missing local trade_date partitions",
                    estimated_job_count=1,
                    status="pending",
                ),
            ),
            jobs=(),
        )

    def update_tushare_stock_basic(self):
        self.stock_basic_calls += 1
        return "stock-basic"

    def update_tushare_all(
        self,
        table: str,
        *,
        kind: str,
        start_date: str,
        end_date: str | None,
        workers: int,
        progress=None,
    ):
        call = {
            "table": table,
            "kind": kind,
            "start_date": start_date,
            "end_date": end_date,
            "workers": workers,
        }
        if progress is not None:
            call["progress"] = progress
        self.calls.append(call)
        return ("snapshot",)

    def update(self, *args, **kwargs):
        return "general"

    def scan_tushare_updates(self, *, specs, start_date, end_date):
        self.scan_calls.append(
            {
                "specs": [(spec.table, spec.kind) for spec in specs],
                "start_date": start_date,
                "end_date": end_date,
                "trading_calendars": {
                    spec.table: (
                        spec.trading_calendar.table
                        if spec.trading_calendar is not None
                        else None
                    )
                    for spec in specs
                },
            }
        )
        return self.report

    def update_tushare_universe(self, table):
        self.reference_calls.append(("universe", table))
        return "stock-basic"

    def update_tushare_trading_calendar(
        self,
        table,
        *,
        start_date,
        end_date,
        filters,
    ):
        self.reference_calls.append(
            ("calendar", table, start_date, end_date, filters)
        )
        return "trade-cal"

    def execute_tushare_update_report(
        self,
        report,
        *,
        workers,
        progress=None,
    ):
        self.executed_reports.append(report)
        self.report_workers = workers
        return ("stock-basic", "snapshot")
