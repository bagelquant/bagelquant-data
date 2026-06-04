from __future__ import annotations

from bagelquant_data.gui.config import GuiConfig, SourceConfig, TableConfig
from bagelquant_data.gui.orchestration import (
    enabled_update_tables,
    run_all_table_updates,
    run_table_update,
    token_available,
    token_from_config,
)


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
                tables=[
                    TableConfig(source="tushare", name="stock_basic", kind="general"),
                    TableConfig(source="tushare", name="daily", kind="price"),
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

    assert enabled_update_tables(config)[0].name == "stock_basic"
    assert snapshots == ("stock-basic", "snapshot")
    assert manager.stock_basic_calls == 1
    assert manager.calls == [
        {
            "table": "daily",
            "kind": "price",
            "start_date": "2020-01-01",
            "end_date": None,
            "workers": 6,
        }
    ]


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
        self.stock_basic_calls = 0

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
