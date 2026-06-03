from __future__ import annotations

from datetime import UTC, datetime

from bagelquant_data.gui.config import GuiConfig, PeriodicJobConfig, TableConfig
from bagelquant_data.gui.orchestration import (
    run_due_jobs,
    run_table_update,
    token_available,
)


def test_token_available_checks_env_without_exposing_value() -> None:
    assert token_available(environ={"TUSHARE_TOKEN": "secret"})
    assert not token_available(environ={})


def test_run_table_update_delegates_to_tushare_all_update() -> None:
    manager = FakeManager()
    table = TableConfig(
        source="tushare",
        name="income_vip",
        kind="fundamental_vip",
        start_date="2024-01-01",
        end_date="2024-12-31",
        workers=8,
    )

    snapshots = run_table_update(manager, table)  # type: ignore[arg-type]

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


def test_run_due_jobs_updates_due_job_timestamps() -> None:
    manager = FakeManager()
    config = GuiConfig(
        periodic_jobs=[
            PeriodicJobConfig(
                name="daily",
                source="tushare",
                table="daily",
                last_run_at="2024-01-01T00:00:00+00:00",
            ),
            PeriodicJobConfig(
                name="recent",
                source="tushare",
                table="income",
                kind="fundamental",
                last_run_at="2024-01-02T00:00:00+00:00",
            ),
        ]
    )
    now = datetime(2024, 1, 2, 1, tzinfo=UTC)

    snapshots = run_due_jobs(manager, config, now=now)  # type: ignore[arg-type]

    assert snapshots == ("snapshot",)
    assert len(manager.calls) == 1
    assert manager.calls[0]["table"] == "daily"
    assert config.periodic_jobs[0].last_run_at == now.isoformat()
    assert config.periodic_jobs[1].last_run_at == "2024-01-02T00:00:00+00:00"


class FakeManager:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def update_tushare_all(
        self,
        table: str,
        *,
        kind: str,
        start_date: str,
        end_date: str | None,
        workers: int,
    ):
        self.calls.append(
            {
                "table": table,
                "kind": kind,
                "start_date": start_date,
                "end_date": end_date,
                "workers": workers,
            }
        )
        return ("snapshot",)
