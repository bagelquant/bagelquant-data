"""Testable orchestration helpers for the Streamlit GUI."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

from bagelquant_data.datasource import DataSourceRegistry, TushareDataSource
from bagelquant_data.gui.config import GuiConfig, PeriodicJobConfig, TableConfig
from bagelquant_data.lake import DataLakeManager
from bagelquant_data.lake.snapshot import SnapshotRef


def token_available(
    *,
    environ: dict[str, str] | None = None,
    streamlit_secrets: Any | None = None,
) -> bool:
    """Return whether a Tushare token is available without exposing it."""

    env = environ or os.environ
    if env.get("TUSHARE_TOKEN"):
        return True
    if streamlit_secrets is None:
        return False
    try:
        token = streamlit_secrets.get("TUSHARE_TOKEN")
    except Exception:
        return False
    return bool(token)


def token_from_environment(streamlit_secrets: Any | None = None) -> str | None:
    """Resolve a GUI token source without persisting it."""

    token = os.environ.get("TUSHARE_TOKEN")
    if token:
        return token
    if streamlit_secrets is None:
        return None
    try:
        value = streamlit_secrets.get("TUSHARE_TOKEN")
    except Exception:
        return None
    return str(value) if value else None


def build_registry(*, tushare_token: str | None = None) -> DataSourceRegistry:
    """Build a registry for configured GUI providers."""

    registry = DataSourceRegistry()
    if tushare_token is not None:
        registry.register(TushareDataSource(token=tushare_token))
    return registry


def run_table_update(
    manager: DataLakeManager,
    table: TableConfig,
) -> tuple[SnapshotRef, ...]:
    """Run the configured provider update for a table."""

    if table.source != "tushare":
        raise ValueError(f"Unsupported GUI update source: {table.source}")
    return manager.update_tushare_all(
        table.name,
        kind=table.kind,
        start_date=table.start_date,
        end_date=table.end_date,
        workers=table.workers,
    )


def run_due_jobs(
    manager: DataLakeManager,
    config: GuiConfig,
    *,
    now: datetime | None = None,
) -> tuple[SnapshotRef, ...]:
    """Run configured jobs that are due and update their timestamps."""

    current = now or datetime.now(UTC)
    snapshots: list[SnapshotRef] = []
    for job in config.periodic_jobs:
        if job.due(current):
            snapshots.extend(run_periodic_job(manager, job))
            job.last_run_at = current.isoformat()
    return tuple(snapshots)


def run_periodic_job(
    manager: DataLakeManager,
    job: PeriodicJobConfig,
) -> tuple[SnapshotRef, ...]:
    """Run one configured periodic job."""

    table = TableConfig(
        source=job.source,
        name=job.table,
        kind=job.kind,
        start_date=job.start_date,
        end_date=job.end_date,
        workers=job.workers,
    )
    return run_table_update(manager, table)
