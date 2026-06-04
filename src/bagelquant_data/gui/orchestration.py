"""Testable orchestration helpers for the Streamlit GUI."""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from typing import Any

from bagelquant_data.datasource import DataSourceRegistry, TushareDataSource
from bagelquant_data.datasource.base import DataRequest
from bagelquant_data.gui.config import GuiConfig, SourceConfig, TableConfig
from bagelquant_data.lake import DataLakeManager, TushareUpdateReport
from bagelquant_data.lake.snapshot import SnapshotRef

ProgressCallback = Callable[[Mapping[str, Any]], None]


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


def token_from_config(
    config: GuiConfig,
    *,
    streamlit_secrets: Any | None = None,
) -> str | None:
    """Resolve a GUI token source, preferring persisted config."""

    for source in config.sources:
        if source.provider == "tushare" and source.token:
            return source.token
    return token_from_environment(streamlit_secrets)


def build_registry(*, tushare_token: str | None = None) -> DataSourceRegistry:
    """Build a registry for configured GUI providers."""

    registry = DataSourceRegistry()
    if tushare_token is not None:
        registry.register(TushareDataSource(token=tushare_token))
    return registry


def run_table_update(
    manager: DataLakeManager,
    table: TableConfig,
    *,
    start_date: str = "2000-01-01",
    end_date: str | None = None,
    workers: int = 4,
    progress: ProgressCallback | None = None,
) -> tuple[SnapshotRef, ...]:
    """Run the configured provider update for a table."""

    if table.source != "tushare":
        raise ValueError(f"Unsupported GUI update source: {table.source}")
    if table.kind == "general":
        if table.name == "stock_basic":
            return (manager.update_tushare_stock_basic(),)
        return (
            manager.update(
                "tushare",
                DataRequest(dataset=table.name),
                mode=table.update_mode,
            ),
        )
    kwargs: dict[str, Any] = {
        "kind": table.kind,
        "start_date": start_date,
        "end_date": end_date,
        "workers": workers,
    }
    if progress is not None:
        kwargs["progress"] = progress
    return manager.update_tushare_all(table.name, **kwargs)


def enabled_update_tables(config: GuiConfig) -> tuple[TableConfig, ...]:
    """Return enabled tables in source order, with each first table first."""

    tables: list[TableConfig] = []
    for source in config.sources:
        if not source.enabled:
            continue
        source_tables = tuple(table for table in source.tables if table.enabled)
        if source.provider == "tushare" and not any(
            table.name == "stock_basic" for table in source_tables
        ):
            tables.append(
                TableConfig(source=source.name, name="stock_basic", kind="general")
            )
        tables.extend(source_tables)
    return tuple(tables)


def build_update_report(
    manager: DataLakeManager,
    config: GuiConfig,
) -> TushareUpdateReport:
    """Scan enabled GUI tables and return a dry-run update report."""

    tables = enabled_update_tables(config)
    return manager.scan_tushare_updates(
        [table.name for table in tables],
        kinds={table.name: table.kind for table in tables},
        start_date=config.update_start_date,
        end_date=config.update_end_date,
    )


def run_update_report(
    manager: DataLakeManager,
    report: TushareUpdateReport,
    *,
    workers: int = 4,
    progress: ProgressCallback | None = None,
) -> tuple[SnapshotRef, ...]:
    """Execute a confirmed update report."""

    return manager.execute_tushare_update_report(
        report,
        workers=workers,
        progress=progress,
    )


def run_all_table_updates(
    manager: DataLakeManager,
    config: GuiConfig,
    *,
    progress: ProgressCallback | None = None,
) -> tuple[SnapshotRef, ...]:
    """Run all enabled configured table updates manually."""

    report = build_update_report(manager, config)
    return run_update_report(
        manager,
        report,
        workers=config.update_workers,
        progress=progress,
    )


def default_tushare_source() -> SourceConfig:
    """Return the default Tushare GUI source."""

    return SourceConfig(
        name="tushare",
        provider="tushare",
        tables=[TableConfig(source="tushare", name="stock_basic", kind="general")],
    )
