"""Testable orchestration helpers for the Streamlit GUI."""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from typing import Any

from bagelquant_data.datasource import DataSourceRegistry, TushareDataSource
from bagelquant_data.datasource.base import DataRequest
from bagelquant_data.gui.config import (
    GuiConfig,
    SourceConfig,
    TableConfig,
    TradingCalendarConfig,
    UniverseConfig,
)
from bagelquant_data.lake import (
    DataLakeManager,
    TushareTableUpdateSpec,
    TushareTradingCalendarRef,
    TushareUniverseRef,
    TushareUpdateReport,
)
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
    """Return enabled non-reference tables in source order."""

    tables: list[TableConfig] = []
    for source in config.sources:
        if not source.enabled:
            continue
        reference_tables = {
            item.table for item in source.universes if item.enabled
        }.union(item.table for item in source.trading_calendars if item.enabled)
        tables.extend(
            table
            for table in source.tables
            if table.enabled and table.name not in reference_tables
        )
    return tuple(tables)


def update_binding_errors(config: GuiConfig) -> tuple[str, ...]:
    """Return validation errors for enabled non-general table bindings."""

    errors: list[str] = []
    source_by_name = {source.name: source for source in config.sources}
    for table in enabled_update_tables(config):
        if table.kind == "general":
            continue
        source = source_by_name.get(table.source)
        if source is None:
            errors.append(f"{table.source}/{table.name} source is not configured")
            continue
        universe_tables = {item.table for item in source.universes if item.enabled}
        calendars = [item for item in source.trading_calendars if item.enabled]
        calendar_tables = {item.table for item in calendars}
        if not table.universe or table.universe not in universe_tables:
            errors.append(f"{table.source}/{table.name} is missing an enabled universe")
        if table.trading_calendar and table.trading_calendar not in calendar_tables:
            errors.append(
                f"{table.source}/{table.name} is missing an enabled trading calendar"
            )
        elif not table.trading_calendar and len(calendars) != 1:
            errors.append(
                f"{table.source}/{table.name} is missing an enabled trading calendar"
            )
    return tuple(errors)


def build_update_report(
    manager: DataLakeManager,
    config: GuiConfig,
) -> TushareUpdateReport:
    """Scan enabled GUI tables and return a dry-run update report."""

    errors = update_binding_errors(config)
    if errors:
        raise ValueError("; ".join(errors))
    tables = enabled_update_tables(config)
    universes = _table_universe_refs(config, tables)
    calendars = _table_calendar_refs(config, tables)
    return manager.scan_tushare_updates(
        specs=tuple(
            TushareTableUpdateSpec(
                table=table.name,
                kind=table.kind,
                universe=universes.get(table.name),
                trading_calendar=calendars.get(table.name),
            )
            for table in tables
        ),
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


def run_reference_updates(
    manager: DataLakeManager,
    config: GuiConfig,
) -> tuple[SnapshotRef, ...]:
    """Update enabled universe and trading calendar reference resources."""

    refs: list[SnapshotRef] = []
    for source in config.sources:
        if not source.enabled or source.provider != "tushare":
            continue
        for universe in source.universes:
            if universe.enabled:
                refs.append(manager.update_tushare_universe(universe.table))
        for calendar in source.trading_calendars:
            if calendar.enabled:
                refs.append(
                    manager.update_tushare_trading_calendar(
                        calendar.table,
                        start_date=config.update_start_date,
                        end_date=config.update_end_date,
                        filters=calendar.filters,
                    )
                )
    return tuple(refs)


def default_tushare_source() -> SourceConfig:
    """Return the default Tushare GUI source."""

    return SourceConfig(
        name="tushare",
        provider="tushare",
        universes=[
            UniverseConfig(
                source="tushare",
                table="stock_basic",
                kind="general",
                code_column="ts_code",
            )
        ],
        trading_calendars=[
            TradingCalendarConfig(
                source="tushare",
                table="trade_cal",
                kind="general",
                date_column="cal_date",
                open_column="is_open",
            )
        ],
        tables=[],
    )


def _table_universe_refs(
    config: GuiConfig,
    tables: tuple[TableConfig, ...],
) -> dict[str, TushareUniverseRef | None]:
    source_by_name = {source.name: source for source in config.sources}
    refs: dict[str, TushareUniverseRef | None] = {}
    for table in tables:
        source = source_by_name.get(table.source)
        universe = _universe_by_name(source, table.universe) if source else None
        if universe is None:
            refs[table.name] = None
        else:
            refs[table.name] = TushareUniverseRef(
                name=universe.table,
                table=universe.table,
                code_column=universe.code_column,
            )
    return refs


def _table_calendar_refs(
    config: GuiConfig,
    tables: tuple[TableConfig, ...],
) -> dict[str, TushareTradingCalendarRef | None]:
    source_by_name = {source.name: source for source in config.sources}
    refs: dict[str, TushareTradingCalendarRef | None] = {}
    for table in tables:
        source = source_by_name.get(table.source)
        calendar = _calendar_for_table(source, table.trading_calendar)
        if calendar is None:
            refs[table.name] = None
        else:
            refs[table.name] = TushareTradingCalendarRef(
                name=calendar.table,
                table=calendar.table,
                date_column=calendar.date_column,
                open_column=calendar.open_column,
                filters=calendar.filters,
            )
    return refs


def _calendar_for_table(
    source: SourceConfig | None,
    name: str | None,
) -> TradingCalendarConfig | None:
    if source is None:
        return None
    if name is not None:
        return _calendar_by_name(source, name)
    enabled = [calendar for calendar in source.trading_calendars if calendar.enabled]
    if len(enabled) == 1:
        return enabled[0]
    return None


def _universe_by_name(
    source: SourceConfig | None,
    name: str | None,
) -> UniverseConfig | None:
    if source is None or name is None:
        return None
    for universe in source.universes:
        if universe.enabled and universe.table == name:
            return universe
    return None


def _calendar_by_name(
    source: SourceConfig | None,
    name: str | None,
) -> TradingCalendarConfig | None:
    if source is None or name is None:
        return None
    for calendar in source.trading_calendars:
        if calendar.enabled and calendar.table == name:
            return calendar
    return None
