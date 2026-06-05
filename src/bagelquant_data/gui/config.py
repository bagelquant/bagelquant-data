"""YAML-backed configuration for the data lake GUI."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal, cast

import yaml  # type: ignore[reportMissingTypeStubs]

from bagelquant_data.lake.manager import TushareTableKind

DEFAULT_CONFIG_PATH = Path(".bagelquant-data-gui.yaml")
DEFAULT_LAKE_ROOT = Path(".bagelquant-data-lake")
UpdateMode = Literal["append", "overwrite"]


@dataclass(slots=True)
class UniverseConfig:
    """Configured source universe reference table."""

    source: str
    table: str
    kind: TushareTableKind = "general"
    code_column: str = "ts_code"
    enabled: bool = True


@dataclass(slots=True)
class TradingCalendarConfig:
    """Configured source trading calendar reference table."""

    source: str
    table: str
    kind: TushareTableKind = "general"
    date_column: str = "cal_date"
    open_column: str = "is_open"
    filters: dict[str, str] = field(default_factory=dict)
    enabled: bool = True


@dataclass(slots=True)
class TableConfig:
    """Configured source table update target."""

    source: str
    name: str
    kind: TushareTableKind = "general"
    end_date: str | None = None
    update_mode: UpdateMode = "overwrite"
    fields: list[str] = field(default_factory=list)
    enabled: bool = True
    universe: str | None = None
    trading_calendar: str | None = None


@dataclass(slots=True)
class SourceConfig:
    """Configured provider source."""

    name: str
    provider: str = "tushare"
    token: str | None = None
    enabled: bool = True
    universes: list[UniverseConfig] = field(default_factory=list)
    trading_calendars: list[TradingCalendarConfig] = field(default_factory=list)
    tables: list[TableConfig] = field(default_factory=list)


@dataclass(slots=True)
class GuiConfig:
    """Persisted non-secret GUI configuration."""

    lake_root: str = str(DEFAULT_LAKE_ROOT)
    update_start_date: str = "2000-01-01"
    update_end_date: str | None = None
    update_workers: int = 8
    sources: list[SourceConfig] = field(default_factory=list)

    def source_names(self) -> tuple[str, ...]:
        """Return configured source names."""

        return tuple(source.name for source in self.sources)

    def tables_for(self, source: str) -> tuple[TableConfig, ...]:
        """Return configured tables for a source."""

        for source_config in self.sources:
            if source_config.name == source:
                return tuple(source_config.tables)
        return ()


def load_config(path: str | Path = DEFAULT_CONFIG_PATH) -> GuiConfig:
    """Load a GUI config file, returning defaults when it does not exist."""

    config_path = Path(path)
    if not config_path.exists():
        return GuiConfig()
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if payload is None:
        return GuiConfig()
    if not isinstance(payload, Mapping):
        raise ValueError("GUI config must be a YAML mapping")
    return config_from_mapping(cast(Mapping[str, Any], payload))


def save_config(config: GuiConfig, path: str | Path = DEFAULT_CONFIG_PATH) -> None:
    """Save a GUI config file."""

    payload = asdict(config)
    config_path = Path(path)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        yaml.safe_dump(payload, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )


def config_from_mapping(payload: Mapping[str, Any]) -> GuiConfig:
    """Build config dataclasses from a plain mapping."""

    return GuiConfig(
        lake_root=str(payload.get("lake_root", DEFAULT_LAKE_ROOT)),
        update_start_date=str(payload.get("update_start_date", "2000-01-01")),
        update_end_date=_optional_str(payload.get("update_end_date")),
        update_workers=max(1, int(payload.get("update_workers", 8))),
        sources=[
            _normalize_source(_source_from_mapping(item))
            for item in _mapping_list(payload.get("sources", []), "sources")
        ],
    )


def _source_from_mapping(payload: Mapping[str, Any]) -> SourceConfig:
    return SourceConfig(
        name=str(payload.get("name", "tushare")),
        provider=str(payload.get("provider", "tushare")),
        token=_optional_str(payload.get("token")),
        enabled=bool(payload.get("enabled", True)),
        universes=[
            _universe_from_mapping(item)
            for item in _mapping_list(
                payload.get("universes", []),
                "sources[].universes",
            )
        ],
        trading_calendars=[
            _trading_calendar_from_mapping(item)
            for item in _mapping_list(
                payload.get("trading_calendars", []),
                "sources[].trading_calendars",
            )
        ],
        tables=[
            _table_from_mapping(item)
            for item in _mapping_list(payload.get("tables", []), "sources[].tables")
        ],
    )


def _universe_from_mapping(payload: Mapping[str, Any]) -> UniverseConfig:
    return UniverseConfig(
        source=str(payload.get("source", "tushare")),
        table=str(payload.get("table", payload.get("name", "stock_basic"))),
        kind=_table_kind(payload.get("kind", "general")),
        code_column=str(payload.get("code_column", "ts_code")),
        enabled=bool(payload.get("enabled", True)),
    )


def _trading_calendar_from_mapping(payload: Mapping[str, Any]) -> TradingCalendarConfig:
    filters = payload.get("filters", {})
    if not isinstance(filters, Mapping):
        raise ValueError("sources[].trading_calendars[].filters must be a mapping")
    return TradingCalendarConfig(
        source=str(payload.get("source", "tushare")),
        table=str(payload.get("table", payload.get("name", "trade_cal"))),
        kind=_table_kind(payload.get("kind", "general")),
        date_column=str(payload.get("date_column", "cal_date")),
        open_column=str(payload.get("open_column", "is_open")),
        filters={str(key): str(value) for key, value in filters.items()},
        enabled=bool(payload.get("enabled", True)),
    )


def _table_from_mapping(payload: Mapping[str, Any]) -> TableConfig:
    return TableConfig(
        source=str(payload.get("source", "tushare")),
        name=str(payload.get("name", "stock_basic")),
        kind=_table_kind(payload.get("kind", "general")),
        end_date=_optional_str(payload.get("end_date")),
        update_mode=_update_mode(payload.get("update_mode", "overwrite")),
        fields=[str(field) for field in payload.get("fields", [])],
        enabled=bool(payload.get("enabled", True)),
        universe=_optional_str(payload.get("universe")),
        trading_calendar=_optional_str(payload.get("trading_calendar")),
    )


def _mapping_list(value: Any, name: str) -> list[Mapping[str, Any]]:
    if not isinstance(value, list):
        raise ValueError(f"{name} must be a list")
    if not all(isinstance(item, Mapping) for item in value):
        raise ValueError(f"{name} must contain mappings")
    return cast(list[Mapping[str, Any]], value)


def _table_kind(value: Any) -> TushareTableKind:
    text = str(value)
    if text not in {"general", "price", "fundamental", "fundamental_vip"}:
        raise ValueError(f"Invalid table kind: {text}")
    return cast(TushareTableKind, text)


def _update_mode(value: Any) -> UpdateMode:
    text = str(value)
    if text not in {"append", "overwrite"}:
        raise ValueError(f"Invalid update mode: {text}")
    return cast(UpdateMode, text)


def _optional_str(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return str(value)


def _normalize_source(source: SourceConfig) -> SourceConfig:
    if source.provider != "tushare":
        return source
    source.universes = _with_default_stock_universe(
        source.name,
        source.universes,
        source.tables,
    )
    source.trading_calendars = _with_default_trading_calendar(
        source.name,
        source.trading_calendars,
    )
    source.tables = [
        table
        for table in source.tables
        if not (
            table.source == source.name
            and table.name in {"stock_basic", "trade_cal"}
        )
    ]
    return source


def _with_default_stock_universe(
    source_name: str,
    universes: list[UniverseConfig],
    tables: list[TableConfig],
) -> list[UniverseConfig]:
    stock_basic = UniverseConfig(
        source=source_name,
        table="stock_basic",
        kind="general",
        code_column="ts_code",
        enabled=True,
    )
    normalized = [item for item in universes if item.table != "stock_basic"]
    if any(
        table.source == source_name and table.name == "stock_basic"
        for table in tables
    ):
        return [stock_basic, *normalized]
    if any(item.table == "stock_basic" for item in universes):
        return universes
    return [stock_basic, *normalized]


def _with_default_trading_calendar(
    source_name: str,
    calendars: list[TradingCalendarConfig],
) -> list[TradingCalendarConfig]:
    if any(item.table == "trade_cal" for item in calendars):
        return calendars
    return [
        TradingCalendarConfig(
            source=source_name,
            table="trade_cal",
            kind="general",
            date_column="cal_date",
            open_column="is_open",
            enabled=True,
        ),
        *calendars,
    ]
