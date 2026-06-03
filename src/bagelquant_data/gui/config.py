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
class TableConfig:
    """Configured source table update target."""

    source: str
    name: str
    kind: TushareTableKind = "general"
    end_date: str | None = None
    update_mode: UpdateMode = "overwrite"
    fields: list[str] = field(default_factory=list)
    enabled: bool = True


@dataclass(slots=True)
class SourceConfig:
    """Configured provider source."""

    name: str
    provider: str = "tushare"
    token: str | None = None
    enabled: bool = True
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
        tables=[
            _table_from_mapping(item)
            for item in _mapping_list(payload.get("tables", []), "sources[].tables")
        ],
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
    source.tables = _with_required_stock_basic(source.name, source.tables)
    return source


def _with_required_stock_basic(
    source_name: str,
    tables: list[TableConfig],
) -> list[TableConfig]:
    stock_basic = TableConfig(
        source=source_name,
        name="stock_basic",
        kind="general",
        enabled=True,
    )
    normalized = [
        table
        for table in tables
        if not (table.source == source_name and table.name == "stock_basic")
    ]
    return [stock_basic, *normalized]
