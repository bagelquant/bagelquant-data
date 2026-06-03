"""YAML-backed configuration for the data lake GUI."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, cast

import yaml  # type: ignore[reportMissingTypeStubs]

from bagelquant_data.lake.manager import ScheduleUnit, TushareTableKind

DEFAULT_CONFIG_PATH = Path(".bagelquant-data-gui.yaml")
DEFAULT_LAKE_ROOT = Path(".bagelquant-data-lake")
UpdateMode = Literal["append", "overwrite"]


@dataclass(slots=True)
class TableConfig:
    """Configured source table update target."""

    source: str
    name: str
    kind: TushareTableKind = "price"
    start_date: str = "2000-01-01"
    end_date: str | None = None
    workers: int = 4
    update_mode: UpdateMode = "overwrite"
    fields: list[str] = field(default_factory=list)
    enabled: bool = True


@dataclass(slots=True)
class SourceConfig:
    """Configured provider source."""

    name: str
    provider: str = "tushare"
    enabled: bool = True
    tables: list[TableConfig] = field(default_factory=list)


@dataclass(slots=True)
class UniverseConfig:
    """User-defined source universe."""

    source: str
    name: str
    asset_ids: list[str] = field(default_factory=list)


@dataclass(slots=True)
class PeriodicJobConfig:
    """Configured periodic update job triggered manually by the GUI."""

    name: str
    source: str
    table: str
    kind: TushareTableKind = "price"
    every: int = 1
    unit: ScheduleUnit = "days"
    start_date: str = "2000-01-01"
    end_date: str | None = None
    workers: int = 4
    enabled: bool = True
    last_run_at: str | None = None

    def due(self, now: datetime | None = None) -> bool:
        """Return whether this job should run at ``now``."""

        if not self.enabled:
            return False
        if self.last_run_at is None:
            return True
        current = now or datetime.now(UTC)
        last_run = datetime.fromisoformat(self.last_run_at)
        if last_run.tzinfo is None:
            last_run = last_run.replace(tzinfo=UTC)
        seconds = {
            "minutes": self.every * 60,
            "hours": self.every * 60 * 60,
            "days": self.every * 24 * 60 * 60,
        }[self.unit]
        return (current - last_run).total_seconds() >= seconds


@dataclass(slots=True)
class GuiConfig:
    """Persisted non-secret GUI configuration."""

    lake_root: str = str(DEFAULT_LAKE_ROOT)
    sources: list[SourceConfig] = field(default_factory=list)
    universes: list[UniverseConfig] = field(default_factory=list)
    periodic_jobs: list[PeriodicJobConfig] = field(default_factory=list)

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
    """Save a GUI config file without writing provider secrets."""

    payload = asdict(config)
    _reject_secret_keys(payload)
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
        sources=[
            _source_from_mapping(item)
            for item in _mapping_list(payload.get("sources", []), "sources")
        ],
        universes=[
            _universe_from_mapping(item)
            for item in _mapping_list(payload.get("universes", []), "universes")
        ],
        periodic_jobs=[
            _job_from_mapping(item)
            for item in _mapping_list(payload.get("periodic_jobs", []), "periodic_jobs")
        ],
    )


def _source_from_mapping(payload: Mapping[str, Any]) -> SourceConfig:
    return SourceConfig(
        name=str(payload.get("name", "tushare")),
        provider=str(payload.get("provider", "tushare")),
        enabled=bool(payload.get("enabled", True)),
        tables=[
            _table_from_mapping(item)
            for item in _mapping_list(payload.get("tables", []), "sources[].tables")
        ],
    )


def _table_from_mapping(payload: Mapping[str, Any]) -> TableConfig:
    return TableConfig(
        source=str(payload.get("source", "tushare")),
        name=str(payload.get("name", "daily")),
        kind=_table_kind(payload.get("kind", "price")),
        start_date=str(payload.get("start_date", "2000-01-01")),
        end_date=_optional_str(payload.get("end_date")),
        workers=max(1, int(payload.get("workers", 4))),
        update_mode=_update_mode(payload.get("update_mode", "overwrite")),
        fields=[str(field) for field in payload.get("fields", [])],
        enabled=bool(payload.get("enabled", True)),
    )


def _universe_from_mapping(payload: Mapping[str, Any]) -> UniverseConfig:
    return UniverseConfig(
        source=str(payload.get("source", "tushare")),
        name=str(payload.get("name", "All")),
        asset_ids=[str(asset_id) for asset_id in payload.get("asset_ids", [])],
    )


def _job_from_mapping(payload: Mapping[str, Any]) -> PeriodicJobConfig:
    return PeriodicJobConfig(
        name=str(payload.get("name", "tushare-daily")),
        source=str(payload.get("source", "tushare")),
        table=str(payload.get("table", "daily")),
        kind=_table_kind(payload.get("kind", "price")),
        every=max(1, int(payload.get("every", 1))),
        unit=_schedule_unit(payload.get("unit", "days")),
        start_date=str(payload.get("start_date", "2000-01-01")),
        end_date=_optional_str(payload.get("end_date")),
        workers=max(1, int(payload.get("workers", 4))),
        enabled=bool(payload.get("enabled", True)),
        last_run_at=_optional_str(payload.get("last_run_at")),
    )


def _mapping_list(value: Any, name: str) -> list[Mapping[str, Any]]:
    if not isinstance(value, list):
        raise ValueError(f"{name} must be a list")
    if not all(isinstance(item, Mapping) for item in value):
        raise ValueError(f"{name} must contain mappings")
    return cast(list[Mapping[str, Any]], value)


def _table_kind(value: Any) -> TushareTableKind:
    text = str(value)
    if text not in {"price", "fundamental", "fundamental_vip"}:
        raise ValueError(f"Invalid table kind: {text}")
    return cast(TushareTableKind, text)


def _schedule_unit(value: Any) -> ScheduleUnit:
    text = str(value)
    if text not in {"minutes", "hours", "days"}:
        raise ValueError(f"Invalid schedule unit: {text}")
    return cast(ScheduleUnit, text)


def _update_mode(value: Any) -> UpdateMode:
    text = str(value)
    if text not in {"append", "overwrite"}:
        raise ValueError(f"Invalid update mode: {text}")
    return cast(UpdateMode, text)


def _optional_str(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return str(value)


def _reject_secret_keys(payload: Mapping[str, Any]) -> None:
    for key, value in payload.items():
        lowered = str(key).lower()
        if "token" in lowered or "secret" in lowered or "password" in lowered:
            raise ValueError(f"Refusing to persist secret-like key: {key}")
        if isinstance(value, Mapping):
            _reject_secret_keys(cast(Mapping[str, Any], value))
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, Mapping):
                    _reject_secret_keys(cast(Mapping[str, Any], item))
