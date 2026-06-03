"""Local Tushare table catalog for GUI configuration."""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import cache
from importlib.resources import files
from typing import Any

from bagelquant_data.lake.manager import TushareTableKind


@dataclass(frozen=True, slots=True)
class TushareTableCatalogEntry:
    """One Tushare API entry shown by the GUI."""

    api: str
    name_zh: str
    description_zh: str
    category_zh: str
    default_kind: TushareTableKind
    source_url: str | None = None

    @property
    def label(self) -> str:
        """Return a compact label for select boxes."""

        return f"{self.api} - {self.name_zh} - {self.description_zh}"


@cache
def tushare_table_catalog() -> tuple[TushareTableCatalogEntry, ...]:
    """Return local Tushare table catalog entries sorted for display."""

    resource = files("bagelquant_data.gui").joinpath("tushare_tables.json")
    payload = json.loads(resource.read_text(encoding="utf-8"))
    entries = tuple(_entry_from_mapping(item) for item in payload)
    return tuple(sorted(entries, key=lambda item: (item.category_zh, item.api)))


@cache
def tushare_table_catalog_by_api() -> dict[str, TushareTableCatalogEntry]:
    """Return catalog entries keyed by API name."""

    return {entry.api: entry for entry in tushare_table_catalog()}


def tushare_table_categories() -> tuple[str, ...]:
    """Return catalog categories sorted for display."""

    return tuple(dict.fromkeys(entry.category_zh for entry in tushare_table_catalog()))


def tushare_table_catalog_for_category(
    category_zh: str,
) -> tuple[TushareTableCatalogEntry, ...]:
    """Return catalog entries in one category."""

    return tuple(
        entry for entry in tushare_table_catalog() if entry.category_zh == category_zh
    )


def tushare_table_entry(api: str) -> TushareTableCatalogEntry | None:
    """Return one catalog entry, if present."""

    return tushare_table_catalog_by_api().get(api)


def default_tushare_table_kind(api: str) -> TushareTableKind:
    """Return the catalog default kind for an API."""

    entry = tushare_table_entry(api)
    return entry.default_kind if entry is not None else "general"


def tushare_table_description(api: str) -> str:
    """Return a Chinese description for an API."""

    entry = tushare_table_entry(api)
    if entry is None:
        return ""
    return f"{entry.name_zh}: {entry.description_zh}"


def _entry_from_mapping(payload: dict[str, Any]) -> TushareTableCatalogEntry:
    return TushareTableCatalogEntry(
        api=str(payload["api"]),
        name_zh=str(payload["name_zh"]),
        description_zh=str(payload["description_zh"]),
        category_zh=str(payload["category_zh"]),
        default_kind=_table_kind(payload["default_kind"]),
        source_url=(
            str(payload["source_url"])
            if payload.get("source_url") not in {None, ""}
            else None
        ),
    )


def _table_kind(value: object) -> TushareTableKind:
    text = str(value)
    if text not in {"general", "price", "fundamental", "fundamental_vip"}:
        raise ValueError(f"Invalid Tushare catalog table kind: {text}")
    return text  # type: ignore[return-value]
