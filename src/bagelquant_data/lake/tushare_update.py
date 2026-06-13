"""Tushare lake update models."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Literal

from bagelquant_data.lake.local import WriteMode

TushareTableKind = Literal["general", "price", "fundamental", "fundamental_vip"]
TushareCallStatus = Literal["success", "empty", "failed"]
TushareUpdateStatus = Literal["pending", "up_to_date"]


@dataclass(frozen=True, slots=True)
class TushareUniverseRef:
    """Local reference table used as the asset universe for Tushare updates."""

    name: str
    table: str
    code_column: str = "ts_code"


@dataclass(frozen=True, slots=True)
class TushareTradingCalendarRef:
    """Local reference table used as the trading calendar for Tushare updates."""

    name: str
    table: str = "trade_cal"
    date_column: str = "cal_date"
    open_column: str = "is_open"
    filters: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TushareTableUpdateSpec:
    """Configured Tushare table update target."""

    table: str
    kind: TushareTableKind | None = None
    universe: TushareUniverseRef | None = None
    trading_calendar: TushareTradingCalendarRef | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TushareUpdateJob:
    """Confirmed provider request needed to update a Tushare lake table."""

    table: str
    kind: TushareTableKind
    filters: Mapping[str, Any] = field(default_factory=dict)
    start_date: date | None = None
    end_date: date | None = None
    partition_column: str | None = None
    partition_granularity: Literal["month", "day", "quarter"] = "month"
    metadata: Mapping[str, Any] = field(default_factory=dict)
    item: str = ""
    item_key: str = ""
    item_value: str = ""
    api_name: str | None = None
    mode: WriteMode = "append"
    universe: str | None = None
    trading_calendar: str | None = None


@dataclass(frozen=True, slots=True)
class TushareUpdatePlan:
    """Dry-run summary for a configured Tushare table."""

    table: str
    kind: TushareTableKind
    requested_start: date
    requested_end: date
    effective_start: date | None
    pending_items: tuple[str, ...]
    reason: str
    estimated_job_count: int
    status: TushareUpdateStatus
    universe: str | None = None
    trading_calendar: str | None = None


@dataclass(frozen=True, slots=True)
class TushareUpdateReport:
    """Dry-run report plus executable jobs for confirmed Tushare updates."""

    generated_at: datetime
    source: str
    requested_start: date
    requested_end: date
    plans: tuple[TushareUpdatePlan, ...]
    jobs: tuple[TushareUpdateJob, ...]
