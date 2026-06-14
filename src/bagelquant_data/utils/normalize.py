"""Shared table normalization helpers."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

import polars as pl

TIME_COLUMNS = (
    "time",
    "f_ann_date",
    "trade_date",
    "cal_date",
    "date",
    "datetime",
    "timestamp",
)
ASSET_COLUMNS = ("asset_id", "ts_code", "symbol", "asset", "code")


def normalize_table_columns(frame: pl.DataFrame) -> pl.DataFrame:
    """Normalize provider-specific time and asset columns."""

    rename: dict[str, str] = {}
    if "time" not in frame.columns:
        for column in TIME_COLUMNS:
            if column in frame.columns:
                rename[column] = "time"
                break
    if "asset_id" not in frame.columns:
        for column in ASSET_COLUMNS:
            if column in frame.columns:
                rename[column] = "asset_id"
                break
    normalized = frame.rename(rename)
    if "time" in normalized.columns:
        normalized = normalized.with_columns(date_column("time"))
    if "asset_id" in normalized.columns:
        normalized = normalized.with_columns(pl.col("asset_id").cast(pl.String))
    return normalized


def date_column(column: str) -> pl.Expr:
    """Parse common date encodings into a Polars Date column."""

    text = pl.col(column).cast(pl.String)
    return (
        pl.coalesce(
            text.str.strptime(pl.Date, "%Y%m%d", strict=False),
            text.str.strptime(pl.Date, "%Y-%m-%d", strict=False),
            pl.col(column).cast(pl.Date, strict=False),
        )
        .alias(column)
    )


def as_date(value: Any) -> date:
    """Coerce common date-like values to ``datetime.date``."""

    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return datetime.fromisoformat(str(value)).date()


def parse_date(value: Any) -> date | None:
    """Best-effort date parser used for logs and metadata."""

    try:
        return as_date(value)
    except (TypeError, ValueError):
        return None


def parse_tushare_date(value: Any) -> date | None:
    """Parse either ``YYYYMMDD`` or ISO-style Tushare date values."""

    text = str(value)
    try:
        return datetime.strptime(text, "%Y%m%d").date()
    except ValueError:
        return parse_date(text)


def tushare_date(value: Any) -> str:
    """Format a date-like value as Tushare's ``YYYYMMDD`` string."""

    return as_date(value).strftime("%Y%m%d")
