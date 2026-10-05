"""Point-in-time availability policies for canonical DataItem panels."""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from dataclasses import asdict, dataclass
from datetime import date
from enum import StrEnum
from typing import Any, Mapping

import polars as pl


FORWARD_FILL_RUNTIME_VERSION = 2


class AvailabilityAlignment(StrEnum):
    EXACT_DAILY = "exact_daily"
    FORWARD_FILL = "forward_fill"
    DERIVED = "derived"


@dataclass(frozen=True, slots=True)
class AvailabilityPolicy:
    """Declare when DataItem observations may enter an Alpha input panel."""

    observation_date_field: str
    base_available_date_field: str | None
    availability_buffer_sessions: int
    alignment: AvailabilityAlignment
    max_forward_fill_sessions: int | None
    calendar_id: str
    base_available_date_source_field: str | None = None
    base_available_date_expression: str | None = None

    def __post_init__(self) -> None:
        if not self.observation_date_field.strip():
            raise ValueError("observation_date_field must not be blank")
        base_field = (self.base_available_date_field or "").strip()
        expression = (self.base_available_date_expression or "").strip()
        if bool(base_field) == bool(expression):
            raise ValueError(
                "availability policy requires exactly one base available date "
                "field or expression"
            )
        if expression != "max_input_available_date":
            if expression:
                raise ValueError(
                    "unsupported base available date expression: "
                    f"{expression}"
                )
        if expression and self.alignment != AvailabilityAlignment.DERIVED:
            raise ValueError(
                "base available date expressions require derived alignment"
            )
        if isinstance(self.availability_buffer_sessions, bool) or not isinstance(self.availability_buffer_sessions, int) or self.availability_buffer_sessions < 0:
            raise ValueError("availability buffer must be non-negative")
        if "/" not in self.calendar_id:
            raise ValueError("availability calendar_id must be source/dataset")
        if self.alignment == AvailabilityAlignment.FORWARD_FILL:
            if (
                self.max_forward_fill_sessions is None
                or isinstance(self.max_forward_fill_sessions, bool)
                or not isinstance(self.max_forward_fill_sessions, int)
                or self.max_forward_fill_sessions <= 0
            ):
                raise ValueError(
                    "forward-fill availability requires a positive finite limit"
                )
        elif self.max_forward_fill_sessions is not None:
            raise ValueError(
                "max_forward_fill_sessions is valid only for forward_fill"
            )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> AvailabilityPolicy:
        if not isinstance(value, Mapping) or not value:
            raise ValueError("DataItem metadata requires availability_policy")
        required = {
            "observation_date_field",
            "availability_buffer_sessions",
            "alignment",
            "calendar_id",
        }
        missing = sorted(required - set(value))
        if missing:
            raise ValueError(
                f"availability_policy is missing required fields: {missing}"
            )
        if not value.get("base_available_date_field") and not value.get(
            "base_available_date_expression"
        ):
            raise ValueError(
                "availability_policy requires base_available_date_field or "
                "base_available_date_expression"
            )
        return cls(
            observation_date_field=str(value["observation_date_field"]),
            base_available_date_field=(
                None
                if not value.get("base_available_date_field")
                else str(value["base_available_date_field"])
            ),
            availability_buffer_sessions=int(
                value["availability_buffer_sessions"]
            ),
            alignment=AvailabilityAlignment(str(value["alignment"])),
            max_forward_fill_sessions=(
                None
                if value.get("max_forward_fill_sessions") is None
                else int(value["max_forward_fill_sessions"])
            ),
            calendar_id=str(value["calendar_id"]),
            base_available_date_source_field=(
                None
                if value.get("base_available_date_source_field") is None
                else str(value["base_available_date_source_field"])
            ),
            base_available_date_expression=(
                None
                if not value.get("base_available_date_expression")
                else str(value["base_available_date_expression"])
            ),
        )

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["alignment"] = self.alignment.value
        return value


def advance_available_date(
    base_available_date: date,
    open_sessions: list[date],
    buffer_sessions: int,
) -> date | None:
    """Add a strict trading-session buffer to a source availability date."""

    sessions = sorted(set(open_sessions))
    return advance_available_date_in_sessions(
        base_available_date,
        sessions,
        buffer_sessions,
    )


def advance_available_date_in_sessions(
    base_available_date: date,
    normalized_sessions: list[date],
    buffer_sessions: int,
) -> date | None:
    """Map a date against an already sorted, unique session calendar."""

    if isinstance(buffer_sessions, bool) or not isinstance(buffer_sessions, int) or buffer_sessions < 0:
        raise ValueError("availability buffer must be a non-negative integer")
    sessions = normalized_sessions
    if buffer_sessions == 0:
        position = bisect_left(sessions, base_available_date)
    else:
        position = bisect_right(sessions, base_available_date) + buffer_sessions - 1
    return sessions[position] if position < len(sessions) else None


def materialize_daily_pit(
    observations: pl.DataFrame,
    calendar: pl.DataFrame,
    policy: AvailabilityPolicy,
    *,
    start: date,
    end: date,
) -> pl.DataFrame:
    """Materialize source observations as an availability-safe daily panel."""

    base_available_date_field = (
        policy.base_available_date_field
        if policy.base_available_date_field is not None
        else "available_date"
    )
    required = {
        "asset_id",
        "value",
        policy.observation_date_field,
        base_available_date_field,
    }
    missing = sorted(required - set(observations.columns))
    if missing:
        raise ValueError(f"availability input is missing required columns: {missing}")
    sessions = _open_sessions(calendar)
    period_sessions = [value for value in sessions if start <= value <= end]
    if not period_sessions or observations.is_empty():
        return _empty(observations.schema.get("value", pl.Float64))
    prepared = observations.with_columns(
        pl.col(policy.observation_date_field)
        .cast(pl.Date, strict=False)
        .alias("observation_date"),
        pl.col(base_available_date_field)
        .cast(pl.Date, strict=False)
        .alias("base_available_date"),
        pl.col("asset_id").cast(pl.String),
    ).drop_nulls(["observation_date", "base_available_date", "asset_id"])
    if prepared.is_empty():
        return _empty(observations.schema.get("value", pl.Float64))
    _reject_conflicting_versions(prepared)
    available_dates = {
        value: advance_available_date_in_sessions(
            value, sessions, policy.availability_buffer_sessions
        )
        for value in prepared.get_column("base_available_date").unique().to_list()
    }
    prepared = (
        prepared.with_columns(
            pl.col("base_available_date")
            .replace_strict(available_dates, default=None)
            .cast(pl.Date)
            .alias("available_date")
        )
        .drop_nulls("available_date")
        .sort(
            "asset_id",
            "available_date",
            "observation_date",
            *(
                ["update_flag"]
                if "update_flag" in observations.columns
                else []
            ),
        )
    )
    if policy.alignment == AvailabilityAlignment.DERIVED:
        return _derived_panel(prepared, start=start, end=end)
    prepared = prepared.unique(
        ["asset_id", "available_date"], keep="last", maintain_order=True
    )
    if policy.alignment == AvailabilityAlignment.EXACT_DAILY:
        return (
            prepared.filter(pl.col("available_date").is_between(start, end))
            .with_columns(pl.col("available_date").alias("time"))
            .sort("time", "asset_id")
        )
    return _forward_fill_panel(
        prepared,
        period_sessions,
        sessions,
        max_sessions=policy.max_forward_fill_sessions or 0,
    )


def _forward_fill_panel(
    observations: pl.DataFrame,
    period_sessions: list[date],
    all_sessions: list[date],
    *,
    max_sessions: int,
) -> pl.DataFrame:
    indices = {value: index for index, value in enumerate(all_sessions)}
    assets = observations.select("asset_id").unique()
    grid = (
        pl.DataFrame({"time": period_sessions})
        .join(assets, how="cross")
        .with_columns(
            pl.col("time")
            .replace_strict(indices)
            .cast(pl.Int64)
            .alias("_time_index")
        )
        .sort("asset_id", "time")
    )
    events = observations.with_columns(
        pl.col("available_date")
        .replace_strict(indices)
        .cast(pl.Int64)
        .alias("_available_index")
    ).sort("asset_id", "available_date")
    return (
        grid.join_asof(
            events,
            left_on="time",
            right_on="available_date",
            by="asset_id",
            strategy="backward",
            check_sortedness=False,
        )
        .filter(
            pl.col("_available_index").is_not_null()
            & ((pl.col("_time_index") - pl.col("_available_index")) < max_sessions)
        )
        .drop("_time_index", "_available_index")
        .sort("time", "asset_id")
    )


def _derived_panel(
    observations: pl.DataFrame, *, start: date, end: date
) -> pl.DataFrame:
    return (
        observations.with_columns(
            pl.max_horizontal(
                pl.col("time").cast(pl.Date),
                pl.col("available_date"),
            ).alias("time")
        )
        .filter(pl.col("time").is_between(start, end))
        .unique(["time", "asset_id"], keep="last")
        .sort("time", "asset_id")
    )


def _reject_conflicting_versions(frame: pl.DataFrame) -> None:
    keys = ["asset_id", "observation_date", "base_available_date"]
    conflicts = (
        frame.group_by(keys)
        .agg(pl.col("value").n_unique().alias("_value_count"))
        .filter(pl.col("_value_count") > 1)
    )
    if not conflicts.is_empty():
        raise ValueError(
            "DataItem source contains conflicting values for the same availability "
            "version key"
        )


def _open_sessions(calendar: pl.DataFrame) -> list[date]:
    if "time" not in calendar.columns:
        raise ValueError("availability calendar is missing time")
    frame = calendar.with_columns(pl.col("time").cast(pl.Date, strict=False))
    if "is_open" in frame.columns:
        frame = frame.filter(pl.col("is_open").cast(pl.Int64) == 1)
    sessions = (
        frame.select("time").drop_nulls().unique().sort("time").get_column("time").to_list()
    )
    if not sessions:
        raise ValueError("availability calendar contains no open sessions")
    return sessions


def _empty(value_type: pl.DataType | type[pl.DataType]) -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "time": pl.Date,
            "asset_id": pl.String,
            "value": value_type,
            "observation_date": pl.Date,
            "base_available_date": pl.Date,
            "available_date": pl.Date,
        }
    )


__all__ = [
    "AvailabilityAlignment",
    "AvailabilityPolicy",
    "advance_available_date",
    "advance_available_date_in_sessions",
    "materialize_daily_pit",
]

