"""Lake-owned update request planning."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Protocol

import polars as pl

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.exceptions import ConfigurationError, DatasetNotFoundError
from bagelquant_data.core.types import DateLike
from bagelquant_data.query.raw import RawQueryService


class ReferenceReader(Protocol):
    def reference(self, dataset: str, *, source: str, collect: bool = False) -> pl.LazyFrame | pl.DataFrame:
        """Read a registered reference dataset."""
        ...


@dataclass(frozen=True, slots=True)
class PlannedUpdate:
    """Requests and commit grouping for one dataset update."""

    requests: tuple[dict[str, object], ...]


def plan_update(
    *,
    spec: DatasetSpec,
    raw: RawQueryService,
    references: ReferenceReader,
    start: DateLike | None = None,
    end: DateLike | None = None,
    today: DateLike | None = None,
    ids: Sequence[str] | None = None,
) -> PlannedUpdate:
    """Plan normalized source requests for a dataset update."""

    final_day = _date_value(end or today or date.today())
    if spec.update_type == "general":
        request = _base_request(spec)
        if start is not None:
            request["start"] = _date_value(start).isoformat()
        if end is not None:
            request["end"] = _date_value(end).isoformat()
        return PlannedUpdate((request,))
    if spec.update_type == "by_daily":
        return PlannedUpdate(
            tuple(_daily_requests(spec, raw=raw, references=references, start=start, final_day=final_day))
        )
    if spec.update_type == "by_id":
        return PlannedUpdate(
            tuple(_id_requests(spec, raw=raw, references=references, ids=ids, start=start, final_day=final_day))
        )
    raise ConfigurationError(f"{spec.source}/{spec.name} unsupported update_type: {spec.update_type}")


def _daily_requests(
    spec: DatasetSpec,
    *,
    raw: RawQueryService,
    references: ReferenceReader,
    start: DateLike | None,
    final_day: date,
) -> list[dict[str, object]]:
    existing = _existing_dates(raw, spec)
    start_value = start if start is not None else spec.start_date
    requested_start = _date_value(start_value) if start_value is not None else None
    dates = _calendar_dates(spec, references)
    missing = [
        value
        for value in dates
        if value <= final_day and value not in existing and (requested_start is None or value >= requested_start)
    ]
    return [_request_for_date(spec, value) for value in missing]


def _id_requests(
    spec: DatasetSpec,
    *,
    raw: RawQueryService,
    references: ReferenceReader,
    ids: Sequence[str] | None,
    start: DateLike | None,
    final_day: date,
) -> list[dict[str, object]]:
    id_values = [str(value) for value in ids] if ids is not None else _reference_ids(spec, references)
    latest = _latest_dates_by_id(raw, spec)
    start_value = start if start is not None else spec.start_date
    fallback_start = _date_value(start_value) if start_value is not None else None
    requests: list[dict[str, object]] = []
    for asset_id in id_values:
        asset_start = latest.get(asset_id)
        request_start = asset_start + timedelta(days=1) if asset_start is not None else fallback_start
        if request_start is not None and request_start > final_day:
            continue
        request = _base_request(spec)
        request[spec.request_id_param] = asset_id
        if request_start is not None:
            request["start"] = request_start.isoformat()
        request["end"] = final_day.isoformat()
        requests.append(request)
    return requests


def _base_request(spec: DatasetSpec) -> dict[str, object]:
    return dict(spec.request_options.get("static_params") or {})


def _request_for_date(spec: DatasetSpec, value: date) -> dict[str, object]:
    request = _base_request(spec)
    request[spec.request_date_param] = value.isoformat()
    return request


def _calendar_dates(spec: DatasetSpec, references: ReferenceReader) -> list[date]:
    if not spec.calendar_dataset:
        raise ConfigurationError(f"{spec.source}/{spec.name} requires calendar_dataset")
    frame = references.reference(spec.calendar_dataset, source=spec.source, collect=True)
    if isinstance(frame, pl.LazyFrame):
        frame = frame.collect()
    if frame.is_empty():
        raise ConfigurationError(f"{spec.source}/{spec.calendar_dataset} is empty")
    if spec.calendar_date_column not in frame.columns:
        raise ConfigurationError(f"{spec.source}/{spec.calendar_dataset} missing {spec.calendar_date_column}")
    filtered = frame
    if spec.calendar_open_column and spec.calendar_open_column in filtered.columns:
        filtered = filtered.filter(pl.col(spec.calendar_open_column).cast(pl.Int8, strict=False) == 1)
    return [
        value
        for value in filtered.select(_date_expr(spec.calendar_date_column).alias("_date"))
        .drop_nulls()
        .unique()
        .sort("_date")
        .get_column("_date")
        .to_list()
    ]


def _reference_ids(spec: DatasetSpec, references: ReferenceReader) -> list[str]:
    if not spec.id_dataset:
        raise ConfigurationError(f"{spec.source}/{spec.name} requires id_dataset")
    frame = references.reference(spec.id_dataset, source=spec.source, collect=True)
    if isinstance(frame, pl.LazyFrame):
        frame = frame.collect()
    if frame.is_empty():
        raise ConfigurationError(f"{spec.source}/{spec.id_dataset} is empty")
    if spec.id_column not in frame.columns:
        raise ConfigurationError(f"{spec.source}/{spec.id_dataset} missing {spec.id_column}")
    return [
        str(value)
        for value in frame.select(pl.col(spec.id_column).cast(pl.String).alias("_id"))
        .drop_nulls()
        .unique()
        .sort("_id")
        .get_column("_id")
        .to_list()
    ]


def _existing_dates(raw: RawQueryService, spec: DatasetSpec) -> set[date]:
    try:
        frame = raw.raw(spec.name, source=spec.source, columns=("time",)).collect()
    except DatasetNotFoundError:
        return set()
    if frame.is_empty() or "time" not in frame.columns:
        return set()
    return set(frame.select(pl.col("time").cast(pl.Date).alias("time")).get_column("time").to_list())


def _latest_dates_by_id(raw: RawQueryService, spec: DatasetSpec) -> dict[str, date]:
    try:
        frame = raw.raw(spec.name, source=spec.source, columns=("asset_id", "time")).collect()
    except DatasetNotFoundError:
        return {}
    if frame.is_empty() or "asset_id" not in frame.columns or "time" not in frame.columns:
        return {}
    rows = (
        frame.select(pl.col("asset_id").cast(pl.String), pl.col("time").cast(pl.Date))
        .group_by("asset_id")
        .agg(pl.max("time").alias("time"))
        .to_dicts()
    )
    return {str(row["asset_id"]): row["time"] for row in rows if row["time"] is not None}


def _date_value(value: DateLike) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value)
    if "T" in text:
        text = text.split("T", maxsplit=1)[0]
    return datetime.strptime(text[:10], "%Y-%m-%d").date()


def _date_expr(column: str) -> pl.Expr:
    return (
        pl.when(pl.col(column).cast(pl.String).str.len_chars() == 8)
        .then(pl.col(column).cast(pl.String).str.strptime(pl.Date, "%Y%m%d", strict=False))
        .otherwise(pl.col(column).cast(pl.Date, strict=False))
    )
