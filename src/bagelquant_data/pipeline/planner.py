"""Lake-owned update request planning."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from itertools import product

import polars as pl

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.exceptions import ConfigurationError, DatasetNotFoundError
from bagelquant_data.core.types import DateLike
from bagelquant_data.query.raw import RawQueryService


@dataclass(frozen=True, slots=True)
class PlannedUpdate:
    """Requests and commit grouping for one dataset update."""

    requests: tuple[dict[str, object], ...]


def plan_update(
    *,
    spec: DatasetSpec,
    raw: RawQueryService,
    start: DateLike | None = None,
    end: DateLike | None = None,
    today: DateLike | None = None,
    ids: Sequence[str] | None = None,
    params: dict[str, object] | None = None,
) -> PlannedUpdate:
    """Plan normalized source requests for a dataset update."""

    final_day = _date_value(end or today or date.today())
    if spec.update_type == "general":
        requests = _base_requests(spec, params)
        for request in requests:
            if start is not None:
                request["start"] = _date_value(start).isoformat()
            if end is not None:
                request["end"] = _date_value(end).isoformat()
        return PlannedUpdate(tuple(requests))
    if spec.update_type == "by_daily":
        return PlannedUpdate(
            tuple(_daily_requests(spec, raw=raw, start=start, final_day=final_day, params=params))
        )
    if spec.update_type == "by_asset":
        return PlannedUpdate(
            tuple(_asset_requests(spec, raw=raw, ids=ids, start=start, final_day=final_day, params=params))
        )
    raise ConfigurationError(f"{spec.source}/{spec.name} unsupported update_type: {spec.update_type}")


def _daily_requests(
    spec: DatasetSpec,
    *,
    raw: RawQueryService,
    start: DateLike | None,
    final_day: date,
    params: dict[str, object] | None,
) -> list[dict[str, object]]:
    existing = _existing_dates(raw, spec)
    requested_start = _date_value(start) if start is not None else None
    dates = _calendar_dates(spec, raw)
    missing = [
        value
        for value in dates
        if value <= final_day and value not in existing and (requested_start is None or value >= requested_start)
    ]
    return [
        _request_for_date(request, value, spec.date_param)
        for value in missing
        for request in _base_requests(spec, params)
    ]


def _asset_requests(
    spec: DatasetSpec,
    *,
    raw: RawQueryService,
    ids: Sequence[str] | None,
    start: DateLike | None,
    final_day: date,
    params: dict[str, object] | None,
) -> list[dict[str, object]]:
    id_values = [str(value) for value in ids] if ids is not None else _asset_ids(spec, raw)
    latest = _latest_dates_by_asset(raw, spec)
    fallback_start = _date_value(start) if start is not None else None
    requests: list[dict[str, object]] = []
    for asset_id in id_values:
        asset_start = latest.get(asset_id)
        request_start = asset_start + timedelta(days=1) if asset_start is not None else fallback_start
        if request_start is not None and request_start > final_day:
            continue
        for request in _base_requests(spec, params):
            request["id"] = asset_id
            if request_start is not None:
                request["start"] = request_start.isoformat()
            request["end"] = final_day.isoformat()
            requests.append(request)
    return requests


def _base_requests(spec: DatasetSpec, params: dict[str, object] | None) -> list[dict[str, object]]:
    defaults = dict(spec.source_api_params)
    overrides = dict(params or {})
    parameter_sets = spec.source_api_param_sets or ({},)
    requests: list[dict[str, object]] = []
    for parameter_set in parameter_sets:
        for variant in _expand_parameter_set(parameter_set):
            request = dict(defaults)
            request.update(variant)
            request.update(overrides)
            requests.append(request)
    return requests


def _expand_parameter_set(parameter_set: dict[str, object]) -> list[dict[str, object]]:
    keys = tuple(parameter_set)
    value_sets = [value if isinstance(value, list) else [value] for value in parameter_set.values()]
    return [dict(zip(keys, values, strict=True)) for values in product(*value_sets)]


def _request_for_date(request: dict[str, object], value: date, date_param: str | None) -> dict[str, object]:
    request[date_param or "date"] = value.isoformat()
    return request


def _calendar_dates(spec: DatasetSpec, raw: RawQueryService) -> list[date]:
    if not spec.calendar:
        raise ConfigurationError(f"{spec.source}/{spec.name} requires calendar")
    frame = raw.query_general(spec.calendar, source=spec.source).collect()
    if frame.is_empty():
        raise ConfigurationError(f"{spec.source}/{spec.calendar} is empty")
    if "time" not in frame.columns:
        raise ConfigurationError(f"{spec.source}/{spec.calendar} missing time")
    filtered = frame.filter(pl.col("is_open").cast(pl.Int8, strict=False) == 1) if "is_open" in frame.columns else frame
    return [
        value
        for value in filtered.select(_date_expr("time").alias("_date"))
        .drop_nulls()
        .unique()
        .sort("_date")
        .get_column("_date")
        .to_list()
    ]


def _asset_ids(spec: DatasetSpec, raw: RawQueryService) -> list[str]:
    if not spec.asset_list:
        raise ConfigurationError(f"{spec.source}/{spec.name} requires asset_list")
    frame = raw.query_general(spec.asset_list, source=spec.source).collect()
    if frame.is_empty():
        raise ConfigurationError(f"{spec.source}/{spec.asset_list} is empty")
    if "asset_id" not in frame.columns:
        raise ConfigurationError(f"{spec.source}/{spec.asset_list} missing asset_id")
    return [
        str(value)
        for value in frame.select(pl.col("asset_id").cast(pl.String).alias("_id"))
        .drop_nulls()
        .unique()
        .sort("_id")
        .get_column("_id")
        .to_list()
    ]


def _existing_dates(raw: RawQueryService, spec: DatasetSpec) -> set[date]:
    try:
        frame = raw.query(spec.name, source=spec.source, fields=("time",)).collect()
    except DatasetNotFoundError:
        return set()
    if frame.is_empty() or "time" not in frame.columns:
        return set()
    return set(frame.select(pl.col("time").cast(pl.Date).alias("time")).get_column("time").to_list())


def _latest_dates_by_asset(raw: RawQueryService, spec: DatasetSpec) -> dict[str, date]:
    try:
        frame = raw.query(spec.name, source=spec.source, fields=("asset_id", "time")).collect()
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


def _date_expr(field: str) -> pl.Expr:
    return (
        pl.when(pl.col(field).cast(pl.String).str.len_chars() == 8)
        .then(pl.col(field).cast(pl.String).str.strptime(pl.Date, "%Y%m%d", strict=False))
        .otherwise(pl.col(field).cast(pl.Date, strict=False))
    )
