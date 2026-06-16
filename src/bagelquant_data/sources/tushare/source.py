"""Tushare source adapter."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import date, datetime
from typing import Any

import polars as pl

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.exceptions import DataSourceError
from bagelquant_data.core.request import RequestContext
from bagelquant_data.sources.tushare.client import build_client


class TushareSource:
    """Tushare Pro implementation of the generic DataSource protocol."""

    def __init__(self, name: str = "tushare", *, token: str | None = None, client: Any | None = None) -> None:
        self._name = name
        self._token = token
        self._client = client

    @property
    def name(self) -> str:
        return self._name

    def __repr__(self) -> str:
        return f"TushareSource(name={self.name!r}, token=<redacted>)"

    def configure(self, **options: Any) -> None:
        if "token" in options:
            self._token = str(options["token"])
            self._client = None

    def test_connection(self) -> None:
        client = self._ensure_client()
        query = getattr(client, "query", None)
        if callable(query):
            query("trade_cal", start_date="20200101", end_date="20200101")
            return
        if not any(callable(getattr(client, name, None)) for name in ("trade_cal", "stock_basic")):
            raise DataSourceError("Tushare client has no callable API methods")

    def fetch(self, source_dataset: str, request: Mapping[str, Any]) -> pl.DataFrame:
        client = self._ensure_client()
        params = _to_tushare_params(request)
        method = getattr(client, source_dataset, None)
        if callable(method):
            result = method(**params)
        else:
            query = getattr(client, "query", None)
            if not callable(query):
                raise DataSourceError(f"Tushare API is not available: {source_dataset}")
            result = query(source_dataset, **params)
        return _from_pandas(result)

    def plan_requests(self, dataset: DatasetSpec, context: RequestContext) -> Iterable[Mapping[str, Any]]:
        static_params = _static_params(dataset)
        if dataset.request_planner == "by_asset_date_range":
            if not context.assets:
                raise DataSourceError(
                    f"{dataset.source}/{dataset.name} requires assets because Tushare API calls need an index code"
                )
            request_param = str(dataset.request_options.get("request_param") or "ts_code")
            chunk_years = int(dataset.request_options.get("date_chunk_years", 10))
            for asset in context.assets:
                for start, end in _date_range_chunks(context.start, context.end, chunk_years):
                    request: dict[str, Any] = {**static_params, request_param: asset}
                    if start is not None:
                        request["start_date"] = start
                    if end is not None:
                        request["end_date"] = end
                    yield request
            return
        if dataset.request_planner == "by_asset_trade_date":
            if not context.assets:
                raise DataSourceError(
                    f"{dataset.source}/{dataset.name} requires assets because Tushare API calls need an index code"
                )
            trade_dates = context.options.get("trade_dates")
            if not trade_dates:
                raise DataSourceError(
                    f"{dataset.source}/{dataset.name} requires trade_dates for asset-by-date market updates"
                )
            request_param = str(dataset.request_options.get("request_param") or "ts_code")
            for asset in context.assets:
                for trade_date in trade_dates:
                    yield {**static_params, request_param: asset, "trade_date": trade_date}
            return
        if dataset.category == "market":
            trade_dates = context.options.get("trade_dates")
            if not trade_dates:
                raise DataSourceError(
                    f"{dataset.source}/{dataset.name} requires trade_dates for day-by-day market updates"
                )
            for trade_date in trade_dates:
                yield {**static_params, "trade_date": trade_date}
            return
        if dataset.request_planner == "by_asset":
            if not context.assets:
                raise DataSourceError(
                    f"{dataset.source}/{dataset.name} requires assets because Tushare API calls need ts_code"
                )
            request_param = str(dataset.request_options.get("request_param") or "ts_code")
            for asset in context.assets:
                request: dict[str, Any] = {**static_params, request_param: asset}
                if context.start is not None:
                    request["start_date"] = context.start
                if context.end is not None:
                    request["end_date"] = context.end
                yield request
            return
        request = dict(static_params)
        if _supports_date_range(dataset):
            if context.start is not None:
                request["start_date"] = context.start
            if context.end is not None:
                request["end_date"] = context.end
        yield request

    def _ensure_client(self) -> Any:
        if self._client is None:
            self._client = build_client(self._token)
        return self._client


def _to_tushare_params(request: Mapping[str, Any]) -> dict[str, Any]:
    params: dict[str, Any] = {}
    for key, value in request.items():
        mapped = {"start": "start_date", "end": "end_date", "asset_id": "ts_code"}.get(key, key)
        params[mapped] = _format_date(value) if mapped.endswith("date") else value
    return params


def _static_params(dataset: DatasetSpec) -> dict[str, Any]:
    params = dataset.request_options.get("static_params")
    return dict(params) if isinstance(params, Mapping) else {}


def _date_range_chunks(start: Any, end: Any, chunk_years: int) -> Iterable[tuple[str | None, str | None]]:
    if chunk_years <= 0:
        raise DataSourceError("date_chunk_years must be positive")
    if start is None or end is None:
        yield (_date_text(start) if start is not None else None, _date_text(end) if end is not None else None)
        return

    current = _date_value(start)
    final = _date_value(end)
    if current > final:
        raise DataSourceError(f"start date {start} is after end date {end}")

    while current <= final:
        block_start_year = (current.year // chunk_years) * chunk_years
        block_end = date(block_start_year + chunk_years - 1, 12, 31)
        chunk_end = min(block_end, final)
        yield current.isoformat(), chunk_end.isoformat()
        current = date(chunk_end.year + 1, 1, 1)


def _supports_date_range(dataset: DatasetSpec) -> bool:
    return bool(dataset.time_column) or dataset.source_dataset == "trade_cal"


def _date_value(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value)
    if "T" in text:
        text = text.split("T", maxsplit=1)[0]
    return datetime.strptime(text[:10], "%Y-%m-%d").date()


def _date_text(value: Any) -> str:
    return _date_value(value).isoformat()


def _format_date(value: Any) -> Any:
    if hasattr(value, "strftime"):
        return value.strftime("%Y%m%d")
    text = str(value)
    return text.replace("-", "") if len(text) == 10 and text[4] == "-" else value


def _from_pandas(value: Any) -> pl.DataFrame:
    try:
        import pandas as pd
    except ImportError as exc:
        raise DataSourceError("Tushare support requires pandas") from exc
    if not isinstance(value, pd.DataFrame):
        raise DataSourceError(f"Tushare returned {type(value)!r}, expected pandas.DataFrame")
    return pl.from_pandas(value.copy(deep=True))
