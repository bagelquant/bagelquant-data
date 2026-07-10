"""Tushare source adapter."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import polars as pl

from bagelquant_data.core.exceptions import DataSourceError
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

    def _ensure_client(self) -> Any:
        if self._client is None:
            self._client = build_client(self._token)
        return self._client


def _to_tushare_params(request: Mapping[str, Any]) -> dict[str, Any]:
    params: dict[str, Any] = {}
    for key, value in request.items():
        mapped = {"start": "start_date", "end": "end_date", "date": "trade_date", "id": "ts_code"}.get(key, key)
        params[mapped] = _format_date(value) if mapped.endswith("date") else value
    return params


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
