"""Tushare source adapter."""

from __future__ import annotations

from collections.abc import Callable, Mapping
import re
import time
from threading import Lock, local
from typing import Any

import polars as pl

from bagelquant_data.core.exceptions import DataSourceError
from bagelquant_data.sources.tushare.client import build_client


class TushareSource:
    """Tushare Pro implementation of the generic DataSource protocol."""

    def __init__(
        self,
        name: str = "tushare",
        *,
        token: str | None = None,
        client: Any | None = None,
    ) -> None:
        self._name = name
        self._token = token
        self._provided_client = client
        self._clients = local()
        self._client_lock = Lock()
        self._rate_lock = Lock()
        self._rate_limits: dict[str, tuple[float, float]] = {}

    @property
    def name(self) -> str:
        return self._name

    def __repr__(self) -> str:
        return f"TushareSource(name={self.name!r}, token=<redacted>)"

    def configure(self, **options: Any) -> None:
        if "token" in options:
            with self._client_lock:
                self._token = str(options["token"])
                self._clients = local()

    def test_connection(self) -> None:
        client = self._ensure_client()
        query = getattr(client, "query", None)
        if callable(query):
            query("trade_cal", start_date="20200101", end_date="20200101")
            return
        if not any(
            callable(getattr(client, name, None))
            for name in ("trade_cal", "stock_basic")
        ):
            raise DataSourceError("Tushare client has no callable API methods")

    def fetch(self, dataset: str, request: Mapping[str, Any]) -> pl.DataFrame:
        client = self._ensure_client()
        params = _to_tushare_params(request)
        method = getattr(client, dataset, None)
        try:
            if callable(method):
                result = method(**params)
            else:
                query = getattr(client, "query", None)
                if not callable(query):
                    raise DataSourceError(f"Tushare API is not available: {dataset}")
                result = query(dataset, **params)
        except Exception as error:
            # The provider reports the account's actual quota. Share the
            # cooldown and subsequent paced admission across all fetch threads.
            match = re.search(r"(\d+)\s*次\s*/\s*分钟", str(error))
            if match and int(match[1]) > 0:
                interval = 60.0 / (int(match[1]) * 0.9)
                with self._rate_lock:
                    self._rate_limits[dataset] = (interval, time.monotonic() + 61.0)
            raise
        return _from_pandas(result)

    def wait_for_request(
        self, dataset: str, cancel_requested: Callable[[], bool] | None = None
    ) -> bool:
        """Admit a physical request, with cancelable shared provider throttling."""

        while True:
            if cancel_requested is not None and cancel_requested():
                return False
            with self._rate_lock:
                interval, next_request = self._rate_limits.get(dataset, (0.0, 0.0))
                now = time.monotonic()
                delay = next_request - now
                if delay <= 0:
                    if interval:
                        self._rate_limits[dataset] = (interval, now + interval)
                    return True
            time.sleep(min(delay, 0.1))

    def _ensure_client(self) -> Any:
        if self._provided_client is not None:
            return self._provided_client
        client = getattr(self._clients, "client", None)
        if client is None:
            # Tushare configures its token through module-global state while a
            # client is created, so serialize initialization but not requests.
            with self._client_lock:
                client = build_client(self._token)
                self._clients.client = client
        return client


def _to_tushare_params(request: Mapping[str, Any]) -> dict[str, Any]:
    params: dict[str, Any] = {}
    for key, value in request.items():
        mapped = {
            "start": "start_date",
            "end": "end_date",
            "date": "trade_date",
            "id": "ts_code",
        }.get(key, key)
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
        raise DataSourceError(
            f"Tushare returned {type(value)!r}, expected pandas.DataFrame"
        )
    return pl.from_pandas(value)
