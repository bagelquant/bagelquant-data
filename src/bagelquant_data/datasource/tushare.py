"""Tushare data source."""

from __future__ import annotations

import importlib
import os
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import polars as pl

from bagelquant_data.config.settings import Settings
from bagelquant_data.datasource.base import DataRequest
from bagelquant_data.utils.exceptions import DataSourceAuthError, DataSourceError
from bagelquant_data.utils.normalize import normalize_table_columns

TransientPredicate = Callable[[Exception], bool]


@dataclass(frozen=True, slots=True)
class RetryConfig:
    """Retry settings for transient provider failures."""

    attempts: int = 3
    delay_seconds: float = 0.5
    backoff: float = 2.0
    access_limit_sleep_seconds: float = 60.0


class TushareDataSource:
    """Tushare Pro data source.

    The token is never exposed through ``describe`` or ``repr``. Users may pass
    it explicitly, set ``TUSHARE_TOKEN``, or provide a settings profile.
    """

    name = "tushare"

    def __init__(
        self,
        *,
        token: str | None = None,
        settings: Settings | None = None,
        client: Any | None = None,
        retry: RetryConfig | None = None,
        transient: TransientPredicate | None = None,
    ) -> None:
        self._token = _resolve_token(token=token, settings=settings)
        self._retry = retry or RetryConfig()
        self._transient = transient or _is_transient
        self._client = client or self._build_client()

    def __repr__(self) -> str:
        """Return a token-safe representation."""

        return "TushareDataSource(token=<redacted>)"

    def read(self, request: DataRequest) -> pl.DataFrame:
        """Read a Tushare dataset."""

        params = self._params_for(request)
        api_name = str(request.options.get("api_name") or request.dataset)

        if request.dataset == "generic" and "api_name" not in request.options:
            raise DataSourceError(
                "generic Tushare requests require options['api_name']"
            )

        def call() -> Any:
            method = getattr(self._client, api_name, None)
            if callable(method):
                return method(**params)
            query = getattr(self._client, "query", None)
            if callable(query):
                return query(api_name, **params)
            raise DataSourceError(f"Tushare API is not available: {api_name}")

        result = self._with_retry(call)
        if not _is_pandas_dataframe(result):
            raise DataSourceError(
                f"Tushare API returned {type(result)!r}, expected DataFrame"
            )
        return normalize_table_columns(pl.from_pandas(result.copy(deep=True)))

    def exists(self, dataset: str) -> bool:
        """Return whether a dataset is supported by the adapter."""

        if dataset.endswith("_vip"):
            return True
        return dataset in {
            "stock_basic",
            "trade_cal",
            "daily",
            "adj_factor",
            "balancesheet",
            "income",
            "cashflow",
            "index_daily",
            "generic",
        }

    def describe(self, dataset: str) -> Mapping[str, Any]:
        """Return token-safe provider metadata."""

        return {
            "provider": self.name,
            "dataset": dataset,
            "supported": self.exists(dataset),
            "token": "<redacted>",
        }

    def _build_client(self) -> Any:
        try:
            tushare = importlib.import_module("tushare")
        except ImportError as exc:
            raise DataSourceError(
                "Tushare support requires installing the optional extra: "
                "uv sync --extra tushare"
            ) from exc
        tushare.set_token(self._token)
        return tushare.pro_api()

    def _params_for(self, request: DataRequest) -> dict[str, Any]:
        params = dict(request.filters)
        params.update(request.options.get("params", {}))
        if request.fields:
            params["fields"] = ",".join(request.fields)
        if request.start_date is not None:
            params["start_date"] = _tushare_date(request.start_date)
        if request.end_date is not None:
            params["end_date"] = _tushare_date(request.end_date)
        return params

    def _with_retry(self, call: Callable[[], Any]) -> Any:
        delay = self._retry.delay_seconds
        last_error: Exception | None = None
        for attempt in range(1, self._retry.attempts + 1):
            try:
                return call()
            except Exception as exc:
                last_error = exc
                if attempt >= self._retry.attempts:
                    raise DataSourceError(f"Tushare request failed: {exc}") from exc
                if _is_access_limit(exc):
                    time.sleep(self._retry.access_limit_sleep_seconds)
                else:
                    time.sleep(delay)
                    delay *= self._retry.backoff
        raise DataSourceError("Tushare request failed") from last_error


def _resolve_token(*, token: str | None, settings: Settings | None) -> str:
    resolved = token or os.environ.get("TUSHARE_TOKEN")
    if resolved is None and settings is not None:
        resolved = settings.tushare_token
    if not resolved:
        raise DataSourceAuthError(
            "Tushare token is required. Pass token=..., set TUSHARE_TOKEN, "
            "or provide Settings(tushare_token=...)."
        )
    return resolved


def _tushare_date(value: Any) -> str:
    import pandas as pd

    timestamp = pd.Timestamp(value)
    if pd.isna(timestamp):
        raise DataSourceError(f"Invalid Tushare date: {value!r}")
    return timestamp.strftime("%Y%m%d")


def _is_pandas_dataframe(value: Any) -> bool:
    try:
        import pandas as pd
    except ImportError as exc:
        raise DataSourceError(
            "Tushare support requires pandas. Install with: uv sync --extra tushare"
        ) from exc
    return isinstance(value, pd.DataFrame)


def _is_transient(exc: Exception) -> bool:
    message = str(exc).lower()
    return _is_access_limit(exc) or any(
        token in message for token in ("timeout", "temporar", "rate", "retry")
    )


def _is_access_limit(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(
        token in message
        for token in (
            "access limit",
            "api access",
            "rate limit",
            "访问限制",
            "频次",
            "每分钟",
            "超过",
        )
    )
