"""Tushare client wrapper."""

from __future__ import annotations

import importlib
from typing import Any

from bagelquant_data.core.exceptions import DataSourceError
from bagelquant_data.sources.tushare.authentication import resolve_token


def build_client(token: str | None = None) -> Any:
    """Build a Tushare Pro API client."""

    try:
        tushare = importlib.import_module("tushare")
    except ImportError as exc:
        raise DataSourceError("Install Tushare support with: uv sync --extra tushare") from exc
    tushare.set_token(resolve_token(token))
    return tushare.pro_api()
