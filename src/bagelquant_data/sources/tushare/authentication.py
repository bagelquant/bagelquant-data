"""Tushare credential resolution."""

from __future__ import annotations

import os

from bagelquant_data.core.exceptions import ConfigurationError


def resolve_token(token: str | None = None) -> str:
    """Resolve a Tushare token without persisting it."""

    resolved = token or os.environ.get("TUSHARE_TOKEN")
    if not resolved:
        raise ConfigurationError("Tushare token is required. Pass token=... or set TUSHARE_TOKEN.")
    return resolved
