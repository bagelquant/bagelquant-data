"""Package settings."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Settings:
    """Runtime settings for bagelquant-data."""

    tushare_token: str | None = None

    @classmethod
    def from_env(cls) -> Settings:
        """Load settings from environment variables."""

        return cls(tushare_token=os.environ.get("TUSHARE_TOKEN"))
