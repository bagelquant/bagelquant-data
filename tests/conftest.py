"""Synthetic historical fixtures use a fixed collection clock, never wall time."""
from datetime import UTC, datetime

import pytest


@pytest.fixture(autouse=True)
def fixed_default_ingestion_clock(monkeypatch):
    from bagelquant_data.pipeline import versions

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            value = datetime(2000, 1, 1, tzinfo=UTC)
            return value.astimezone(tz) if tz is not None else value.replace(tzinfo=None)

    monkeypatch.setattr(versions, "datetime", Clock)
