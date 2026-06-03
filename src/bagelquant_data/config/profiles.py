"""Profile helpers."""

from __future__ import annotations

from dataclasses import dataclass

from bagelquant_data.config.settings import Settings


@dataclass(frozen=True, slots=True)
class Profile:
    """Named runtime settings profile."""

    name: str
    settings: Settings
