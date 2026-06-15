"""Shared type aliases."""

from __future__ import annotations

from datetime import date, datetime
from os import PathLike
from typing import TypeAlias

DateLike: TypeAlias = str | date | datetime
PathLikeStr: TypeAlias = str | PathLike[str]
