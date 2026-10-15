"""Disposable, byte-bounded input preparation for one admitted Item window."""
from __future__ import annotations

from collections import OrderedDict
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

import polars as pl


@dataclass(slots=True)
class PreparedWindow:
    path: Path
    byte_limit: int
    frames: OrderedDict[tuple[Any, ...], tuple[pl.DataFrame, int]] = field(default_factory=OrderedDict)
    retained_bytes: int = 0
    hits: int = 0
    misses: int = 0
    source_rows: int = 0
    key_rows: int = 0
    peak_bytes: int = 0

    def get(self, key: tuple[Any, ...]) -> pl.DataFrame | None:
        cached = self.frames.pop(key, None)
        if cached is None:
            self.misses += 1
            return None
        self.frames[key] = cached
        self.hits += 1
        return cached[0].clone()

    def put(self, key: tuple[Any, ...], frame: pl.DataFrame) -> None:
        charge = 4096 + int(frame.estimated_size())
        if charge > self.byte_limit:
            return
        while self.frames and (self.retained_bytes + charge > self.byte_limit or len(self.frames) >= 128):
            _, (_, removed) = self.frames.popitem(last=False)
            self.retained_bytes -= removed
        self.frames[key] = frame.clone(), charge
        self.retained_bytes += charge
        self.peak_bytes = max(self.peak_bytes, self.retained_bytes)


_PREPARED: ContextVar[PreparedWindow | None] = ContextVar("item_prepared_window", default=None)


def current_preparation(path: Path) -> PreparedWindow | None:
    state = _PREPARED.get()
    return state if state is not None and state.path == path.resolve() else None


@contextmanager
def prepared_window(path: Path, byte_limit: int) -> Iterator[PreparedWindow]:
    """Never retain values or proof validity beyond the coordinator's window."""
    state = PreparedWindow(path.resolve(), byte_limit)
    token = _PREPARED.set(state)
    try:
        yield state
    finally:
        state.frames.clear()
        state.retained_bytes = 0
        _PREPARED.reset(token)
