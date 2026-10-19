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
    encoding_counts: dict[str, int] = field(default_factory=dict)

    def get(self, key: tuple[Any, ...], *, count: bool = True) -> pl.DataFrame | None:
        cached = self.frames.pop(key, None)
        if cached is None:
            self.misses += int(count)
            return None
        self.frames[key] = cached
        self.hits += int(count)
        return cached[0].clone()

    def put(self, key: tuple[Any, ...], frame: pl.DataFrame) -> None:
        charge = 4096 + int(frame.estimated_size())
        if charge > self.byte_limit:
            return
        previous = self.frames.pop(key, None)
        if previous is not None:
            self.retained_bytes -= previous[1]
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
        from bagelquant_data.pipeline.versions import _record_key_preparation
        with _record_key_preparation(state.path, lambda key: state.get(key, count=False),
                                     state.put, state.encoding_counts):
            yield state
    finally:
        state.frames.clear()
        state.retained_bytes = 0
        _PREPARED.reset(token)
