"""Stateless transform pipeline."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import polars as pl

TransformStep = Callable[[pl.DataFrame], pl.DataFrame]


@dataclass(frozen=True, slots=True)
class TransformPipeline:
    """Immutable chain of DataFrame transforms."""

    steps: tuple[TransformStep, ...] = field(default_factory=tuple)

    def add(self, step: TransformStep) -> TransformPipeline:
        return TransformPipeline(steps=(*self.steps, step))

    def run(self, data: pl.DataFrame) -> pl.DataFrame:
        result = data.clone()
        for step in self.steps:
            result = step(result)
        return result


class Transform:
    """Fluent transform builder."""

    def __init__(self, pipeline: TransformPipeline | None = None) -> None:
        self._pipeline = pipeline or TransformPipeline()

    def validate(
        self, predicate: Callable[[pl.DataFrame], Any] | None = None
    ) -> Transform:
        def step(frame: pl.DataFrame) -> pl.DataFrame:
            if predicate is not None:
                predicate(frame)
            return frame

        return Transform(self._pipeline.add(step))

    def run(self, data: pl.DataFrame) -> pl.DataFrame:
        return self._pipeline.run(data)
