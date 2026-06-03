"""Stateless transform pipeline."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

TransformStep = Callable[[pd.DataFrame], pd.DataFrame]


@dataclass(frozen=True, slots=True)
class TransformPipeline:
    """Immutable chain of DataFrame transforms."""

    steps: tuple[TransformStep, ...] = field(default_factory=tuple)

    def add(self, step: TransformStep) -> TransformPipeline:
        """Return a new pipeline with one extra step."""

        return TransformPipeline(steps=(*self.steps, step))

    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """Run all steps on a defensive copy."""

        result = data.copy(deep=True)
        for step in self.steps:
            result = step(result)
        return result


class Transform:
    """Fluent transform builder."""

    def __init__(self, pipeline: TransformPipeline | None = None) -> None:
        self._pipeline = pipeline or TransformPipeline()

    def align(
        self,
        *,
        index: pd.Index | None = None,
        columns: pd.Index | None = None,
    ) -> Transform:
        """Align frames to an optional index and column set."""

        def step(frame: pd.DataFrame) -> pd.DataFrame:
            target_index = index if index is not None else frame.index
            target_columns = columns if columns is not None else frame.columns
            return frame.reindex(index=target_index, columns=target_columns)

        return Transform(self._pipeline.add(step))

    def validate(
        self,
        predicate: Callable[[pd.DataFrame], Any] | None = None,
    ) -> Transform:
        """Add an optional validation step."""

        def step(frame: pd.DataFrame) -> pd.DataFrame:
            if predicate is not None:
                predicate(frame)
            return frame

        return Transform(self._pipeline.add(step))

    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """Run the pipeline."""

        return self._pipeline.run(data)
