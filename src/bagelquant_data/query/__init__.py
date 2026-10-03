"""Minimal lazy query facade."""

from __future__ import annotations

from collections.abc import Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import Iterator

import polars as pl

from bagelquant_data.core.dataset import incremental_key
from bagelquant_data.core.exceptions import ConfigurationError
from bagelquant_data.core.types import DateLike
from bagelquant_data.management.datasets import DatasetManager
from bagelquant_data.query.raw import RawQueryService


_READ_BOUNDARIES: ContextVar[dict[str, int]] = ContextVar("lake_read_boundaries", default={})


@contextmanager
def frozen_raw_reads(root: str | Path, max_commit: int) -> Iterator[None]:
    """Bind newly opened query facades to one explicit committed source view.

    Query objects capture this boundary and retain it when passed to workers.
    The scope never changes a lake or an existing reader. Nested scopes may
    narrow the boundary, but cannot escape their parent's frozen view.
    """
    if isinstance(max_commit, bool) or not isinstance(max_commit, int) or max_commit < 0:
        raise ValueError("max_commit must be a nonnegative integer")
    key = str(Path(root).resolve())
    boundaries = _READ_BOUNDARIES.get()
    if key in boundaries and max_commit > boundaries[key]:
        raise ValueError("a nested source view cannot exceed its frozen read boundary")
    token = _READ_BOUNDARIES.set({**boundaries, key: max_commit})
    try:
        yield
    finally:
        _READ_BOUNDARIES.reset(token)


class LakeQuery:
    """Read general and canonical-keyed datasets as Polars LazyFrames."""

    def __init__(self, raw_service: RawQueryService, datasets: DatasetManager) -> None:
        self._raw = raw_service
        self._datasets = datasets
        root = raw_service.metadata.path.resolve().parent.parent
        self._max_commit = _READ_BOUNDARIES.get().get(str(root))

    def _version_options(self, options: dict) -> dict:
        if self._max_commit is not None:
            selected = options.get("max_commit", self._max_commit)
            if selected is None or int(selected) > self._max_commit:
                raise ValueError("query cannot exceed its frozen committed source view")
            options["max_commit"] = selected
        return options

    def query_general(
        self,
        dataset: str,
        *,
        source: str,
        fields: Sequence[str] | None = None,
        **version_options,
    ) -> pl.LazyFrame:
        """Read any dataset without `time` or `asset_id` filters."""

        version_options = self._version_options(version_options)
        return self._raw.query_general(
            dataset, source=source, fields=fields, **version_options
        )

    def freeze(self) -> int:
        """Freeze the committed input boundary for one computation."""
        if self._max_commit is not None:
            return self._max_commit
        rows = self._raw.metadata._rows("select coalesce(max(seq),0) as seq from version_commits where status='committed'")
        return int(rows[0]["seq"])

    def frozen(self, *, max_commit: int | None = None) -> "LakeQuery":
        """Return an independent reader at an explicit or current boundary."""
        selected = self.freeze() if max_commit is None else max_commit
        if isinstance(selected, bool) or not isinstance(selected, int) or selected < 0:
            raise ValueError("max_commit must be a nonnegative integer")
        if selected > self.freeze():
            raise ValueError("requested source boundary is not available in this reader")
        query = LakeQuery(self._raw, self._datasets)
        query._max_commit = selected
        return query

    def snapshots(self, dataset: str, *, source: str) -> list[dict]:
        """List complete General snapshots, including empty snapshots, at this read boundary."""
        if self._datasets.get(dataset, source=source).update_type != "general":
            raise ConfigurationError("snapshots requires a General dataset")
        return self._raw._commits(source, dataset, None, self._max_commit)

    def version_evidence(self, dataset: str, *, source: str,
                         as_of_date: DateLike | None = None,
                         observation_start: DateLike | None = None,
                         observation_end: DateLike | None = None) -> list[dict]:
        """Immutable visible batch identities for dependency planning, without file reads.

        General returns its selected complete snapshot. Daily returns the batches
        containing eligible observation versions; rechecks never change this proof.
        """
        from .raw import _date_value

        spec = self._datasets.get(dataset, source=source)
        upper = str(_date_value(as_of_date)) if as_of_date is not None else None
        first = str(_date_value(observation_start)) if observation_start is not None else None
        last = str(_date_value(observation_end)) if observation_end is not None else None
        conditions = ["c.source=?", "c.dataset=?", "c.status='committed'"]
        parameters: list[str | int] = [source, dataset]
        if self._max_commit is not None:
            conditions.append("b.commit_seq<=?")
            parameters.append(self._max_commit)
        if upper is not None:
            conditions.append("(c.mode='initialize' OR coalesce(b.min_available,c.pit_date)<=?)" if spec.update_type == "general" else "coalesce(b.min_available,c.pit_date)<=?")
            parameters.append(upper)
        if first is not None:
            conditions.append("(b.max_observation IS NULL OR b.max_observation>=?)")
            parameters.append(first)
        if last is not None:
            conditions.append("(b.min_observation IS NULL OR b.min_observation<=?)")
            parameters.append(last)
        rows = self._raw.metadata._rows(
            "select b.commit_seq,b.partition_path,b.content_hash,b.row_count,"
            "b.min_available,b.max_available,b.min_observation,b.max_observation,c.mode,c.pit_date "
            "from version_batches b join version_commits c on c.seq=b.commit_seq "
            "where " + " and ".join(conditions) + " order by b.commit_seq,b.partition_path",
            tuple(parameters),
        )
        selected = rows
        if spec.update_type == "general" and selected:
            latest = max(row["commit_seq"] for row in selected)
            selected = [row for row in selected if row["commit_seq"] == latest]
        return selected

    def query(
        self,
        dataset: str,
        *,
        source: str,
        start: DateLike | None = None,
        end: DateLike | None = None,
        assets: Sequence[str] | None = None,
        fields: Sequence[str] | None = None,
        **version_options,
    ) -> pl.LazyFrame:
        """Read an incremental dataset filtered by its canonical key."""

        spec = self._datasets.get(dataset, source=source)
        if incremental_key(spec) is None:
            raise ConfigurationError(
                f"{source}/{dataset} is general; use query_general()"
            )
        version_options = self._version_options(version_options)
        return self._raw.query(
            dataset,
            source=source,
            start=start,
            end=end,
            assets=assets,
            fields=fields,
            **version_options,
        )

    def observations(self, dataset: str, *, source: str, start=None, end=None,
                     assets=None, fields=None, as_of_date=None) -> pl.LazyFrame:
        """Return the numerical observation axis after PIT version selection.

        Without a cutoff, each observation uses the version available on its own
        date. An explicit cutoff resolves a historical input window at that date.
        """
        frame = self.query(dataset, source=source, observation_start=start,
                           observation_end=end, assets=assets, as_of_date=as_of_date,
                           view="history" if as_of_date is None else "latest")
        frame = frame.with_columns(pl.col("source_time").alias("time"))
        return frame if fields is None else frame.select(fields)
