"""Minimal lazy query facade."""

from __future__ import annotations

from collections.abc import Sequence

import polars as pl

from bagelquant_data.core.dataset import incremental_key
from bagelquant_data.core.exceptions import ConfigurationError
from bagelquant_data.core.types import DateLike
from bagelquant_data.management.datasets import DatasetManager
from bagelquant_data.query.raw import RawQueryService


class LakeQuery:
    """Read general and canonical-keyed datasets as Polars LazyFrames."""

    def __init__(self, raw_service: RawQueryService, datasets: DatasetManager) -> None:
        self._raw = raw_service
        self._datasets = datasets
        self._max_commit = None

    def query_general(
        self,
        dataset: str,
        *,
        source: str,
        fields: Sequence[str] | None = None,
        **version_options,
    ) -> pl.LazyFrame:
        """Read any dataset without `time` or `asset_id` filters."""

        if self._max_commit is not None:
            version_options.setdefault("max_commit", self._max_commit)
        return self._raw.query_general(
            dataset, source=source, fields=fields, **version_options
        )

    def freeze(self) -> int:
        """Freeze the committed input boundary for one computation."""
        rows = self._raw.metadata._rows("select coalesce(max(seq),0) as seq from version_commits where status='committed'")
        return int(rows[0]["seq"])

    def frozen(self) -> "LakeQuery":
        query = LakeQuery(self._raw, self._datasets)
        query._max_commit = self.freeze()
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
        rows = self._raw.metadata._rows(
            "select b.commit_seq,b.partition_path,b.content_hash,b.row_count,"
            "b.min_available,b.max_available,b.min_observation,b.max_observation,c.mode,c.pit_date "
            "from version_batches b join version_commits c on c.seq=b.commit_seq "
            "where c.source=? and c.dataset=? and c.status='committed' order by b.commit_seq,b.partition_path",
            (source, dataset),
        )
        upper = str(_date_value(as_of_date)) if as_of_date is not None else None
        first = str(_date_value(observation_start)) if observation_start is not None else None
        last = str(_date_value(observation_end)) if observation_end is not None else None
        selected = [row for row in rows
                    if (self._max_commit is None or row["commit_seq"] <= self._max_commit)
                    and (upper is None or (row["mode"] == "initialize" if spec.update_type == "general" else False)
                         or str(row["min_available"] or row["pit_date"]) <= upper)
                    and (first is None or row["max_observation"] is None or row["max_observation"] >= first)
                    and (last is None or row["min_observation"] is None or row["min_observation"] <= last)]
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
        if self._max_commit is not None:
            version_options.setdefault("max_commit", self._max_commit)
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
