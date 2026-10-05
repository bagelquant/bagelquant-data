"""Minimal lazy query facade."""

from __future__ import annotations

from collections.abc import Sequence

import polars as pl

from bagelquant_data.core.dataset import incremental_key
from bagelquant_data.core.exceptions import ConfigurationError
from bagelquant_data.core.types import DateLike
from bagelquant_data.management.datasets import DatasetManager
from bagelquant_data.query.raw import RawQueryService


class _RawReader:
    """Read general and canonical-keyed datasets as Polars LazyFrames."""

    def __init__(self, raw_service: RawQueryService, datasets: DatasetManager) -> None:
        self._raw = raw_service
        self._datasets = datasets
        from bagelquant_data.inputs import (
            current_input_boundary,
            current_input_check_boundary,
        )

        self._max_commit, self._as_of = current_input_boundary(
            raw_service.metadata.data_meta_path
        )
        self._max_check_id = current_input_check_boundary(raw_service.metadata.data_meta_path)

    def _version_options(self, options: dict) -> dict:
        if self._max_check_id is not None:
            selected_check = options.get("max_check_id")
            selected_check = (
                self._max_check_id if selected_check is None else selected_check
            )
            if selected_check > self._max_check_id:
                raise ValueError(
                    "query cannot exceed its frozen check evidence boundary"
                )
            options["max_check_id"] = selected_check
        if self._max_commit is not None:
            selected = options.get("max_commit")
            selected = self._max_commit if selected is None else selected
            if int(selected) > self._max_commit:
                raise ValueError("query cannot exceed its frozen committed source view")
            options["max_commit"] = selected
        if self._as_of is not None:
            from bagelquant_data.query.raw import _date_value

            cutoff = options.get("as_of_date")
            if cutoff is not None and _date_value(cutoff) > self._as_of:
                raise ValueError("query cannot exceed its information cutoff")
            options["as_of_date"] = self._as_of if cutoff is None else cutoff
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

    def snapshots(self, dataset: str, *, source: str) -> list[dict]:
        """List complete General snapshots, including empty snapshots, at this read boundary."""
        if self._datasets.get(dataset, source=source).update_type != "general":
            raise ConfigurationError("snapshots requires a General dataset")
        from .raw import _date_value

        rows = self._raw._commits(source, dataset, None, self._max_commit)
        return (
            rows
            if self._as_of is None
            else [row for row in rows if _date_value(row["pit_date"]) <= self._as_of]
        )

    def version_evidence(
        self,
        dataset: str,
        *,
        source: str,
        as_of_date: DateLike | None = None,
        observation_start: DateLike | None = None,
        observation_end: DateLike | None = None,
    ) -> list[dict]:
        """Immutable visible batch identities for dependency planning, without file reads.

        General returns its selected complete snapshot. Daily returns the batches
        containing eligible observation versions; rechecks never change this proof.
        """
        from .raw import _date_value

        as_of_date = self._version_options({"as_of_date": as_of_date}).get("as_of_date")
        spec = self._datasets.get(dataset, source=source)
        upper = str(_date_value(as_of_date)) if as_of_date is not None else None
        first = (
            str(_date_value(observation_start))
            if observation_start is not None
            else None
        )
        last = (
            str(_date_value(observation_end)) if observation_end is not None else None
        )
        conditions = ["c.source=?", "c.dataset=?", "c.status='committed'"]
        parameters: list[str | int] = [source, dataset]
        if self._max_commit is not None:
            conditions.append("b.commit_seq<=?")
            parameters.append(self._max_commit)
        if upper is not None:
            conditions.append(
                "(c.mode='initialize' OR coalesce(b.min_available,c.pit_date)<=?)"
                if spec.update_type == "general"
                else "coalesce(b.min_available,c.pit_date)<=?"
            )
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
            "where "
            + " and ".join(conditions)
            + " order by b.commit_seq,b.partition_path",
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

    def observations(
        self,
        dataset: str,
        *,
        source: str,
        start=None,
        end=None,
        assets=None,
        fields=None,
        as_of_date=None,
        max_commit=None,
        strict: bool = False,
        view: str | None = None,
    ) -> pl.LazyFrame:
        """Return the numerical observation axis after PIT version selection.

        Without a cutoff, each observation uses the version available on its own
        date. An explicit cutoff resolves a historical input window at that date.
        """
        frame = self.query(
            dataset,
            source=source,
            observation_start=start,
            observation_end=end,
            assets=assets,
            as_of_date=as_of_date,
            view=view or ("history" if as_of_date is None else "latest"),
            max_commit=max_commit,
            strict=strict,
        )
        frame = frame.with_columns(pl.col("source_time").alias("time"))
        return frame if fields is None else frame.select(fields)
