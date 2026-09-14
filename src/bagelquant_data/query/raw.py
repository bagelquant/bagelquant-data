"""Version-aware, manifest-only record queries."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import date, datetime
import polars as pl
from bagelquant_data.core.types import DateLike
from bagelquant_data.core.exceptions import DatasetNotFoundError
from bagelquant_data.core.schema import align_lazy_frame, compatible_schema
from bagelquant_data.storage.metadata import MetadataStore
from bagelquant_data.storage.parquet import ParquetStore


class RawQueryService:
    def __init__(self, parquet: ParquetStore, metadata: MetadataStore) -> None:
        self.parquet, self.metadata = parquet, metadata

    def query_general(
        self,
        dataset: str,
        *,
        source: str,
        fields: Sequence[str] | None = None,
        as_of_date: DateLike | None = None,
        ingested_before: datetime | None = None,
        snapshot_id: str | None = None,
        max_commit: int | None = None,
        view: str = "latest",
    ) -> pl.LazyFrame:
        frame = self._scan(dataset, source=source)
        names = frame.collect_schema().names()
        if "_snapshot_id" in names:
            commits = self._commits(source, dataset, ingested_before, max_commit)
            if as_of_date is not None:
                commits = [
                    c
                    for c in commits
                    if _date_value(c["pit_date"]) <= _date_value(as_of_date)
                ]
            ids = [
                int(c["seq"])
                for c in commits
                if snapshot_id is None or str(c["seq"]) == snapshot_id
            ]
            if view not in {"latest", "versions"}:
                raise ValueError("view must be latest or versions")
            frame = frame.filter(pl.col("_commit_seq").is_in(ids)) if view == "versions" else frame.filter(pl.col("_commit_seq") == (max(ids) if ids else -1))
            if view == "latest" and ids:
                import pyarrow as pa

                schema_rows = self.metadata._rows("select schema_ipc from version_batches where commit_seq=? limit 1", (max(ids),))
                if schema_rows:
                    names = pa.ipc.read_schema(pa.BufferReader(schema_rows[0]["schema_ipc"])).names
                    frame = frame.select(names)
        return (
            frame if fields is None else frame.select([f for f in fields if f in names])
        )

    def query(
        self,
        dataset: str,
        *,
        source: str,
        start: DateLike | None = None,
        end: DateLike | None = None,
        assets: Sequence[str] | None = None,
        fields: Sequence[str] | None = None,
        view: str = "latest",
        as_of_date: DateLike | None = None,
        ingested_before: datetime | None = None,
        observation_start: DateLike | None = None,
        observation_end: DateLike | None = None,
        max_commit: int | None = None,
    ) -> pl.LazyFrame:
        if view not in {"latest", "versions", "history"}:
            raise ValueError("view must be latest, versions, or history")
        frame = self._scan(
            dataset,
            source=source,
            observation_start=observation_start,
            observation_end=observation_end,
            upper_available=as_of_date,
        )
        names = frame.collect_schema().names()
        if "_commit_seq" in names:
            visible = [
                int(c["seq"])
                for c in self._commits(source, dataset, ingested_before, max_commit)
            ]
            frame = frame.filter(pl.col("_commit_seq").is_in(visible))
        if assets is not None:
            frame = frame.filter(pl.col("asset_id").is_in(list(assets)))
        observation = "source_time" if "source_time" in names else "time"
        if observation_start is not None:
            frame = frame.filter(pl.col(observation) >= _date_value(observation_start))
        if observation_end is not None:
            frame = frame.filter(pl.col(observation) <= _date_value(observation_end))
        if as_of_date is not None:
            frame = frame.filter(pl.col("time") <= _date_value(as_of_date))
        if view == "history":
            frame = frame.filter(pl.col("time") <= pl.col(observation))
        if "_record_id" in names and view in {"latest", "history"}:
            frame = frame.sort("time", "ingested_at", "_commit_seq").unique(
                "_record_id", keep="last", maintain_order=True
            )
        # Availability filters are deliberately applied after resolving versions.
        if start is not None:
            frame = frame.filter(pl.col("time") >= _date_value(start))
        if end is not None:
            frame = frame.filter(pl.col("time") <= _date_value(end))
        return (
            frame if fields is None else frame.select([f for f in fields if f in names])
        )

    def _commits(self, source, dataset, ingested_before, max_commit):
        rows = self.metadata._rows(
            "select c.seq,c.mode,c.ingested_at,c.pit_date,c.spec_hash,"
            "coalesce((select sum(b.row_count) from version_batches b "
            "where b.commit_seq=c.seq),0) as row_count "
            "from version_commits c where c.source=? and c.dataset=? "
            "and c.status='committed' order by c.seq",
            (source, dataset),
        )
        if ingested_before is not None:
            if ingested_before.tzinfo is None:
                raise ValueError("ingested_before must be timezone-aware")
            rows = [
                r
                for r in rows
                if datetime.fromisoformat(r["ingested_at"]) <= ingested_before
            ]
        return [r for r in rows if max_commit is None or int(r["seq"]) <= max_commit]

    def _scan(
        self,
        dataset,
        *,
        source,
        observation_start=None,
        observation_end=None,
        upper_available=None,
    ):
        manifests = self.metadata.manifest(source, dataset)
        canonical = self.parquet.canonical_schema(source, dataset)
        if not manifests and canonical is None:
            raise DatasetNotFoundError(f"No canonical data for {source}/{dataset}")
        rows = []
        for row in manifests:
            values = row.get("partition_values", {})
            if isinstance(values, str):
                values = json.loads(values)
            low = values.get("min_source_time")
            high = values.get("max_source_time")
            if (
                observation_start is not None
                and high
                and _date_value(high) < _date_value(observation_start)
            ):
                continue
            if (
                observation_end is not None
                and low
                and _date_value(low) > _date_value(observation_end)
            ):
                continue
            if (
                upper_available is not None
                and row.get("min_time")
                and _date_value(row["min_time"]) > _date_value(upper_available)
            ):
                continue
            rows.append(row)
        if not rows:
            return pl.DataFrame(schema=canonical or {}).lazy()
        root = self.parquet.paths.dataset_root(source, dataset)
        grouped = {}
        for row in rows:
            path = root / row["partition_path"]
            if not path.is_file():
                raise DatasetNotFoundError(
                    f"Canonical manifest references missing partition: {path}"
                )
            grouped.setdefault(row["schema_hash"], []).append(str(path))
        frames = [
            pl.scan_parquet(paths, hive_partitioning=False)
            for paths in grouped.values()
        ]
        schema = compatible_schema(
            [
                *([canonical] if canonical is not None else []),
                *(f.collect_schema() for f in frames),
            ]
        )
        aligned = [align_lazy_frame(f, schema, list(schema)) for f in frames]
        return pl.concat(aligned, how="vertical") if len(aligned) > 1 else aligned[0]


def _date_value(value: DateLike) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).split("T", maxsplit=1)[0]
    return (
        date(int(text[:4]), int(text[4:6]), int(text[6:8]))
        if len(text) == 8 and text.isdigit()
        else date.fromisoformat(text)
    )
