"""Version-aware, manifest-only record queries."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from bagelquant_data.storage.paths import LakePaths
import polars as pl
from bagelquant_data.core.types import DateLike
from bagelquant_data.core.exceptions import DatasetNotFoundError
from bagelquant_data.core.schema import align_lazy_frame, compatible_schema
from bagelquant_data.storage.data_meta import DataMetaStore
from bagelquant_data.storage.parquet import ParquetStore
from bagelquant_data.storage.atomic import _filesystem_path


class RawQueryService:
    def __init__(self, parquet: ParquetStore, metadata: DataMetaStore) -> None:
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
        strict: bool = False,
        max_check_id: int | None = None,
    ) -> pl.LazyFrame:
        snapshot = self.metadata.dataset_snapshot(source, dataset)
        frame = self._scan(dataset, source=source, snapshot=snapshot)
        frame = _attested_versions(frame, snapshot["checks"], snapshot["record_checks"],
                                   as_of_date=as_of_date, ingested_before=ingested_before, max_check_id=max_check_id,
                                   full_commit_checks=snapshot.get("full_commit_checks", ()), event_paths=self.parquet.paths)
        names = frame.collect_schema().names()
        if strict and "_baseline" in names:
            frame = frame.filter(~pl.col("_baseline"))
        if "_snapshot_id" in names:
            commits = self._commits(source, dataset, ingested_before, max_commit, snapshot)
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
            if as_of_date is not None:
                frame = frame.filter(pl.col("snapshot_date") <= _date_value(as_of_date))
            if view == "latest":
                frame = frame.filter(pl.col("snapshot_date") == pl.col("snapshot_date").max())
                frame = frame.filter(pl.col("ingested_at") == pl.col("ingested_at").max())
                frame = frame.filter(pl.col("_attestation_id").fill_null(0) == pl.col("_attestation_id").fill_null(0).max())
            if view == "latest" and ids:
                import pyarrow as pa

                schema_ipc = snapshot["batch_schemas"].get(max(ids))
                if schema_ipc:
                    names = [*pa.ipc.read_schema(pa.BufferReader(schema_ipc)).names, "_attestation_id"]
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
        strict: bool = False,
        max_check_id: int | None = None,
    ) -> pl.LazyFrame:
        if view not in {"latest", "versions", "history"}:
            raise ValueError("view must be latest, versions, or history")
        snapshot = self.metadata.dataset_snapshot(source, dataset)
        frame = self._scan(
            dataset,
            source=source,
            observation_start=observation_start,
            observation_end=observation_end,
            upper_available=as_of_date,
            snapshot=snapshot,
        )
        frame = _attested_versions(frame, snapshot["checks"], snapshot["record_checks"],
                                   as_of_date=as_of_date, ingested_before=ingested_before, max_check_id=max_check_id,
                                   full_commit_checks=snapshot.get("full_commit_checks", ()), event_paths=self.parquet.paths,
                                   observation_start=observation_start, observation_end=observation_end)
        names = frame.collect_schema().names()
        if strict and "_baseline" in names:
            frame = frame.filter(~pl.col("_baseline"))
        if "_commit_seq" in names:
            visible = [
                int(c["seq"])
                for c in self._commits(source, dataset, ingested_before, max_commit, snapshot)
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
            frame = frame.sort("time", "ingested_at", "_commit_seq", "_attestation_id").unique(
                "_record_id", keep="last", maintain_order=True
            )
        # Availability filters are deliberately applied after resolving versions.
        if start is not None:
            frame = frame.filter(pl.col("time") >= _date_value(start))
        if end is not None:
            frame = frame.filter(pl.col("time") <= _date_value(end))
        if view == "versions":
            frame = frame.sort([field for field in ("time",observation,"asset_id","ingested_at","_commit_seq","_attestation_id")
                if field in names])
        return (
            frame if fields is None else frame.select([f for f in fields if f in names])
        )

    def _commits(self, source, dataset, ingested_before, max_commit, snapshot=None):
        rows = (snapshot or self.metadata.dataset_snapshot(source, dataset))["commits"]
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
        snapshot=None,
    ):
        snapshot = snapshot or self.metadata.dataset_snapshot(source, dataset)
        manifests = snapshot["manifests"]
        schema_ipc = snapshot["schema_ipc"]
        if schema_ipc is not None:
            import pyarrow as pa
            canonical = pl.Schema(pa.ipc.read_schema(pa.BufferReader(schema_ipc)))
        else:
            canonical = None
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
        grouped = {}
        for row in rows:
            path = self.parquet.paths.generation_path(source, dataset, row["partition_path"], row["generation_path"])
            filesystem_path = _filesystem_path(path)
            if not os.path.isfile(filesystem_path):
                raise DatasetNotFoundError(
                    f"Canonical manifest references missing partition: {path}"
                )
            grouped.setdefault(row["schema_hash"], []).append(filesystem_path)
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
        if "source_time" in schema:
            # Observation bounds commute with per-key version resolution and
            # baseline visibility. Availability bounds deliberately stay later.
            if observation_start is not None:
                aligned = [f.filter(pl.col("source_time") >= _date_value(observation_start)) for f in aligned]
            if observation_end is not None:
                aligned = [f.filter(pl.col("source_time") <= _date_value(observation_end)) for f in aligned]
            definition = self.metadata.get_dataset(source,dataset)
            extra = () if definition is None else tuple(json.loads(definition["spec_json"]).get("primary_key_extra",()))
            keys = ["source_time","asset_id",*extra]
            from bagelquant_data.storage.recovery import with_record_key
            keyed_schema = {**dict(schema),"_record_id":pl.String}
            aligned = [f.map_batches(lambda frame:with_record_key(frame,keys=keys),schema=keyed_schema,
                predicate_pushdown=False,projection_pushdown=False,slice_pushdown=False) for f in aligned]
        return pl.concat(aligned, how="vertical") if len(aligned) > 1 else aligned[0]


def _attested_versions(
    frame: pl.LazyFrame, checks: Sequence[Mapping], record_checks: Sequence[Mapping], *,
    as_of_date: DateLike | None = None, ingested_before: datetime | None = None,
    max_check_id: int | None = None,
    full_commit_checks: Sequence[Mapping] = (),
    event_paths: LakePaths | None = None,
    observation_start: DateLike | None = None,
    observation_end: DateLike | None = None,
) -> pl.LazyFrame:
    """Overlay exact unchanged-content witnesses without replacing stored history.

    Only baseline content needs a witnessed copy: already observed nonbaseline
    content retains its first availability. Frozen replay supplies its captured
    checks so later collections cannot change this result.
    """
    names = frame.collect_schema().names()
    if "_baseline" not in names or "_commit_seq" not in names:
        return frame
    if ingested_before is not None and ingested_before.tzinfo is None:
        raise ValueError("ingested_before must be timezone-aware")
    cutoff = None if as_of_date is None else _date_value(as_of_date)
    eligible = {
        int(check["id"]): check for check in checks
        if not bool(check["baseline"])
        and (max_check_id is None or int(check["id"]) <= max_check_id)
        and (cutoff is None or _date_value(check["pit_date"]) <= cutoff)
        and (ingested_before is None or datetime.fromisoformat(str(check["checked_at"])) <= ingested_before)
    }
    schema = {"_commit_seq": pl.Int64, "_attested_date": pl.Date,
              "_attested_ingested": pl.Datetime("us", "UTC"), "_attestation_id": pl.Int64}
    general = "_snapshot_id" in names
    if not general and "_record_id" not in names:
        # A bounded window can contain no original observations while retaining
        # visibility events for other dates. Its canonical empty schema has no
        # temporary natural-key identity, and those events cannot add rows.
        # A nonempty unidentified frame is damaged input, never verified absence.
        if frame.select(pl.len()).collect().item():
            raise RuntimeError("Visibility overlay requires natural-key identities for nonempty observations")
        return frame.with_columns(pl.lit(None, dtype=pl.Int64).alias("_attestation_id"))
    keys = ["_commit_seq"] if general else ["_commit_seq", "_record_id", "_payload_hash"]
    witnesses = []
    if general:
        witnesses = [
            {"_commit_seq": int(check["visible_commit"]), "_attested_date": _date_value(check["pit_date"]),
             "_attested_ingested": datetime.fromisoformat(str(check["checked_at"])), "_attestation_id": check_id}
            for check_id, check in eligible.items() if check["visible_commit"] is not None
        ]
    elif "_record_id" in names and "_payload_hash" in names:
        schema.update({"_record_id": pl.String, "_payload_hash": pl.String})
        witnesses = [
            {"_commit_seq": int(record["version_commit"]), "_record_id": record["record_id"],
             "_payload_hash": record["payload_hash"], "_attested_date": _date_value(record["available_date"]),
             "_attested_ingested": datetime.fromisoformat(str(eligible[int(record["check_id"])]["checked_at"])),
             "_attestation_id": int(record["check_id"])}
            for record in record_checks if not record.get("event") and int(record["check_id"]) in eligible
            and (cutoff is None or _date_value(record["available_date"]) <= cutoff)
        ]
    originals = frame.with_columns(pl.lit(None, dtype=pl.Int64).alias("_attestation_id"))
    copies = []
    from bagelquant_data.storage.full_commit_checks import validate_seal
    for seal in full_commit_checks:
        validate_seal(seal)
        check = seal["binding"]["check"]
        if int(check["id"]) not in eligible or cutoff is not None and _date_value(seal["available_date"]) > cutoff:
            continue
        copies.append(frame.filter(pl.col("_baseline") & (pl.col("_commit_seq") == int(check["visible_commit"]))).with_columns(
            pl.max_horizontal(pl.col("time"), pl.lit(_date_value(seal["available_date"]))).alias("time"),
            pl.lit(datetime.fromisoformat(str(check["checked_at"])), dtype=pl.Datetime("us", "UTC")).alias("ingested_at"),
            pl.lit(False).alias("_baseline"), pl.lit(int(check["id"]), dtype=pl.Int64).alias("_attestation_id")))
    for event in record_checks:
        if not event.get("event") or int(event["check_id"]) not in eligible:
            continue
        if event_paths is None:
            raise RuntimeError("Visibility events require their configured original lake")
        check = eligible[int(event["check_id"])]
        base = event_paths.dataset_root(str(event["source"]),str(event["dataset"]))
        path = (base / str(event["generation_path"])).resolve()
        if not path.is_relative_to(base) or path == base:
            raise RuntimeError("Visibility event reference escapes its dataset")
        event_frame = pl.scan_parquet(path,hive_partitioning=False).rename({"version_commit":"_commit_seq","available_date":"_attested_date"})
        # Retain the full typed natural key while pruning unrelated observations
        # before Python key encoding. Do not restrict first-known availability
        # to the requested observation dates: genuinely late events remain late.
        if observation_start is not None:
            event_frame = event_frame.filter(pl.col("source_time") >= _date_value(observation_start))
        if observation_end is not None:
            event_frame = event_frame.filter(pl.col("source_time") <= _date_value(observation_end))
        natural_keys = [name for name in event_frame.collect_schema().names() if name != "_attested_date"]
        from bagelquant_data.storage.recovery import with_record_key
        # SQL-style equality drops null keys and merges signed float zero.
        # Match complete typed natural keys temporarily; originals store no hash.
        observation_keys = [name for name in natural_keys if name != "_commit_seq"]
        event_schema = {**dict(event_frame.collect_schema()),"_record_id":pl.String}
        event_frame = event_frame.map_batches(lambda part, keys=observation_keys:with_record_key(part,keys=keys),
            schema=event_schema,predicate_pushdown=False,projection_pushdown=False,slice_pushdown=False)
        witnessed = frame.filter(pl.col("_baseline")).join(
            event_frame.select("_commit_seq","_record_id","_attested_date"),
            on=["_commit_seq","_record_id"],how="inner")
        if cutoff is not None:
            witnessed = witnessed.filter(pl.col("_attested_date") <= cutoff)
        # An Item baseline's supplied version date is unknown history, not an
        # observed publication floor. Its first verified result is constrained
        # by the observation and declared availability, then this actual event.
        availability_axes = (["source_time","available_date"] if "_item_definition_hash" in names else ["time"])
        witnessed = witnessed.with_columns(pl.max_horizontal(*availability_axes,"_attested_date").alias("time"),
            pl.lit(datetime.fromisoformat(str(check["checked_at"])),dtype=pl.Datetime("us","UTC")).alias("ingested_at"),
            pl.lit(False).alias("_baseline"),pl.lit(int(event["check_id"]),dtype=pl.Int64).alias("_attestation_id"))
        copies.append(witnessed.select(originals.collect_schema().names()))
    if not witnesses:
        return pl.concat([originals, *copies], how="vertical") if copies else originals
    witnessed = frame.filter(pl.col("_baseline")).join(pl.DataFrame(witnesses, schema=schema).lazy(), on=keys, how="inner")
    axis = "snapshot_date" if general else "time"
    witnessed = witnessed.with_columns(
        pl.max_horizontal(pl.col(axis), pl.col("_attested_date")).alias(axis),
        pl.col("_attested_ingested").alias("ingested_at"),
        pl.lit(False).alias("_baseline"),
    ).drop("_attested_date", "_attested_ingested")
    return pl.concat([originals, witnessed.select(originals.collect_schema().names()), *copies], how="vertical")


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
