"""Commit availability-dated records and their durable recovery evidence."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast
from zoneinfo import ZoneInfo

import polars as pl

from bagelquant_data.core.dataset import DatasetSpec, record_key
from bagelquant_data.core.types import DateLike
from bagelquant_data.core.schema import (
    concat_compatible_frames,
    align_frame,
    compatible_schema,
)
from bagelquant_data.storage.parquet import (
    ParquetStore,
    finalize_partition_writes,
    rollback_partition_writes,
    partition_write_context,
)
from bagelquant_data.storage.recovery import append_batch, sort_versions


VERSION_FIELDS = frozenset(
    {
        "ingested_at",
        "_commit_seq",
        "_record_id",
        "_payload_hash",
        "_snapshot_id",
        "snapshot_date",
        "_baseline",
    }
)


def commit_versions(
    spec: DatasetSpec,
    frame: pl.DataFrame,
    parquet: ParquetStore,
    *,
    run_id: str,
    mode: str = "incremental",
    ingested_at: datetime | None = None,
    requests: list[dict] | None = None,
):
    from bagelquant_data.pipeline.commit import CommitResult

    if mode not in {"initialize", "incremental", "refresh"}:
        raise ValueError("mode must be initialize, incremental, or refresh")
    now = ingested_at or datetime.now(UTC)
    if now.tzinfo is None:
        raise ValueError("ingested_at must be timezone-aware")
    now = now.astimezone(UTC)
    available = now.astimezone(ZoneInfo(spec.availability_timezone)).date() + timedelta(
        days=spec.availability_day_offset
    )
    root = parquet.paths.dataset_root(spec.source, spec.name)
    manifests = parquet.metadata.manifest(spec.source, spec.name)
    if mode == "initialize":
        initialization = parquet.metadata._rows(
            "select status from dataset_initializations where source=? and dataset=?",
            (spec.source, spec.name),
        )
        if initialization and initialization[0]["status"] != "running":
            raise ValueError("Historical initialization is already complete")
        previous = parquet.metadata._rows(
            "select mode,run_id from version_commits where source=? and dataset=? and status='committed'",
            (spec.source, spec.name),
        )
        if previous and not initialization:
            raise ValueError("Historical initialization requires a new dataset")
        if any(row["mode"] != "initialize" for row in previous):
            raise ValueError(
                "Historical initialization cannot backdate an existing incremental dataset"
            )
    reserved = VERSION_FIELDS.intersection(frame.columns)
    if reserved:
        raise ValueError(
            f"Provider input contains reserved version columns: {sorted(reserved)}"
        )
    source_times = (
        frozenset(str(v) for v in frame["source_time"].unique())
        if "source_time" in frame.columns
        else frozenset()
    )
    if spec.update_type == "by_daily":
        checked_rows = frame.height
        keys = list(record_key(spec))
        frame = frame.with_columns(
            pl.struct(keys).struct.json_encode().alias("_record_id")
        )
        payload = sorted(set(frame.columns) - {"time", "year", "month", "_record_id"})
        frame = frame.with_columns(
            pl.struct(payload)
            .struct.json_encode()
            .map_elements(
                lambda text: hashlib.sha256(text.encode()).hexdigest(),
                return_dtype=pl.String,
            )
            .alias("_payload_hash")
        )
        conflicts = (
            frame.group_by("_record_id")
            .agg(pl.col("_payload_hash").n_unique())
            .filter(pl.col("_payload_hash") > 1)
        )
        if conflicts.height:
            conflict_id = str(
                conflicts.sort("_record_id")["_record_id"].item(0)
            )
            conflict_rows = frame.filter(pl.col("_record_id") == conflict_id)
            differing_fields = [
                name
                for name in payload
                if conflict_rows[name].n_unique() > 1
            ]
            raise ValueError(
                "Conflicting rows for a single observation in one provider response: "
                f"record_id={conflict_id}; row_count={conflict_rows.height}; "
                f"differing_fields={differing_fields}"
            )
        frame = frame.unique("_record_id", maintain_order=True)
        if manifests and frame.height:
            from bagelquant_data.query.raw import RawQueryService

            observation_start = cast(DateLike, frame["source_time"].min())
            observation_end = cast(DateLike, frame["source_time"].max())
            old = (
                RawQueryService(parquet, parquet.metadata)
                .query(
                    spec.name,
                    source=spec.source,
                    observation_start=observation_start,
                    observation_end=observation_end,
                    fields=["_record_id", "_payload_hash"],
                )
                .collect()
            )
            if old.height:
                frame = frame.join(old, on=["_record_id", "_payload_hash"], how="anti")
        if frame.is_empty():
            with parquet.metadata.connect() as db:
                db.execute(
                    "insert into version_checks(source,dataset,run_id,checked_at,visible_commit,request_json,row_count) "
                    "values(?,?,?,?,(select max(seq) from version_commits where source=? and dataset=? and status='committed'),?,?)",
                    (spec.source, spec.name, run_id, now.isoformat(), spec.source, spec.name,
                     json.dumps(requests or [], sort_keys=True, default=str), checked_rows),
                )
            return CommitResult(0, 0, len({value[:7] for value in source_times}), 0, present_times=source_times)
        frame = frame.with_columns(
            (
                pl.col("source_time")
                if mode == "initialize"
                else pl.max_horizontal(pl.col("source_time"), pl.lit(available))
            ).alias("time")
        )
    else:
        frame = frame.unique(maintain_order=True)
        if manifests:
            from bagelquant_data.core.hashing import frame_content_hash
            from bagelquant_data.query.raw import RawQueryService

            current_spec_hash = parquet.metadata.dataset_spec_hash(
                spec.source, spec.name
            )
            latest_commit = parquet.metadata._rows(
                "select spec_hash from version_commits "
                "where source=? and dataset=? and status='committed' "
                "order by seq desc limit 1",
                (spec.source, spec.name),
            )
            definition_is_current = bool(
                latest_commit
                and latest_commit[0]["spec_hash"] == current_spec_hash
            )

            current = (
                RawQueryService(parquet, parquet.metadata)
                .query_general(spec.name, source=spec.source)
                .collect()
            )
            current = current.drop(
                [name for name in VERSION_FIELDS if name in current.columns]
            )
            same_schema = set(current.columns) == set(frame.columns) and all(
                current.schema[name] == frame.schema[name] for name in frame.columns
            )
            if (
                definition_is_current
                and same_schema
                and frame_content_hash(current.select(frame.columns))
                == frame_content_hash(frame)
            ):
                with parquet.metadata.connect() as db:
                    db.execute(
                        "insert into version_checks(source,dataset,run_id,checked_at,visible_commit,request_json,row_count) "
                        "values(?,?,?,?,(select max(seq) from version_commits where source=? and dataset=? and status='committed'),?,?)",
                        (
                            spec.source,
                            spec.name,
                            run_id,
                            now.isoformat(),
                            spec.source,
                            spec.name,
                            json.dumps(requests or [], sort_keys=True, default=str),
                            frame.height,
                        ),
                    )
                return CommitResult(0, 0, 1, 0)
        frame = frame.with_columns(pl.lit(available).alias("snapshot_date"))
    with parquet.metadata.connect() as db:
        seq = db.execute(
            "insert into version_commits(source,dataset,run_id,ingested_at,pit_date,mode,status,spec_hash,request_json) values(?,?,?,?,?,?,'prepared',?,?)",
            (
                spec.source,
                spec.name,
                run_id,
                now.isoformat(),
                available.isoformat(),
                mode,
                parquet.metadata.dataset_spec_hash(spec.source, spec.name),
                json.dumps(requests or [], sort_keys=True, default=str),
            ),
        ).lastrowid
    if seq is None:
        raise RuntimeError("Failed to allocate an ingestion commit sequence")
    frame = frame.with_columns(
        pl.lit(now, dtype=pl.Datetime("us", "UTC")).alias("ingested_at"),
        pl.lit(seq, dtype=pl.Int64).alias("_commit_seq"),
        pl.lit(mode == "initialize").alias("_baseline"),
    )
    if spec.update_type == "general":
        frame = frame.with_columns(pl.lit(str(seq)).alias("_snapshot_id"))
    partition_field = "snapshot_date" if spec.update_type == "general" else "time"
    groups = frame.with_columns(
        pl.col(partition_field).dt.strftime("%Y-%m").alias("_partition")
    ).partition_by("_partition", as_dict=True, maintain_order=True)
    if not groups:
        groups = {
            (available.strftime("%Y-%m"),): frame.with_columns(
                pl.lit(None, dtype=pl.String).alias("_partition")
            )
        }
    writes = []
    batches = []
    stored_schema = parquet.canonical_schema(spec.source, spec.name)
    schemas = [frame.schema, *([stored_schema] if stored_schema is not None else [])]
    if spec.update_type == "by_daily":
        frame = align_frame(frame, compatible_schema(schemas))
        groups = frame.with_columns(pl.col(partition_field).dt.strftime("%Y-%m").alias("_partition")).partition_by(
            "_partition", as_dict=True, maintain_order=True
        )
    try:
        for (month,), group in groups.items():
            delta = group.drop("_partition")
            partition = f"year={month[:4]}/month={month[5:]}/data.parquet"
            path = root / partition
            manifest = next(
                (m for m in manifests if m["partition_path"] == partition), None
            )
            if manifest is not None and not path.is_file():
                raise RuntimeError(
                    "Committed partition is missing; restore ingestion evidence before updating"
                )
            if manifest is not None:
                old = pl.read_parquet(path, hive_partitioning=False)
                from bagelquant_data.core.hashing import frame_content_hash

                if frame_content_hash(old) != manifest["content_hash"]:
                    raise RuntimeError(
                        "Committed partition is damaged; repair it before updating"
                    )
                merged = concat_compatible_frames([old, delta])
            else:
                merged = delta
            merged = sort_versions(merged)
            if spec.update_type != "general":
                delta = align_frame(delta, merged.schema)
            batches.append(append_batch(root, partition, int(seq), delta))
            values = {
                "year": int(month[:4]),
                "month": int(month[5:]),
                "versioned": True,
            }
            if "source_time" in merged.columns and merged.height:
                values.update(
                    min_source_time=str(merged["source_time"].min()),
                    max_source_time=str(merged["source_time"].max()),
                )
            context = partition_write_context(merged.schema)
            writes.append(
                parquet.write_partition_file_result(
                    spec,
                    merged,
                    Path(partition),
                    values,
                    existing_manifest=manifest,
                    retain_backup=True,
                    write_context=context,
                )
            )
            schemas.append(merged.schema)
        canonical = compatible_schema(schemas)
        parquet.commit_metadata(
            spec,
            canonical,
            [w.manifest for w in writes],
            version_commit={"seq": seq, "batches": batches},
        )
    except BaseException:
        rollback_partition_writes(writes)
        raise
    finalize_partition_writes(writes)
    return CommitResult(
        frame.height,
        len(writes),
        0,
        sum(w.bytes_written for w in writes),
        present_times=source_times,
    )
