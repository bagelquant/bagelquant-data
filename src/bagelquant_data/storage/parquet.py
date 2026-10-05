"""Canonical Parquet storage."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import polars as pl
import pyarrow as pa

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.hashing import frame_content_hash
from bagelquant_data.storage.atomic import _filesystem_path, atomic_write_parquet
from bagelquant_data.storage.data_meta import DataMetaStore
from bagelquant_data.storage.paths import LakePaths

_LOAD_EXISTING_MANIFEST = object()


@dataclass(frozen=True, slots=True)
class PartitionWriteContext:
    """Schema values shared by every file write in one canonical commit."""

    schema_hash: str
    arrow_schema: pa.Schema
    schema_ipc: bytes


@dataclass(frozen=True, slots=True)
class PartitionWriteResult:
    """Result of comparing and optionally publishing one canonical partition."""

    path: Path
    manifest: dict[str, Any]
    rewritten: bool
    bytes_written: int


class ParquetStore:
    """Read and write canonical lake Parquet files."""

    def __init__(self, paths: LakePaths, metadata: DataMetaStore) -> None:
        self.paths = paths
        self.metadata = metadata

    def write_partition_file_result(
        self,
        spec: DatasetSpec,
        frame: pl.DataFrame,
        relative_path: Path,
        partition_values: dict[str, Any] | None = None,
        *,
        existing_manifest: dict[str, Any] | None | object = _LOAD_EXISTING_MANIFEST,
        write_context: PartitionWriteContext | None = None,
    ) -> PartitionWriteResult:
        """Write one changed partition and retain a byte-identical manifest on no-op."""

        self.metadata.ensure_writable()
        root = self.paths.dataset_root(spec.source, spec.name)
        if relative_path.is_absolute() or ".." in relative_path.parts:
            raise ValueError("Partition path escapes dataset root")
        path = root / relative_path
        time_values = (
            frame.select(
                pl.min("time").alias("min_time"), pl.max("time").alias("max_time")
            ).row(0)
            if "time" in frame.columns and frame.height
            else (None, None)
        )
        content_hash = frame_content_hash(frame)
        if existing_manifest is _LOAD_EXISTING_MANIFEST:
            existing_manifest = next(
                (
                    row
                    for row in self.metadata.manifest(spec.source, spec.name)
                    if row["partition_path"] == relative_path.as_posix()
                ),
                None,
            )
        else:
            existing_manifest = cast(dict[str, Any] | None, existing_manifest)
        if existing_manifest is not None:
            path = self.paths.generation_path(spec.source, spec.name, existing_manifest["partition_path"], existing_manifest["generation_path"])
        if write_context is None:
            write_context = partition_write_context(pl.Schema(frame.schema))
        if (
            existing_manifest is not None
            and existing_manifest.get("content_hash") == content_hash
            and _is_file(path)
        ):
            return PartitionWriteResult(
                path, _manifest_payload(existing_manifest), False, 0
            )
        if (
            spec.update_type == "general"
            and existing_manifest is not None
            and _is_file(path)
            and pl.read_parquet(_filesystem_path(path)).equals(frame)
        ):
            # Some foreign producers use a different in-memory Arrow buffer
            # layout for the same logical values.  A general dataset is small
            # enough to compare directly when hashes disagree, avoiding a
            # spurious rewrite and downstream invalidation.
            return PartitionWriteResult(
                path, _manifest_payload(existing_manifest), False, 0
            )
        # A metadata transaction switches generations. Readers holding the old
        # immutable path remain valid even if they collect their LazyFrame later.
        generation = relative_path.with_name(f"data-{uuid4().hex}.parquet")
        path = root / generation
        atomic_write_parquet(frame, path, expected_schema=write_context.arrow_schema)
        if spec.update_type == "general":
            # Hash the durable representation.  Parquet normalizes foreign
            # Arrow buffers, so this is the value a later deep scan will see.
            content_hash = frame_content_hash(pl.read_parquet(_filesystem_path(path)))
        file_size = os.stat(_filesystem_path(path)).st_size
        manifest = {
            "source": spec.source,
            "dataset": spec.name,
            "partition_path": relative_path.as_posix(),
            "generation_path": generation.as_posix(),
            "partition_values": partition_values or {},
            "row_count": frame.height,
            "file_size_bytes": file_size,
            "min_time": str(time_values[0]) if time_values[0] is not None else None,
            "max_time": str(time_values[1]) if time_values[1] is not None else None,
            "content_hash": content_hash,
            "schema_hash": write_context.schema_hash,
        }
        return PartitionWriteResult(
            path,
            manifest,
            True,
            file_size,
        )

    def canonical_schema(self, source: str, dataset: str) -> pl.Schema | None:
        """Load the dataset's canonical Arrow schema from metadata."""

        payload = self.metadata.dataset_schema(source, dataset)
        if payload is None:
            return None
        arrow_schema = pa.ipc.read_schema(pa.BufferReader(payload))
        return pl.Schema(arrow_schema)

    def commit_metadata(
        self,
        spec: DatasetSpec,
        schema: pl.Schema,
        manifests: list[dict[str, Any]],
        *,
        replace_manifests: bool = False,
        write_context: PartitionWriteContext | None = None,
        version_commit: dict[str, Any] | None = None,
        scope_transitions: list[dict[str, Any]] | None = None,
        run_id: str | None = None,
        committed_rows: int = 0,
    ) -> None:
        """Publish manifest and schema metadata in one SQLite transaction."""

        context = write_context or partition_write_context(schema)
        self.metadata.commit_dataset_metadata(
            spec.source,
            spec.name,
            manifests=manifests,
            schema_ipc=context.schema_ipc,
            schema_hash=context.schema_hash,
            replace_manifests=replace_manifests,
            version_commit=version_commit,
            scope_transitions=scope_transitions,
            run_id=run_id,
            committed_rows=committed_rows,
        )


def _schema_hash(frame: pl.DataFrame) -> str:
    return _schema_payload_hash(pl.Schema(frame.schema))


def partition_write_context(schema: pl.Schema) -> PartitionWriteContext:
    """Build the canonical physical schema once for a multi-file commit."""

    arrow_schema = pl.DataFrame(schema=schema).to_arrow().schema
    return PartitionWriteContext(
        schema_hash=_schema_payload_hash(schema),
        arrow_schema=arrow_schema,
        schema_ipc=arrow_schema.serialize().to_pybytes(),
    )


def _schema_payload_hash(schema: pl.Schema) -> str:
    payload = "|".join(f"{name}:{dtype}" for name, dtype in schema.items())
    return hashlib.blake2b(payload.encode("utf-8"), digest_size=16).hexdigest()


def _manifest_payload(row: dict[str, Any]) -> dict[str, Any]:
    partition_values = row.get("partition_values", {})
    if isinstance(partition_values, str):
        partition_values = json.loads(partition_values)
    return {
        "source": row["source"],
        "dataset": row["dataset"],
        "partition_path": row["partition_path"],
        "generation_path": row["generation_path"],
        "partition_values": partition_values,
        "row_count": row["row_count"],
        "file_size_bytes": row["file_size_bytes"],
        "min_time": row.get("min_time"),
        "max_time": row.get("max_time"),
        "content_hash": row["content_hash"],
        "schema_hash": row["schema_hash"],
    }


def rollback_partition_writes(results: list[PartitionWriteResult]) -> None:
    """Remove failed unpublished generations; prior committed files stay untouched."""
    failures: list[str] = []
    for result in reversed(results):
        if not result.rewritten:
            continue
        try:
            _unlink_missing(result.path)
        except OSError as error:
            failures.append(f"{result.path}: {error}")
    if failures:
        raise RuntimeError("Failed to remove unpublished generations: " + "; ".join(failures))


def _is_file(path: Path) -> bool:
    return os.path.isfile(_filesystem_path(path))


def _unlink_missing(path: Path) -> None:
    try:
        os.unlink(_filesystem_path(path))
    except FileNotFoundError:
        pass
