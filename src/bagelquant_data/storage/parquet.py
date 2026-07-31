"""Canonical Parquet storage."""

from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

import polars as pl
import pyarrow as pa

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.hashing import frame_content_hash
from bagelquant_data.core.schema import compatible_schema
from bagelquant_data.storage.atomic import atomic_write_parquet
from bagelquant_data.storage.metadata import MetadataStore
from bagelquant_data.storage.paths import LakePaths

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PartitionWriteResult:
    """Result of comparing and optionally publishing one canonical partition."""

    path: Path
    manifest: dict[str, Any]
    rewritten: bool
    bytes_written: int
    backup_path: Path | None = None
    existed_before: bool = False


class ParquetStore:
    """Read and write canonical lake Parquet files."""

    def __init__(self, paths: LakePaths, metadata: MetadataStore) -> None:
        self.paths = paths
        self.metadata = metadata

    def write_partition(
        self,
        spec: DatasetSpec,
        frame: pl.DataFrame,
        relative_path: Path,
        partition_values: dict[str, Any] | None = None,
    ) -> Path:
        result = self.write_partition_file_result(
            spec, frame, relative_path, partition_values
        )
        if result.rewritten:
            self.metadata.upsert_manifest(**result.manifest)
        self._update_canonical_schema(spec, frame)
        return result.path

    def write_partition_file(
        self,
        spec: DatasetSpec,
        frame: pl.DataFrame,
        relative_path: Path,
        partition_values: dict[str, Any] | None = None,
    ) -> tuple[Path, dict[str, Any]]:
        result = self.write_partition_file_result(
            spec, frame, relative_path, partition_values
        )
        self._update_canonical_schema(spec, frame)
        return result.path, result.manifest

    def write_partition_file_result(
        self,
        spec: DatasetSpec,
        frame: pl.DataFrame,
        relative_path: Path,
        partition_values: dict[str, Any] | None = None,
        *,
        existing_manifest: dict[str, Any] | None = None,
        retain_backup: bool = False,
    ) -> PartitionWriteResult:
        """Write one changed partition and retain a byte-identical manifest on no-op."""

        path = self.paths.dataset_root(spec.source, spec.name) / relative_path
        time_values = (
            frame.select(
                pl.min("time").alias("min_time"), pl.max("time").alias("max_time")
            ).row(0)
            if "time" in frame.columns and frame.height
            else (None, None)
        )
        content_hash = frame_content_hash(frame)
        if existing_manifest is None:
            existing_manifest = next(
                (
                    row
                    for row in self.metadata.manifest(spec.source, spec.name)
                    if row["partition_path"] == relative_path.as_posix()
                ),
                None,
            )
        if (
            existing_manifest is not None
            and existing_manifest.get("content_hash") == content_hash
            and path.is_file()
        ):
            return PartitionWriteResult(
                path, _manifest_payload(existing_manifest), False, 0
            )
        existed_before = path.is_file()
        backup_path = None
        if retain_backup and existed_before:
            backup_path = path.with_name(
                f".{path.name}.{uuid4().hex}.rollback"
            )
            os.link(path, backup_path)
        try:
            atomic_write_parquet(frame, path)
        except BaseException:
            if backup_path is not None:
                backup_path.unlink(missing_ok=True)
            raise
        manifest = {
            "source": spec.source,
            "dataset": spec.name,
            "partition_path": relative_path.as_posix(),
            "partition_values": partition_values or {},
            "row_count": frame.height,
            "file_size_bytes": path.stat().st_size,
            "min_time": str(time_values[0]) if time_values[0] is not None else None,
            "max_time": str(time_values[1]) if time_values[1] is not None else None,
            "content_hash": content_hash,
            "schema_hash": _schema_hash(frame),
        }
        return PartitionWriteResult(
            path,
            manifest,
            True,
            path.stat().st_size,
            backup_path,
            existed_before,
        )

    def canonical_schema(self, source: str, dataset: str) -> pl.Schema | None:
        """Load the dataset's canonical Arrow schema from metadata."""

        payload = self.metadata.dataset_schema(source, dataset)
        if payload is None:
            return None
        arrow_schema = pa.ipc.read_schema(pa.BufferReader(payload))
        return pl.Schema(arrow_schema)

    def set_canonical_schema(
        self, source: str, dataset: str, schema: pl.Schema
    ) -> None:
        """Persist an Arrow schema after its canonical partitions are durable."""

        arrow_schema = schema.to_arrow()
        self.metadata.upsert_dataset_schema(
            source,
            dataset,
            schema_ipc=arrow_schema.serialize().to_pybytes(),
            schema_hash=_schema_payload_hash(schema),
        )

    def commit_metadata(
        self,
        spec: DatasetSpec,
        schema: pl.Schema,
        manifests: list[dict[str, Any]],
        *,
        replace_manifests: bool = False,
    ) -> None:
        """Publish manifest and schema metadata in one SQLite transaction."""

        arrow_schema = schema.to_arrow()
        self.metadata.commit_dataset_metadata(
            spec.source,
            spec.name,
            manifests=manifests,
            schema_ipc=arrow_schema.serialize().to_pybytes(),
            schema_hash=_schema_payload_hash(schema),
            replace_manifests=replace_manifests,
        )

    def _update_canonical_schema(
        self, spec: DatasetSpec, frame: pl.DataFrame
    ) -> None:
        incoming = pl.Schema(frame.schema)
        if spec.update_type == "general":
            schema = incoming
        else:
            existing = self.canonical_schema(spec.source, spec.name)
            schema = compatible_schema(
                candidate
                for candidate in (existing, incoming)
                if candidate is not None
            )
        self.set_canonical_schema(spec.source, spec.name, schema)

    def scan_dataset(
        self, source: str, dataset: str, paths: list[Path] | None = None
    ) -> pl.LazyFrame:
        root = self.paths.dataset_root(source, dataset)
        if paths:
            files = [root / path for path in paths]
        else:
            files = sorted(root.glob("**/*.parquet"))
        if not files:
            from bagelquant_data.core.exceptions import DatasetNotFoundError

            raise DatasetNotFoundError(f"No canonical data for {source}/{dataset}")
        return pl.scan_parquet([str(path) for path in files])


def _schema_hash(frame: pl.DataFrame) -> str:
    return _schema_payload_hash(pl.Schema(frame.schema))


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
        "partition_values": partition_values,
        "row_count": row["row_count"],
        "file_size_bytes": row["file_size_bytes"],
        "min_time": row.get("min_time"),
        "max_time": row.get("max_time"),
        "content_hash": row["content_hash"],
        "schema_hash": row["schema_hash"],
    }


def finalize_partition_writes(results: list[PartitionWriteResult]) -> None:
    """Discard rollback links after file and metadata publication succeeds."""

    for result in results:
        if result.backup_path is None:
            continue
        try:
            result.backup_path.unlink(missing_ok=True)
        except OSError as error:
            logger.warning(
                "Could not remove committed partition rollback link %s: %s",
                result.backup_path,
                error,
            )


def rollback_partition_writes(results: list[PartitionWriteResult]) -> None:
    """Restore every changed destination after a batch publication failure."""

    failures: list[str] = []
    for result in reversed(results):
        if not result.rewritten:
            continue
        try:
            if result.backup_path is not None and result.backup_path.exists():
                os.replace(result.backup_path, result.path)
            elif not result.existed_before:
                result.path.unlink(missing_ok=True)
        except OSError as error:
            failures.append(f"{result.path}: {error}")
    for result in results:
        if result.backup_path is not None:
            try:
                result.backup_path.unlink(missing_ok=True)
            except OSError as error:
                failures.append(f"{result.backup_path}: {error}")
    if failures:
        raise RuntimeError(
            "Failed to roll back canonical partition publication: "
            + "; ".join(failures)
        )
