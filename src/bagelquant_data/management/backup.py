"""Same-schema Data backups and verification owned by the Data package."""

from __future__ import annotations

import hashlib
import os
import shutil
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any

import polars as pl

from bagelquant_data.core.hashing import frame_content_hash
from bagelquant_data.storage.atomic import _filesystem_path
from bagelquant_data.storage.data_meta import DataMetaStore
from bagelquant_data.storage.paths import LakePaths
from bagelquant_data.storage.parquet import ParquetStore
from bagelquant_data.storage.recovery import reconstruct


def verify(data_meta: DataMetaStore, paths: LakePaths) -> dict[str, Any]:
    """Check the single metadata file, every current generation and recovery batch."""
    with data_meta.connect() as db:
        if [row[0] for row in db.execute("pragma integrity_check")] != ["ok"]:
            raise RuntimeError("Data backup failed SQLite integrity_check")
    files = [_file(paths.data_meta_path, "data_meta", "")]
    parquet = ParquetStore(paths, data_meta)
    for row in data_meta.manifest():
        path = paths.generation_path(
            row["source"], row["dataset"], row["partition_path"], row["generation_path"]
        )
        frame = pl.read_parquet(_filesystem_path(path), hive_partitioning=False)
        if (
            frame.height != row["row_count"]
            or frame_content_hash(frame) != row["content_hash"]
        ):
            raise RuntimeError("Data backup generation fails its committed checksum")
        reconstruct(parquet, row["source"], row["dataset"], row["partition_path"])
        files.append(_file(path, "lake", path.relative_to(paths.lake).as_posix()))
    from bagelquant_data.management.lake import DataLake

    lake = DataLake.open(
        data_meta_path=paths.data_meta_path, lake_path=paths.lake, read_only=True
    )
    for row in data_meta._rows(
        "select receipt_id from frozen_inputs order by receipt_id"
    ):
        lake.inputs.verify(str(row["receipt_id"]))
    return {
        "data_meta_path": str(paths.data_meta_path),
        "lake_path": str(paths.lake),
        "files": files,
        "valid": True,
    }


def export(
    data_meta: DataMetaStore,
    paths: LakePaths,
    *,
    data_meta_path: str | Path,
    lake_path: str | Path,
) -> dict[str, Any]:
    """Export one consistent SQLite view and its immutable current generations.

    Complete historical Arrow/frozen evidence remains in SQLite. Older physical
    generations are unnecessary for reads reopened from this backup.
    """
    target = LakePaths.open(data_meta_path=data_meta_path, lake_path=lake_path)
    if target.data_meta_path.exists() or target.lake.exists():
        raise FileExistsError("Data backup destinations must be new")
    if target.data_meta_path.is_relative_to(paths.lake) or target.lake.is_relative_to(
        paths.lake
    ):
        raise ValueError("Data backup must be outside the source lake")
    if paths.lake.is_relative_to(target.lake):
        raise ValueError("Data backup destination cannot contain the source lake")
    target.data_meta_path.parent.mkdir(parents=True, exist_ok=True)
    target.lake.mkdir(parents=True)
    with (
        data_meta.connect() as reader,
        closing(sqlite3.connect(target.data_meta_path)) as writer,
    ):
        reader.backup(writer)
        writer.execute("pragma journal_mode=delete")
        try:
            location = Path(
                os.path.relpath(target.lake, target.data_meta_path.parent)
            ).as_posix()
        except ValueError:
            location = target.lake.as_posix()
        with writer:
            writer.execute(
                "update data_meta_state set value=? where key='lake_location'",
                (location,),
            )
    snapshot = DataMetaStore(data_meta_path=target.data_meta_path, read_only=True)
    snapshot.bind_lake(target.lake)
    for row in snapshot.manifest():
        source = paths.generation_path(
            row["source"], row["dataset"], row["partition_path"], row["generation_path"]
        )
        destination = target.generation_path(
            row["source"], row["dataset"], row["partition_path"], row["generation_path"]
        )
        Path(_filesystem_path(destination.parent)).mkdir(parents=True, exist_ok=True)
        shutil.copy2(_filesystem_path(source), _filesystem_path(destination))
    return verify(snapshot, target)


def _file(path: Path, kind: str, relative: str) -> dict[str, Any]:
    with open(_filesystem_path(path), "rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    return {
        "kind": kind,
        "path": relative,
        "sha256": digest,
        "bytes": os.stat(_filesystem_path(path)).st_size,
    }
