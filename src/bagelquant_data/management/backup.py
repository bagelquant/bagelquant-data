"""Same-schema Data backups and verification owned by the Data package."""

from __future__ import annotations

import hashlib
import os
import re
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
from bagelquant_data.storage.recovery import reconstruct, verify_visibility
from bagelquant_data.storage.snapshot import copy_database, copy_file


def verify(data_meta: DataMetaStore, paths: LakePaths) -> dict[str, Any]:
    """Check the single metadata file, every current generation and recovery batch."""
    with data_meta.connect() as db:
        if [row[0] for row in db.execute("pragma integrity_check")] != ["ok"]:
            raise RuntimeError("Data backup failed SQLite integrity_check")
    inventory = _immutable_files(data_meta,paths,paths)
    files = [_file(paths.data_meta_path, "data_meta", "")]
    parquet = ParquetStore(paths, data_meta)
    partitions = set()
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
        partitions.add((row["source"], row["dataset"], row["partition_path"]))
    with data_meta.connect() as db:
        partitions.update(tuple(row) for row in db.execute(
            "select c.source,c.dataset,b.partition_path from version_segments b "
            "join version_commits c on c.seq=b.commit_seq where c.status='committed'"))
        events = [dict(row) for row in db.execute(
            "select e.*,c.source,c.dataset from visibility_segments e "
            "join version_checks c on c.id=e.check_id order by e.check_id")]
    for source,dataset,partition in sorted(partitions):
        reconstruct(parquet,source,dataset,partition)
    for event in events:
        verify_visibility(data_meta,event,buffer_bytes=1024*1024)
    for path,_,_ in inventory:
        files.append(_file(path,"lake",path.relative_to(paths.lake).as_posix()))
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

    Retained content and visibility originals remain exact files; frozen
    authority and all historical metadata remain in the copied SQLite view.
    """
    target = LakePaths.open(data_meta_path=data_meta_path, lake_path=lake_path)
    sidecars = _validate_destinations(paths, target)
    target.data_meta_path.parent.mkdir(parents=True, exist_ok=True)
    target.lake.mkdir(parents=True)
    try:
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
        for source,destination,_ in _immutable_files(snapshot,paths,target):
            Path(_filesystem_path(destination.parent)).mkdir(parents=True, exist_ok=True)
            shutil.copy2(_filesystem_path(source),_filesystem_path(destination))
        return verify(snapshot, target)
    except BaseException:
        shutil.rmtree(target.lake)
        for path in sidecars:
            path.unlink(missing_ok=True)
        raise


def _immutable_files(data_meta: DataMetaStore, source: LakePaths,
                     target: LakePaths) -> list[tuple[Path,Path,bool]]:
    """Inventory only registered immutable authority from the copied metadata."""
    with data_meta.connect() as db:
        rows = list(db.execute(
            "select source,dataset,partition_path,generation_path,0 as visibility from partition_manifest "
            "union select c.source,c.dataset,b.partition_path,b.generation_path,0 from version_segments b "
            "join version_commits c on c.seq=b.commit_seq where c.status='committed' "
            "union select c.source,c.dataset,'',e.generation_path,1 from visibility_segments e "
            "left join version_checks c on c.id=e.check_id "
            "order by source,dataset,generation_path,partition_path"))
    files: dict[Path,tuple[Path,Path,bool]] = {}
    for row in rows:
        if row[0] is None or row[1] is None:
            raise RuntimeError("Registered visibility event has no check owner")
        paths = []
        for owner in (source,target):
            owner.dataset_root(row[0],row[1])  # Preserve owner/name validation.
            base = owner.lake / ("items" if row[0] == "items" else f"raw/{row[0]}") / row[1]
            if row[4]:
                if not re.fullmatch(r"\.visibility/[0-9a-f]{32}\.parquet",row[3]):
                    raise ValueError("Invalid visibility file reference")
                path = base / row[3]
            else:
                # Validate generation syntax without resolving away source links.
                from bagelquant_data.storage.paths import validate_generation
                path = base / validate_generation(row[2],row[3])
            if not path.resolve().is_relative_to(owner.lake):
                raise ValueError("Immutable file reference escapes its lake")
            relative = path.relative_to(owner.lake)
            if any((owner.lake / Path(*relative.parts[:index])).is_symlink()
                   for index in range(1,len(relative.parts)+1)):
                raise ValueError("Immutable file reference contains a symbolic link")
            paths.append(path)
        key = paths[1].resolve()
        value = (paths[0],paths[1],bool(row[4]))
        if key in files and files[key] != value:
            raise ValueError("Immutable file reference has conflicting ownership")
        files[key] = value
    return [files[key] for key in sorted(files)]


def _file(path: Path, kind: str, relative: str) -> dict[str, Any]:
    with open(_filesystem_path(path), "rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    return {
        "kind": kind,
        "path": relative,
        "sha256": digest,
        "bytes": os.stat(_filesystem_path(path)).st_size,
    }


def snapshot(data_meta: DataMetaStore, paths: LakePaths, *,
             data_meta_path: str | Path, lake_path: str | Path) -> dict[str, Any]:
    """Create an independent consistent copy, explicitly without full verification."""
    target = LakePaths.open(data_meta_path=data_meta_path, lake_path=lake_path)
    sidecars = _validate_destinations(paths, target)
    target.data_meta_path.parent.mkdir(parents=True, exist_ok=True)
    target.lake.mkdir(parents=True)
    try:
        copy_database(data_meta.data_meta_path, target.data_meta_path)
        with closing(sqlite3.connect(target.data_meta_path)) as writer:
            # Recovery/checkpoint affects copied files only. No full-history scan.
            writer.execute("pragma journal_mode=delete")
            location = Path(os.path.relpath(target.lake, target.data_meta_path.parent)).as_posix()
            with writer:
                writer.execute("update data_meta_state set value=? where key='lake_location'", (location,))
        copied = DataMetaStore(data_meta_path=target.data_meta_path, read_only=True)
        copied.bind_lake(target.lake)
        count,visibility_count = 0,0
        for source,destination,visibility in _immutable_files(copied,paths,target):
            destination.parent.mkdir(parents=True, exist_ok=True)
            copy_file(source, destination)
            count += 1
            visibility_count += visibility
        return {"data_meta_path": str(target.data_meta_path), "lake_path": str(target.lake),
                "valid": None, "verification": "unverified", "generation_count": len(copied.manifest()),
                "immutable_file_count": count, "visibility_count": visibility_count}
    except BaseException:
        # These destinations were new and belong solely to this failed operation.
        shutil.rmtree(target.lake)
        for path in sidecars:
            path.unlink(missing_ok=True)
        raise


def _validate_destinations(paths: LakePaths, target: LakePaths) -> tuple[Path, ...]:
    """Keep both copy modes and their failure cleanup disjoint from originals."""
    sidecars = tuple(target.data_meta_path.with_name(target.data_meta_path.name + suffix)
                     for suffix in ("", "-wal", "-shm", "-journal"))
    original_metadata = {paths.data_meta_path.with_name(paths.data_meta_path.name + suffix)
                         for suffix in ("", "-wal", "-shm", "-journal")}
    if any(path.exists() or path.is_symlink() for path in (*sidecars, target.lake)):
        raise FileExistsError("Data copy destinations must be new")
    if any(
        destination.is_relative_to(original) or original.is_relative_to(destination)
        for destination in (*sidecars, target.lake)
        for original in (*original_metadata, paths.lake)
    ):
        raise ValueError("Data copy destinations must not overlap original storage")
    if any(target.lake.is_relative_to(path) or path.is_relative_to(target.lake)
           for path in sidecars):
        raise ValueError("Data metadata and lake destinations overlap")
    return sidecars
