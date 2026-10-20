"""Read-only filesystem accounting at explicit owner-selected paths."""

from __future__ import annotations

import os
import stat
from collections.abc import Iterable
from pathlib import Path
from typing import Any


def file_inventory(
    paths: Iterable[Path], *, exclude: Iterable[Path] = ()
) -> list[dict[str, Any]]:
    """Stat every entry, including hidden files and links, without reading bodies.

    Paths are lexical absolute paths: resolving a symlink would escape the
    selected inventory. Missing entries may disappear during a sampled scan;
    other errors propagate instead of producing an incomplete successful total.
    The caller composes/deduplicates independent owner inventories by inode.
    """
    omitted = {os.path.abspath(path) for path in exclude}
    pending = sorted({os.path.abspath(path) for path in paths}, reverse=True)
    visited: set[str] = set()
    entries = []
    while pending:
        path = pending.pop()
        if path in visited or path in omitted:
            continue
        visited.add(path)
        try:
            facts = os.lstat(path)
            directory = stat.S_ISDIR(facts.st_mode)
            if directory:
                with os.scandir(path) as children:
                    pending.extend(
                        sorted((entry.path for entry in children), reverse=True)
                    )
        except FileNotFoundError:
            continue
        entries.append(
            {
                "path": path,
                "bytes": 0 if directory else facts.st_size,
                "allocated_bytes": facts.st_blocks * 512,
                "device": facts.st_dev,
                "inode": facts.st_ino,
                "kind": "directory"
                if directory
                else ("symlink" if stat.S_ISLNK(facts.st_mode) else "file"),
            }
        )
    return sorted(entries, key=lambda entry: entry["path"])


def store_usage(
    meta_path: Path, artifact_path: Path, *, within_root: Path | None = None
) -> dict[str, Any]:
    """Return complete owner paths and stat facts, with unique-inode totals."""
    paths = [
        Path(str(meta_path) + suffix) for suffix in ("", "-wal", "-shm", "-journal")
    ]
    paths.append(artifact_path)
    entries = file_inventory(
        path
        for path in paths
        if within_root is None or path.is_relative_to(within_root)
    )
    seen = set()
    size = allocated = count = 0
    for entry in entries:
        identity = (entry["device"], entry["inode"])
        if identity not in seen:
            seen.add(identity)
            size += entry["bytes"]
            allocated += entry["allocated_bytes"]
            count += entry["kind"] != "directory"
    return {
        "paths": [os.path.abspath(path) for path in paths],
        "entries": entries,
        "bytes": size,
        "allocated_bytes": allocated,
        "file_count": count,
    }
