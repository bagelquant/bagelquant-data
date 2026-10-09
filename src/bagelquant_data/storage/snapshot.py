"""Independent file clones and stable SQLite snapshots without source writes."""

from __future__ import annotations

import ctypes
import errno
import os
from pathlib import Path
import shutil
import sqlite3
import sys

from bagelquant_data.core.exceptions import ConfigurationError
from bagelquant_data.storage.atomic import _filesystem_path


def file_signature(path: Path) -> tuple[int, int, int, int, int] | None:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return None
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns


def _clone_file(source: Path, destination: Path) -> bool:
    if sys.platform != "darwin":
        return False
    libc = ctypes.CDLL(None, use_errno=True)
    clone = getattr(libc, "clonefile", None)
    if clone is None:
        return False
    clone.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int]
    clone.restype = ctypes.c_int
    if clone(os.fsencode(_filesystem_path(source)), os.fsencode(_filesystem_path(destination)), 0) == 0:
        return True
    code = ctypes.get_errno()
    if code in {errno.EXDEV, errno.ENOTSUP, errno.EINVAL, errno.ENOSYS}:
        return False
    raise OSError(code, os.strerror(code), str(destination))


def copy_file(source: Path, destination: Path) -> None:
    """Create a new independent copy, preferring filesystem copy-on-write."""
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f"Snapshot destination already exists: {destination}")
    if not source.exists():
        raise FileNotFoundError(f"Snapshot source is missing: {source}")
    if source.is_symlink() or not source.is_file():
        raise ValueError(f"Snapshot source must be a regular file: {source}")
    if not _clone_file(source, destination):
        shutil.copy2(_filesystem_path(source), _filesystem_path(destination))


def copy_database(source: Path, destination: Path) -> None:
    """Copy stable main/WAL bytes; recovery may touch only the destination.

    Reject active rollback journals. No source connection, checkpoint, hard link
    or shared writable inode is created. Changing source signatures force retry.
    """
    source = source.resolve()
    originals = (source, source.with_name(source.name + "-wal"),
                 source.with_name(source.name + "-journal"))
    targets = (destination, destination.with_name(destination.name + "-wal"))
    sidecars = (*targets, destination.with_name(destination.name + "-shm"),
                destination.with_name(destination.name + "-journal"))
    if any(path.exists() or path.is_symlink() for path in sidecars):
        raise FileExistsError("SQLite snapshot destinations must be new")
    try:
        for _ in range(3):
            before = tuple(file_signature(path) for path in originals)
            try:
                if before[2] is not None:
                    with originals[2].open("rb") as stream:
                        if any(stream.read(8)):
                            raise sqlite3.OperationalError("Data metadata has an active rollback journal")
                copy_file(source, destination)
                if before[1] is not None and before[1][2] > 0:
                    copy_file(originals[1], targets[1])
            except (OSError, sqlite3.Error):
                if before == tuple(file_signature(path) for path in originals):
                    raise
            else:
                if before == tuple(file_signature(path) for path in originals):
                    return
            for path in targets:
                path.unlink(missing_ok=True)
        raise ConfigurationError("Data metadata changed during snapshot; retry when it is stable")
    except BaseException:
        for path in targets:
            path.unlink(missing_ok=True)
        raise
