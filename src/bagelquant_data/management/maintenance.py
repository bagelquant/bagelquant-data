"""Owner-scoped storage accounting and explicitly frozen temporary cleanup."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections import Counter
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from uuid import uuid4

from bagelquant_data.core.exceptions import ConfigurationError

_TEMP = re.compile(r"\.data-[0-9a-f]{32}\.parquet\.[0-9a-f]{32}\.tmp")


def _hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _lake_id(lake) -> str:
    return lake._data_meta._rows("select value from data_meta_state where key='lake_id'")[0]["value"]


def _files(root: Path):
    """Inventory regular files without following directory or file symlinks."""
    if not root.is_dir() or root.is_symlink():
        return
    for directory, names, files in os.walk(root, followlinks=False):
        base = Path(directory)
        names[:] = sorted(name for name in names if not (base / name).is_symlink())
        for name in sorted(files):
            path = base / name
            facts = path.lstat()
            if stat.S_ISREG(facts.st_mode):
                yield path, facts


def _identity(path: Path, facts=None, *, content: bool = False) -> dict[str, Any]:
    facts = facts or path.lstat()
    result = {"bytes": facts.st_size, "device": facts.st_dev, "inode": facts.st_ino,
              "mtime_ns": facts.st_mtime_ns, "nlink": facts.st_nlink}
    if content:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(descriptor, "rb") as stream:
            if _identity(path, os.fstat(stream.fileno())) != result:
                raise ConfigurationError("Temporary file changed during inventory")
            result["sha256"] = hashlib.file_digest(stream, "sha256").hexdigest()
    return result


def _inventory(lake) -> list[dict[str, Any]]:
    store, root = lake._data_meta, lake.lake_path
    current = set()
    registered = set()
    dataset_roots = {}
    for row in store._rows("select source,name from datasets"):
        relative = (Path("items") / row["name"] if row["source"] == "items"
                    else Path("raw") / row["source"] / row["name"])
        dataset_roots[relative.as_posix()] = (row["source"], row["name"])
    for row in store._rows("select source,dataset,generation_path from partition_generations"):
        base = Path("items") / row["dataset"] if row["source"] == "items" else Path("raw") / row["source"] / row["dataset"]
        registered.add((base / row["generation_path"]).as_posix())
    for row in store.manifest():
        base = Path("items") / row["dataset"] if row["source"] == "items" else Path("raw") / row["source"] / row["dataset"]
        current.add((base / row["generation_path"]).as_posix())
    active = {(row["source"], row["dataset"]) for row in store.active_update_leases()}
    result = []
    seen = set()
    meta_files = [store.data_meta_path, Path(str(store.data_meta_path) + "-wal"), Path(str(store.data_meta_path) + "-shm"), Path(str(store.data_meta_path) + "-journal")]
    for path in meta_files:
        if path.is_file() and not path.is_symlink():
            facts = path.lstat()
            physical = (facts.st_dev, facts.st_ino)
            if physical in seen:
                continue
            seen.add(physical)
            result.append({"path": path.name, "category": "metadata", "eligible": False,
                           "reason": "metadata_and_recovery", **_identity(path, facts)})
    for path, facts in _files(root):
        physical = (facts.st_dev, facts.st_ino)
        if physical in seen:
            continue
        seen.add(physical)
        relative = path.relative_to(root).as_posix()
        owner = None
        parts = list(Path(relative).parts)
        if len(parts) >= 4:
            owner = dataset_roots.get(Path(*parts[:-3]).as_posix())
        temporary = (owner is not None and _TEMP.fullmatch(parts[-1]) is not None
                     and re.fullmatch(r"year=\d{4}", parts[-3]) is not None
                     and re.fullmatch(r"month=(0[1-9]|1[0-2])", parts[-2]) is not None)
        if relative in current:
            category, reason = "current", "committed_generation"
        elif relative in registered:
            category, reason = "history", "retained_historical_generation"
        elif relative.startswith(".rejected/"):
            category, reason = "rejected", "retained_rejection_evidence"
        elif temporary:
            category, reason = "temporary", "active_writer" if owner in active else "abandoned_atomic_temporary"
        else:
            category, reason = "unknown", "unregistered_or_unknown_file"
        eligible = temporary and owner not in active and facts.st_nlink == 1
        result.append({"path": relative, "category": category, "eligible": eligible,
                       "reason": reason if facts.st_nlink == 1 else "shared_hard_link",
                       "source": owner[0] if owner else None, "dataset": owner[1] if owner else None,
                       **_identity(path, facts)})
    return result


def storage_usage(lake) -> dict[str, Any]:
    """Count actual owned disk bytes once, retaining history and recovery evidence."""
    files = _inventory(lake)
    totals = Counter()
    for row in files:
        totals[row["category"]] += row["bytes"]
    raw = lake.raw.list()
    items = lake.items.list()
    raw_stored = {(row["source"], row["dataset"]) for row in lake.raw.status_many() if row["row_count"]}
    item_stored = {row["dataset"] for row in lake.items.status() if row["row_count"]}
    return {"schema": "bagelquant.data.storage-usage.v1", "lake_id": _lake_id(lake),
            "bytes": sum(row["bytes"] for row in files), "file_count": len(files),
            "metadata_bytes": totals["metadata"], "lake_bytes": sum(totals[k] for k in totals if k != "metadata"),
            "current_bytes": totals["current"], "history_bytes": totals["history"],
            "rejected_bytes": totals["rejected"], "unknown_bytes": totals["unknown"],
            "temporary_bytes": totals["temporary"],
            "reclaimable_temporary_bytes": sum(row["bytes"] for row in files if row["eligible"]),
            "sources": len(lake.catalog.sources.list()), "raw_datasets": len(raw), "data_items": len(items),
            "available_raw_datasets": sum((row["source"], row["name"]) in raw_stored for row in raw),
            "available_data_items": sum(spec.name in item_stored for spec in items)}


def cleanup_plan(lake) -> dict[str, Any]:
    files = _inventory(lake)
    candidates = []
    for row in files:
        if row["eligible"]:
            candidate = {key: row[key] for key in ("path", "source", "dataset", "bytes", "device", "inode", "mtime_ns", "nlink")}
            candidate.update(_identity(lake.lake_path / row["path"], content=True))
            candidates.append(candidate)
    result = {"schema": "bagelquant.data.temporary-cleanup.v1", "lake_id": _lake_id(lake),
              "candidates": candidates, "bytes": sum(row["bytes"] for row in candidates),
              "protected_temporary_bytes": sum(row["bytes"] for row in files if row["category"] == "temporary" and not row["eligible"])}
    result["plan_hash"] = _hash(result)
    return result


def cleanup_temporary(lake, plan: Mapping[str, Any]) -> dict[str, Any]:
    """Revalidate exact frozen candidates under their Data writer leases."""
    store = lake._data_meta
    store.ensure_writable()
    supplied = dict(plan)
    digest = supplied.pop("plan_hash", None)
    if supplied.get("schema") != "bagelquant.data.temporary-cleanup.v1" or supplied.get("lake_id") != _lake_id(lake) or digest != _hash(supplied):
        raise ConfigurationError("Temporary cleanup plan is invalid or belongs to another lake")
    owners = sorted({(row["source"], row["dataset"]) for row in supplied["candidates"]})
    run_ids = [uuid4().hex for _ in owners]
    leases = [(source, dataset, owner) for (source, dataset), owner in zip(owners, run_ids, strict=True)]
    if leases:
        store.acquire_update_leases(leases)
    try:
        inventory = {row["path"]: row for row in _inventory(lake)}
        validated = []
        missing = []
        for row in supplied["candidates"]:
            relative = Path(row["path"])
            if relative.is_absolute() or ".." in relative.parts or relative.as_posix() != row["path"]:
                raise ConfigurationError("Cleanup candidate is not a canonical lake-relative path")
            path = lake.lake_path
            for part in relative.parts:
                path /= part
                if path.is_symlink():
                    raise ConfigurationError("Cleanup candidate contains a symbolic link")
            # Matching immutable path membership is checked against the fresh inventory.
            actual = inventory.get(row["path"])
            if actual is None:
                if path.is_symlink() or path.exists():
                    raise ConfigurationError("Cleanup candidate no longer belongs to the Data inventory")
                missing.append(row["path"])
                continue
            expected = {key: row[key] for key in ("bytes", "device", "inode", "mtime_ns", "nlink", "sha256")}
            if actual["category"] != "temporary" or (actual["source"], actual["dataset"]) not in owners or _identity(path, content=True) != expected or actual["nlink"] != 1:
                raise ConfigurationError("Temporary cleanup plan is stale; preview the current files")
            validated.append((path, row))
        removed = []
        for path, row in validated:
            path.unlink()
            removed.append(row["path"])
        return {"plan_hash": digest, "deleted": removed, "already_missing": missing,
                "bytes": sum(row["bytes"] for _, row in validated)}
    finally:
        store.release_update_leases(run_ids)
