"""Status and inspection API."""

from __future__ import annotations

import hashlib
from collections import Counter
from collections.abc import Iterable
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from bagelquant_data.core.hashing import frame_content_hash
from bagelquant_data.storage.metadata import MetadataStore
from bagelquant_data.storage.paths import LakePaths


class StatusManager:
    """Manifest-driven status queries."""

    def __init__(self, metadata: MetadataStore, paths: LakePaths) -> None:
        self.metadata = metadata
        self.paths = paths

    def summary(self) -> dict[str, Any]:
        datasets = self.metadata.list_datasets()
        manifest = self.metadata.manifest()
        return {
            "sources": len(self.metadata.list_sources()),
            "datasets": len(datasets),
            "partitions": len(manifest),
            "rows": sum(int(row["row_count"]) for row in manifest),
            "bytes": sum(int(row["file_size_bytes"]) for row in manifest),
        }

    def dataset(
        self, dataset: str, *, source: str, deep: bool = False
    ) -> dict[str, Any]:
        manifest = self.metadata.manifest(source, dataset)
        return {
            "source": source,
            "dataset": dataset,
            "file_count": len(manifest),
            "partition_count": len(manifest),
            "total_size": sum(int(row["file_size_bytes"]) for row in manifest),
            "row_count": sum(int(row["row_count"]) for row in manifest),
            "minimum_time": min(
                (row["min_time"] for row in manifest if row["min_time"]), default=None
            ),
            "maximum_time": max(
                (row["max_time"] for row in manifest if row["max_time"]), default=None
            ),
            "last_update": max((row["updated_at"] for row in manifest), default=None),
            "deep": deep,
        }

    def partitions(self, dataset: str, *, source: str) -> list[dict[str, Any]]:
        return self.metadata.manifest(source, dataset)

    def runs(self, limit: int = 20) -> list[dict[str, Any]]:
        return self.metadata.runs(limit)

    def failures(
        self, dataset: str | None = None, source: str | None = None
    ) -> list[dict[str, Any]]:
        return [
            run
            for run in self.metadata.runs(1000)
            if run["status"] != "success"
            and (dataset is None or run["dataset"] == dataset)
            and (source is None or run["source"] == source)
        ]

    def update_scopes(
        self,
        dataset: str | None = None,
        source: str | None = None,
        status: str | Iterable[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Return filtered authoritative update-scope rows."""

        return self.metadata.update_scopes(
            source=source, dataset=dataset, status=status
        )

    def provider_scope_checks(
        self, dataset: str | None = None, source: str | None = None
    ) -> list[dict[str, Any]]:
        """Return provider-check scheduling records separately from local coverage."""

        return self.metadata.provider_scope_checks(source=source, dataset=dataset)

    def update_summary(
        self, dataset: str | None = None, source: str | None = None
    ) -> list[dict[str, Any]]:
        """Summarize ledger counts and watermarks per dataset."""

        rows = self.update_scopes(dataset=dataset, source=source)
        provider_checks = {
            int(row["scope_id"]): row
            for row in self.provider_scope_checks(dataset=dataset, source=source)
        }
        grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault((str(row["source"]), str(row["dataset"])), []).append(
                row
            )
        summaries = []
        for (row_source, row_dataset), scopes in sorted(grouped.items()):
            counts = Counter(str(scope["status"]) for scope in scopes)
            pending_keys = [
                str(scope["scope_key"])
                for scope in scopes
                if scope["status"] in {"pending", "failed"}
            ]
            local_maxima = [
                str(scope["data_max_time"])
                for scope in scopes
                if scope["data_max_time"] is not None
            ]
            checked = [
                str(provider_checks[int(scope["id"])]["checked_through"])
                for scope in scopes
                if int(scope["id"]) in provider_checks
            ]
            successes = [
                str(scope["last_success_at"])
                for scope in scopes
                if scope["last_success_at"] is not None
            ]
            revision_due = sum(
                _revision_due(
                    scope, provider_checks.get(int(scope["id"]))
                )
                for scope in scopes
            )
            future_rechecks = [
                str(check["recheck_after"])
                for scope in scopes
                if scope["status"] in {"success", "empty"}
                and (check := provider_checks.get(int(scope["id"]))) is not None
                and check["recheck_after"] is not None
                and date.fromisoformat(str(check["recheck_after"])) > date.today()
            ]
            summaries.append(
                {
                    "source": row_source,
                    "dataset": row_dataset,
                    "total": len(scopes),
                    "pending": counts["pending"],
                    "running": counts["running"],
                    "success": counts["success"],
                    "empty": counts["empty"],
                    "failed": counts["failed"],
                    "invalid": counts["invalid"],
                    "actionable_pending_failed": counts["pending"]
                    + counts["failed"],
                    "deferred_recheck": len(future_rechecks),
                    "next_provider_recheck": min(future_rechecks, default=None),
                    "earliest_pending_scope": min(pending_keys, default=None),
                    "local_data_max_min": min(local_maxima, default=None),
                    "local_data_max_max": max(local_maxima, default=None),
                    "provider_checked_through_min": min(checked, default=None),
                    "provider_checked_through_max": max(checked, default=None),
                    "last_success_at": max(successes, default=None),
                    "revision_due_assets": revision_due,
                }
            )
        return summaries

    def reset_update_scopes(
        self, scope_ids: Iterable[int], *, clear_watermark: bool = False
    ) -> int:
        """Move selected terminal scopes back to pending."""

        return self.metadata.reset_update_scopes(
            scope_ids, clear_watermark=clear_watermark
        )

    def rejected(self, dataset: str, *, source: str) -> list[dict[str, Any]]:
        return self.metadata.rejected(source, dataset)

    def files(self, dataset: str, *, source: str) -> list[dict[str, Any]]:
        root = self.paths.dataset_root(source, dataset)
        rows = self.partitions(dataset, source=source)
        for row in rows:
            path = root / row["partition_path"]
            row["path"] = str(path)
            row["exists"] = path.exists()
        return rows

    def rebuild_manifest(self, dataset: str, *, source: str) -> dict[str, Any]:
        root = self.paths.dataset_root(source, dataset)
        manifests: list[dict[str, Any]] = []
        for path in sorted(root.glob("**/*.parquet")):
            relative_path = path.relative_to(root)
            frame = pl.read_parquet(path)
            time_values = (
                frame.select(
                    pl.min("time").alias("min_time"), pl.max("time").alias("max_time")
                ).row(0)
                if "time" in frame.columns and frame.height
                else (None, None)
            )
            manifests.append(
                {
                    "source": source,
                    "dataset": dataset,
                    "partition_path": relative_path.as_posix(),
                    "partition_values": _partition_values(relative_path),
                    "row_count": frame.height,
                    "file_size_bytes": path.stat().st_size,
                    "min_time": str(time_values[0])
                    if time_values[0] is not None
                    else None,
                    "max_time": str(time_values[1])
                    if time_values[1] is not None
                    else None,
                    "content_hash": frame_content_hash(frame),
                    "schema_hash": _schema_hash(frame),
                }
            )
        self.metadata.replace_manifests(source, dataset, manifests)
        return {
            "source": source,
            "dataset": dataset,
            "files_scanned": len(manifests),
            "rows": sum(int(row["row_count"]) for row in manifests),
            "bytes": sum(int(row["file_size_bytes"]) for row in manifests),
        }

    def validate_manifest(
        self, dataset: str, *, source: str, deep: bool = False
    ) -> dict[str, Any]:
        files = self.files(dataset, source=source)
        missing = [row["partition_path"] for row in files if not row["exists"]]
        root = self.paths.dataset_root(source, dataset)
        manifested = {str(row["partition_path"]) for row in files}
        physical = {
            path.relative_to(root).as_posix()
            for path in root.rglob("*.parquet")
        } if root.exists() else set()
        orphaned = sorted(physical - manifested)
        issues: list[dict[str, Any]] = [
            {"kind": "missing", "path": path, "detail": "manifest file is missing"}
            for path in missing
        ]
        issues.extend(
            {"kind": "orphaned", "path": path, "detail": "file is not manifested"}
            for path in orphaned
        )
        bytes_read = 0
        scanned = 0
        if deep:
            for row in files:
                if not row["exists"]:
                    continue
                relative = str(row["partition_path"])
                path = root / relative
                try:
                    frame = pl.read_parquet(path)
                    scanned += 1
                    bytes_read += path.stat().st_size
                    time_values = (
                        frame.select(
                            pl.min("time").alias("min_time"),
                            pl.max("time").alias("max_time"),
                        ).row(0)
                        if "time" in frame.columns and frame.height
                        else (None, None)
                    )
                    actual = {
                        "row_count": frame.height,
                        "file_size_bytes": path.stat().st_size,
                        "min_time": None if time_values[0] is None else str(time_values[0]),
                        "max_time": None if time_values[1] is None else str(time_values[1]),
                        "content_hash": frame_content_hash(frame),
                        "schema_hash": _schema_hash(frame),
                    }
                    for field, value in actual.items():
                        if value != row[field]:
                            issues.append(
                                {
                                    "kind": "mismatch",
                                    "path": relative,
                                    "field": field,
                                    "expected": row[field],
                                    "actual": value,
                                    "detail": f"{field} does not match manifest",
                                }
                            )
                except Exception as error:  # noqa: BLE001 - isolate corrupt files.
                    issues.append(
                        {
                            "kind": "unreadable",
                            "path": relative,
                            "detail": str(error),
                        }
                    )
        return {
            "source": source,
            "dataset": dataset,
            "manifest_files": len(files),
            "missing_files": missing,
            "orphaned_files": orphaned,
            "files_scanned": scanned,
            "bytes_read": bytes_read,
            "issues": issues,
            "deep": deep,
            "valid": not issues,
        }


def _partition_values(relative_path: Path) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for part in relative_path.parts[:-1]:
        key, sep, value = part.partition("=")
        if sep:
            values[key] = _partition_scalar(value)
    return values


def _partition_scalar(value: str) -> object:
    try:
        return int(value)
    except ValueError:
        return value


def _schema_hash(frame: pl.DataFrame) -> str:
    payload = "|".join(f"{name}:{dtype}" for name, dtype in frame.schema.items())
    return hashlib.blake2b(payload.encode("utf-8"), digest_size=16).hexdigest()


def _revision_due(
    scope: dict[str, Any], provider_check: dict[str, Any] | None
) -> bool:
    if scope["scope_kind"] != "asset" or scope["status"] == "running":
        return False
    if provider_check is None or provider_check["recheck_after"] is None:
        return True
    return date.fromisoformat(str(provider_check["recheck_after"])) <= date.today()
