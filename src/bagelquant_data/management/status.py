"""Status and inspection API."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from collections import Counter
from collections.abc import Iterable
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from bagelquant_data.core.dataset import DatasetSpec, incremental_key
from bagelquant_data.core.exceptions import DestructiveOperationError
from bagelquant_data.core.hashing import frame_content_hash, stable_bucket
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

    def validate_dataset(
        self, spec: DatasetSpec, *, deep: bool = True
    ) -> dict[str, Any]:
        """Validate physical files against the registered dataset contract."""

        manifest = self.validate_manifest(
            spec.name, source=spec.source, deep=False
        )
        issues = [
            _health_issue(
                _manifest_issue_code(issue),
                str(issue.get("detail", "manifest integrity issue")),
                path=issue.get("path"),
                field=issue.get("field"),
            )
            for issue in manifest["issues"]
        ]
        files_scanned = 0
        bytes_read = 0
        canonical_schema = self.metadata.dataset_schema(spec.source, spec.name)
        canonical_hash = _canonical_schema_hash(canonical_schema)
        rows = self.metadata.manifest(spec.source, spec.name)
        root = self.paths.dataset_root(spec.source, spec.name)
        if rows and canonical_hash is None:
            issues.append(
                _health_issue(
                    "canonical_schema_missing",
                    "canonical dataset schema is missing",
                    path=None,
                )
            )
        if deep:
            for row in rows:
                relative = str(row["partition_path"])
                path = root / relative
                if not path.is_file():
                    continue
                try:
                    frame = pl.read_parquet(path)
                except Exception as error:  # noqa: BLE001 - report corrupt files.
                    issues.append(
                        _health_issue(
                            "unreadable_file", str(error), path=relative
                        )
                    )
                    continue
                files_scanned += 1
                bytes_read += path.stat().st_size
                actual = _file_facts(frame, path)
                for field, value in actual.items():
                    if value != row[field]:
                        issues.append(
                            _health_issue(
                                "manifest_mismatch",
                                f"{field} does not match manifest",
                                path=relative,
                                field=field,
                                expected=row[field],
                                actual=value,
                            )
                        )
                actual_schema_hash = _schema_hash(frame)
                if canonical_hash is not None and actual_schema_hash != canonical_hash:
                    issues.append(
                        _health_issue(
                            "canonical_schema_mismatch",
                            "file schema does not match canonical dataset schema",
                            path=relative,
                            expected=canonical_hash,
                            actual=actual_schema_hash,
                        )
                    )
                issues.extend(_contract_issues(frame, spec, relative))
        repairable = sum(bool(issue["repairable"]) for issue in issues)
        return {
            "source": spec.source,
            "dataset": spec.name,
            "update_type": spec.update_type,
            "manifest_files": manifest["manifest_files"],
            "files_scanned": files_scanned,
            "bytes_read": bytes_read,
            "issues": issues,
            "issue_counts": dict(Counter(str(issue["code"]) for issue in issues)),
            "repairable_issue_count": repairable,
            "deep": deep,
            "valid": not issues,
        }

    def quarantine_partitions(
        self,
        spec: DatasetSpec,
        partition_paths: Iterable[str],
        *,
        reason: str,
        confirm: bool = False,
        repair_id: str | None = None,
    ) -> dict[str, Any]:
        """Move suspect partitions aside and remove their manifest rows safely."""

        if not confirm:
            raise DestructiveOperationError(
                "Pass confirm=True to quarantine canonical partitions"
            )
        if not reason.strip():
            raise ValueError("quarantine reason must not be blank")
        selected = tuple(
            dict.fromkeys(_safe_relative_partition(path) for path in partition_paths)
        )
        if not selected:
            return {
                "source": spec.source,
                "dataset": spec.name,
                "repair_id": repair_id,
                "quarantined": [],
                "removed_manifests": [],
            }
        operation_id = repair_id or uuid.uuid4().hex
        source_root = self.paths.dataset_root(spec.source, spec.name).resolve()
        quarantine_root = (
            self.paths.root
            / ".health-repair-quarantine"
            / operation_id
            / spec.source
            / spec.name
        ).resolve()
        if not quarantine_root.is_relative_to(self.paths.root.resolve()):
            raise DestructiveOperationError("quarantine path escapes lake root")
        journal = quarantine_root / "journal.json"
        moves: list[tuple[Path, Path, str]] = []
        for relative in selected:
            source_path = (source_root / Path(relative)).resolve()
            if not source_path.is_relative_to(source_root):
                raise DestructiveOperationError(
                    f"Partition path escapes dataset root: {relative}"
                )
            if source_path.is_file():
                moves.append(
                    (source_path, quarantine_root / Path(relative), relative)
                )
        _atomic_json(
            journal,
            {
                "schema": "bagelquant-data.health-quarantine.v1",
                "repair_id": operation_id,
                "source": spec.source,
                "dataset": spec.name,
                "reason": reason,
                "state": "planned",
                "partitions": list(selected),
            },
        )
        moved: list[tuple[Path, Path, str]] = []
        removed: list[dict[str, Any]] = []
        try:
            for source_path, target, relative in moves:
                target.parent.mkdir(parents=True, exist_ok=True)
                os.replace(source_path, target)
                moved.append((source_path, target, relative))
            removed = self.metadata.remove_manifests(
                spec.source, spec.name, selected
            )
            _atomic_json(
                journal,
                {
                    "schema": "bagelquant-data.health-quarantine.v1",
                    "repair_id": operation_id,
                    "source": spec.source,
                    "dataset": spec.name,
                    "reason": reason,
                    "state": "committed",
                    "partitions": list(selected),
                    "manifest_rows": [
                        str(row["partition_path"]) for row in removed
                    ],
                },
            )
        except Exception:
            if removed:
                self.metadata.upsert_manifests(removed)
            for source_path, target, _ in reversed(moved):
                source_path.parent.mkdir(parents=True, exist_ok=True)
                if target.exists():
                    os.replace(target, source_path)
            raise
        return {
            "source": spec.source,
            "dataset": spec.name,
            "repair_id": operation_id,
            "journal": str(journal),
            "quarantine_root": str(quarantine_root),
            "quarantined": [relative for _, _, relative in moved],
            "removed_manifests": [
                str(row["partition_path"]) for row in removed
            ],
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


def _canonical_schema_hash(payload: bytes | None) -> str | None:
    if payload is None:
        return None
    import pyarrow as pa

    schema = pl.Schema(pa.ipc.read_schema(pa.BufferReader(payload)))
    frame = pl.DataFrame(schema=schema)
    return _schema_hash(frame)


def _file_facts(frame: pl.DataFrame, path: Path) -> dict[str, Any]:
    time_values = (
        frame.select(
            pl.min("time").alias("min_time"),
            pl.max("time").alias("max_time"),
        ).row(0)
        if "time" in frame.columns and frame.height
        else (None, None)
    )
    return {
        "row_count": frame.height,
        "file_size_bytes": path.stat().st_size,
        "min_time": None if time_values[0] is None else str(time_values[0]),
        "max_time": None if time_values[1] is None else str(time_values[1]),
        "content_hash": frame_content_hash(frame),
        "schema_hash": _schema_hash(frame),
    }


def _contract_issues(
    frame: pl.DataFrame, spec: DatasetSpec, relative: str
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    key = incremental_key(spec)
    if key is not None:
        missing = [column for column in key if column not in frame.columns]
        if missing:
            issues.append(
                _health_issue(
                    "missing_key_column",
                    f"missing canonical key columns: {', '.join(missing)}",
                    path=relative,
                )
            )
        else:
            null_rows = frame.select(
                pl.any_horizontal(*(pl.col(column).is_null() for column in key))
                .sum()
                .alias("count")
            ).item()
            if null_rows:
                issues.append(
                    _health_issue(
                        "null_key",
                        f"{null_rows} rows contain null canonical keys",
                        path=relative,
                        actual=int(null_rows),
                    )
                )
            duplicate_rows = (
                frame.group_by(list(key)).len().filter(pl.col("len") > 1).height
            )
            if duplicate_rows:
                issues.append(
                    _health_issue(
                        "duplicate_key",
                        f"{duplicate_rows} duplicate canonical key groups",
                        path=relative,
                        actual=duplicate_rows,
                    )
                )
    values = _partition_values(Path(relative))
    expected_parts = (
        {"year", "month"}
        if spec.update_type == "by_daily"
        else {"year", "bucket"}
        if spec.update_type == "by_asset"
        else set()
    )
    if spec.update_type == "general":
        if relative != "data.parquet":
            issues.append(
                _health_issue(
                    "partition_path",
                    "general dataset must use data.parquet",
                    path=relative,
                )
            )
        return issues
    if set(values) != expected_parts or Path(relative).name != "data.parquet":
        issues.append(
            _health_issue(
                "partition_path",
                f"partition path must contain {sorted(expected_parts)}",
                path=relative,
            )
        )
        return issues
    if frame.is_empty() or "time" not in frame.columns:
        return issues
    if spec.update_type == "by_daily":
        mismatches = frame.filter(
            (pl.col("time").dt.year() != int(values["year"]))
            | (pl.col("time").dt.month() != int(values["month"]))
        ).height
    elif "asset_id" in frame.columns:
        expected_bucket = int(values["bucket"])
        mismatches = sum(
            1
            for asset_id, row_time in frame.select("asset_id", "time").iter_rows()
            if row_time.year != int(values["year"])
            or stable_bucket(str(asset_id), spec.asset_bucket_count)
            != expected_bucket
        )
    else:
        mismatches = 0
    if mismatches:
        issues.append(
            _health_issue(
                "partition_value_mismatch",
                f"{mismatches} rows do not belong to the physical partition",
                path=relative,
                actual=mismatches,
            )
        )
    return issues


def _manifest_issue_code(issue: dict[str, Any]) -> str:
    return {
        "missing": "missing_file",
        "orphaned": "orphan_file",
        "mismatch": "manifest_mismatch",
        "unreadable": "unreadable_file",
    }.get(str(issue.get("kind")), str(issue.get("kind", "manifest_issue")))


def _health_issue(
    code: str,
    detail: str,
    *,
    path: object,
    field: object = None,
    expected: object = None,
    actual: object = None,
) -> dict[str, Any]:
    return {
        "code": code,
        "severity": "critical",
        "repairable": True,
        "path": None if path is None else str(path),
        "field": field,
        "expected": expected,
        "actual": actual,
        "detail": detail,
    }


def _safe_relative_partition(value: object) -> str:
    path = Path(str(value))
    if path.is_absolute() or ".." in path.parts or path.suffix != ".parquet":
        raise DestructiveOperationError(f"Unsafe partition path: {value}")
    return path.as_posix()


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _revision_due(
    scope: dict[str, Any], provider_check: dict[str, Any] | None
) -> bool:
    if scope["scope_kind"] != "asset" or scope["status"] == "running":
        return False
    if provider_check is None or provider_check["recheck_after"] is None:
        return True
    return date.fromisoformat(str(provider_check["recheck_after"])) <= date.today()
