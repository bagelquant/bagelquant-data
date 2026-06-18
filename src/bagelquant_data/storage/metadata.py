"""SQLite operational metadata."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from bagelquant_data.core.dataset import DatasetSpec


class MetadataStore:
    """SQLite metadata store using WAL mode."""

    _BUSY_TIMEOUT_MS = 30_000

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout={self._BUSY_TIMEOUT_MS}")
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def upsert_source(
        self,
        name: str,
        adapter: str,
        configured: bool = False,
        options: dict[str, Any] | None = None,
    ) -> None:
        now = _now()
        options_json = None if options is None else json.dumps(options, sort_keys=True, default=str)
        with self.connect() as db:
            db.execute(
                """
                insert into sources(name, adapter, configured, options_json, created_at, updated_at)
                values (?, ?, ?, ?, ?, ?)
                on conflict(name) do update set
                    adapter=excluded.adapter,
                    configured=excluded.configured,
                    options_json=coalesce(excluded.options_json, sources.options_json),
                    updated_at=excluded.updated_at
                """,
                (name, adapter, int(configured), options_json, now, now),
            )

    def source_options(self, name: str) -> dict[str, Any]:
        rows = self._rows("select options_json from sources where name = ?", (name,))
        if not rows or not rows[0].get("options_json"):
            return {}
        return json.loads(str(rows[0]["options_json"]))

    def remove_source(self, name: str) -> None:
        with self.connect() as db:
            db.execute("delete from sources where name = ?", (name,))

    def list_sources(self) -> list[dict[str, Any]]:
        rows = self._rows("select * from sources order by name")
        for row in rows:
            if row.get("options_json"):
                options = json.loads(str(row["options_json"]))
                row["options"] = _redact_options(options)
            row.pop("options_json", None)
        return rows

    def upsert_dataset(self, spec: DatasetSpec) -> None:
        now = _now()
        payload = json.dumps(_spec_payload(spec), sort_keys=True, default=str)
        spec_hash = hashlib.blake2b(payload.encode("utf-8"), digest_size=16).hexdigest()
        with self.connect() as db:
            db.execute(
                """
                insert into datasets(
                    name, source, source_dataset, category, enabled, spec_hash,
                    spec_json, created_at, updated_at
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                on conflict(source, name) do update set
                    source_dataset=excluded.source_dataset,
                    category=excluded.category,
                    enabled=excluded.enabled,
                    spec_hash=excluded.spec_hash,
                    spec_json=excluded.spec_json,
                    updated_at=excluded.updated_at
                """,
                (
                    spec.name,
                    spec.source,
                    spec.source_dataset,
                    spec.category,
                    int(spec.enabled),
                    spec_hash,
                    payload,
                    now,
                    now,
                ),
            )

    def set_dataset_enabled(self, source: str, dataset: str, enabled: bool) -> None:
        with self.connect() as db:
            db.execute(
                "update datasets set enabled = ?, updated_at = ? where source = ? and name = ?",
                (int(enabled), _now(), source, dataset),
            )

    def remove_dataset(self, source: str, dataset: str) -> None:
        with self.connect() as db:
            db.execute("delete from datasets where source = ? and name = ?", (source, dataset))

    def list_datasets(self, source: str | None = None) -> list[dict[str, Any]]:
        if source is None:
            return self._rows("select * from datasets order by source, name")
        return self._rows(
            "select * from datasets where source = ? order by name",
            (source,),
        )

    def get_dataset(self, source: str, dataset: str) -> dict[str, Any] | None:
        rows = self._rows(
            "select * from datasets where source = ? and name = ?",
            (source, dataset),
        )
        return rows[0] if rows else None

    def upsert_manifest(
        self,
        *,
        source: str,
        dataset: str,
        partition_path: str,
        partition_values: dict[str, Any],
        row_count: int,
        file_size_bytes: int,
        min_time: str | None,
        max_time: str | None,
        content_hash: str,
        schema_hash: str,
    ) -> None:
        self.upsert_manifests(
            [
                {
                    "source": source,
                    "dataset": dataset,
                    "partition_path": partition_path,
                    "partition_values": partition_values,
                    "row_count": row_count,
                    "file_size_bytes": file_size_bytes,
                    "min_time": min_time,
                    "max_time": max_time,
                    "content_hash": content_hash,
                    "schema_hash": schema_hash,
                }
            ]
        )

    def upsert_manifests(self, manifests: Iterable[dict[str, Any]]) -> None:
        rows = list(manifests)
        if not rows:
            return
        with self.connect() as db:
            db.executemany(
                """
                insert into partition_manifest(
                    source, dataset, partition_path, partition_values, row_count,
                    file_size_bytes, min_time, max_time, content_hash, schema_hash, updated_at
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                on conflict(source, dataset, partition_path) do update set
                    partition_values=excluded.partition_values,
                    row_count=excluded.row_count,
                    file_size_bytes=excluded.file_size_bytes,
                    min_time=excluded.min_time,
                    max_time=excluded.max_time,
                    content_hash=excluded.content_hash,
                    schema_hash=excluded.schema_hash,
                    updated_at=excluded.updated_at
                """,
                [
                    (
                        row["source"],
                        row["dataset"],
                        str(row["partition_path"]),
                        json.dumps(row["partition_values"], sort_keys=True, default=str),
                        int(row["row_count"]),
                        int(row["file_size_bytes"]),
                        row.get("min_time"),
                        row.get("max_time"),
                        row["content_hash"],
                        row["schema_hash"],
                        _now(),
                    )
                    for row in rows
                ],
            )

    def manifest(self, source: str | None = None, dataset: str | None = None) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if source is not None:
            clauses.append("source = ?")
            params.append(source)
        if dataset is not None:
            clauses.append("dataset = ?")
            params.append(dataset)
        where = f" where {' and '.join(clauses)}" if clauses else ""
        return self._rows(
            f"select * from partition_manifest{where} order by source, dataset, partition_path",
            params,
        )

    def record_run(
        self,
        *,
        run_id: str,
        source: str,
        dataset: str,
        mode: str,
        status: str,
        request_count: int = 0,
        success_count: int = 0,
        failure_count: int = 0,
        rows_downloaded: int = 0,
        rows_committed: int = 0,
        error_message: str | None = None,
    ) -> None:
        now = _now()
        with self.connect() as db:
            db.execute(
                """
                insert into ingestion_runs(
                    run_id, source, dataset, mode, started_at, finished_at, status,
                    request_count, success_count, failure_count, rows_downloaded,
                    rows_committed, error_message
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    source,
                    dataset,
                    mode,
                    now,
                    now,
                    status,
                    request_count,
                    success_count,
                    failure_count,
                    rows_downloaded,
                    rows_committed,
                    error_message,
                ),
            )

    def record_api_call(
        self,
        *,
        run_id: str,
        source: str,
        dataset: str,
        request_key: str,
        request_params: dict[str, Any],
        status: str,
        row_count: int = 0,
        retry_count: int = 0,
        error_message: str | None = None,
        asset_id: str | None = None,
    ) -> None:
        self.record_api_calls(
            [
                {
                    "run_id": run_id,
                    "source": source,
                    "dataset": dataset,
                    "request_key": request_key,
                    "request_params": request_params,
                    "status": status,
                    "row_count": row_count,
                    "retry_count": retry_count,
                    "error_message": error_message,
                    "asset_id": asset_id,
                }
            ]
        )

    def record_api_calls(self, calls: Iterable[dict[str, Any]]) -> None:
        rows = list(calls)
        if not rows:
            return
        now = _now()
        with self.connect() as db:
            db.executemany(
                """
                insert into api_calls(
                    run_id, source, dataset, request_key, asset_id, request_params,
                    status, row_count, retry_count, started_at, finished_at, error_message
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row["run_id"],
                        row["source"],
                        row["dataset"],
                        str(row["request_key"]),
                        row.get("asset_id"),
                        json.dumps(row["request_params"], sort_keys=True, default=str),
                        row["status"],
                        int(row.get("row_count", 0)),
                        int(row.get("retry_count", 0)),
                        now,
                        now,
                        row.get("error_message"),
                    )
                    for row in rows
                ],
            )

    def runs(self, limit: int = 20) -> list[dict[str, Any]]:
        return self._rows(
            "select * from ingestion_runs order by started_at desc limit ?",
            (limit,),
        )

    def _rows(self, sql: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
        with self.connect() as db:
            return [dict(row) for row in db.execute(sql, tuple(params)).fetchall()]

    def _initialize(self) -> None:
        with self.connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript(
                """
                create table if not exists sources (
                    name text primary key,
                    adapter text not null,
                    configured integer not null default 0,
                    options_json text,
                    created_at text not null,
                    updated_at text not null
                );
                create table if not exists datasets (
                    name text not null,
                    source text not null,
                    source_dataset text not null,
                    category text not null,
                    enabled integer not null default 1,
                    spec_hash text not null,
                    spec_json text not null,
                    created_at text not null,
                    updated_at text not null,
                    primary key(source, name)
                );
                create table if not exists ingestion_runs (
                    run_id text primary key,
                    source text not null,
                    dataset text not null,
                    mode text not null,
                    started_at text not null,
                    finished_at text,
                    status text not null,
                    request_count integer not null default 0,
                    success_count integer not null default 0,
                    failure_count integer not null default 0,
                    rows_downloaded integer not null default 0,
                    rows_committed integer not null default 0,
                    error_message text
                );
                create table if not exists api_calls (
                    run_id text not null,
                    source text not null,
                    dataset text not null,
                    request_key text not null,
                    asset_id text,
                    request_params text not null,
                    status text not null,
                    row_count integer not null default 0,
                    retry_count integer not null default 0,
                    started_at text not null,
                    finished_at text,
                    error_message text
                );
                create table if not exists asset_state (
                    source text not null,
                    dataset text not null,
                    asset_id text not null,
                    row_count integer not null,
                    min_time text,
                    max_time text,
                    content_hash text not null,
                    last_success_at text not null,
                    last_changed_at text not null,
                    primary key(source, dataset, asset_id)
                );
                create table if not exists partition_manifest (
                    source text not null,
                    dataset text not null,
                    partition_path text not null,
                    partition_values text not null,
                    row_count integer not null,
                    file_size_bytes integer not null,
                    min_time text,
                    max_time text,
                    content_hash text not null,
                    schema_hash text not null,
                    updated_at text not null,
                    primary key(source, dataset, partition_path)
                );
                create table if not exists rejected_summary (
                    run_id text not null,
                    source text not null,
                    dataset text not null,
                    reason text not null,
                    row_count integer not null,
                    created_at text not null
                );
                create table if not exists partition_locks (
                    source text not null,
                    dataset text not null,
                    partition_path text not null,
                    owner text not null,
                    acquired_at text not null,
                    expires_at text not null,
                    primary key(source, dataset, partition_path)
                );
                """
            )
            _ensure_column(db, "sources", "options_json", "text")


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _spec_payload(spec: DatasetSpec) -> dict[str, Any]:
    return {
        field: getattr(spec, field)
        for field in spec.__dataclass_fields__
    }


def _ensure_column(db: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    columns = {row["name"] for row in db.execute(f"pragma table_info({table})").fetchall()}
    if column not in columns:
        db.execute(f"alter table {table} add column {column} {definition}")


def _redact_options(options: dict[str, Any]) -> dict[str, Any]:
    redacted = dict(options)
    for key in list(redacted):
        if "token" in key.lower() or "secret" in key.lower() or "password" in key.lower():
            redacted[key] = "<redacted>"
    return redacted
