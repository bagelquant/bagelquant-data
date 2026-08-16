"""SQLite operational metadata."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import zlib
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import local
from typing import Any

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.exceptions import ConfigurationError


class MetadataStore:
    """SQLite metadata store using WAL mode."""

    _BUSY_TIMEOUT_MS = 30_000
    SCHEMA_VERSION = "3"

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._thread_state = local()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        """Yield one transactional connection and close owned connections."""

        active = getattr(self._thread_state, "writer_connection", None)
        if isinstance(active, sqlite3.Connection):
            with active as connection:
                yield connection
            return
        connection = self._new_connection()
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def _new_connection(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout={self._BUSY_TIMEOUT_MS}")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA synchronous=NORMAL")
        return connection

    @contextmanager
    def writer_session(self) -> Iterator[sqlite3.Connection]:
        """Reuse one scheduler-thread connection while preserving transactions."""

        active = getattr(self._thread_state, "writer_connection", None)
        if isinstance(active, sqlite3.Connection):
            yield active
            return
        connection = self._new_connection()
        self._thread_state.writer_connection = connection
        try:
            yield connection
        finally:
            del self._thread_state.writer_connection
            connection.close()

    def upsert_source(
        self,
        name: str,
        adapter: str,
        configured: bool = False,
        enabled: bool = True,
        options: dict[str, Any] | None = None,
    ) -> None:
        now = _now()
        options_json = (
            None
            if options is None
            else json.dumps(options, sort_keys=True, default=str)
        )
        with self.connect() as db:
            db.execute(
                """
                insert into sources(name, adapter, configured, enabled, options_json, created_at, updated_at)
                values (?, ?, ?, ?, ?, ?, ?)
                on conflict(name) do update set
                    adapter=excluded.adapter,
                    configured=excluded.configured,
                    enabled=excluded.enabled,
                    options_json=coalesce(excluded.options_json, sources.options_json),
                    updated_at=excluded.updated_at
                """,
                (name, adapter, int(configured), int(enabled), options_json, now, now),
            )

    def source_options(self, name: str) -> dict[str, Any]:
        rows = self._rows("select options_json from sources where name = ?", (name,))
        if not rows or not rows[0].get("options_json"):
            return {}
        return json.loads(str(rows[0]["options_json"]))

    def remove_source(self, name: str) -> None:
        with self.connect() as db:
            db.execute("delete from sources where name = ?", (name,))

    def set_source_enabled(self, name: str, enabled: bool) -> None:
        with self.connect() as db:
            db.execute(
                "update sources set enabled = ?, updated_at = ? where name = ?",
                (int(enabled), _now(), name),
            )

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
                    name, source, enabled, spec_hash, spec_json, created_at, updated_at
                )
                values (?, ?, ?, ?, ?, ?, ?)
                on conflict(source, name) do update set
                    spec_hash=excluded.spec_hash,
                    spec_json=excluded.spec_json,
                    updated_at=excluded.updated_at
                """,
                (
                    spec.name,
                    spec.source,
                    1,
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
            db.execute(
                "delete from dataset_schemas where source = ? and dataset = ?",
                (source, dataset),
            )
            db.execute(
                "delete from datasets where source = ? and name = ?", (source, dataset)
            )

    def clear_dataset_data(self, source: str, dataset: str) -> dict[str, int]:
        """Clear current dataset state while preserving its registration and audit."""

        with self.connect() as db:
            db.execute("begin immediate")
            lease = db.execute(
                "select 1 from update_leases where source=? and dataset=?",
                (source, dataset),
            ).fetchone()
            if lease is not None:
                raise RuntimeError(f"Dataset update is active: {source}/{dataset}")
            manifest = db.execute(
                "select count(*), coalesce(sum(row_count), 0) from partition_manifest "
                "where source=? and dataset=?",
                (source, dataset),
            ).fetchone()
            scopes = db.execute(
                "select count(*) from update_scopes where source=? and dataset=?",
                (source, dataset),
            ).fetchone()
            db.execute(
                "delete from partition_manifest where source=? and dataset=?",
                (source, dataset),
            )
            db.execute(
                "delete from dataset_schemas where source=? and dataset=?",
                (source, dataset),
            )
            db.execute(
                "delete from provider_scope_checks where scope_id in "
                "(select id from update_scopes where source=? and dataset=?)",
                (source, dataset),
            )
            db.execute(
                "delete from update_scopes where source=? and dataset=?",
                (source, dataset),
            )
            db.execute(
                "delete from update_leases where source=? and dataset=?",
                (source, dataset),
            )
        return {
            "partitions": int(manifest[0]),
            "rows": int(manifest[1]),
            "scopes": int(scopes[0]),
        }

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

    def dataset_schema(self, source: str, dataset: str) -> bytes | None:
        """Return the serialized canonical Arrow schema for a dataset."""

        rows = self._rows(
            "select schema_ipc from dataset_schemas where source=? and dataset=?",
            (source, dataset),
        )
        if not rows:
            return None
        return bytes(rows[0]["schema_ipc"])

    def dataset_schema_hashes(self, source: str) -> dict[str, str]:
        """Return canonical schema hashes for a source in one read."""

        return {
            str(row["dataset"]): str(row["schema_hash"])
            for row in self._rows(
                "select dataset, schema_hash from dataset_schemas "
                "where source = ? order by dataset",
                (source,),
            )
        }

    def dataset_statuses(
        self,
        *,
        source: str | None = None,
        datasets: Iterable[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Aggregate exact manifest-backed status in one SQLite query."""

        selected = None if datasets is None else tuple(dict.fromkeys(datasets))
        clauses: list[str] = []
        parameters: list[Any] = []
        if source is not None:
            clauses.append("d.source = ?")
            parameters.append(source)
        if selected is not None:
            if not selected:
                return []
            clauses.append(f"d.name IN ({','.join('?' for _ in selected)})")
            parameters.extend(selected)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return self._rows(
            f"""
            SELECT d.source AS source, d.name AS dataset,
                   COUNT(m.partition_path) AS file_count,
                   COUNT(m.partition_path) AS partition_count,
                   COALESCE(SUM(m.file_size_bytes), 0) AS total_size,
                   COALESCE(SUM(m.row_count), 0) AS row_count,
                   MIN(m.min_time) AS minimum_time,
                   MAX(m.max_time) AS maximum_time,
                   MAX(m.updated_at) AS last_update
            FROM datasets AS d
            LEFT JOIN partition_manifest AS m
              ON m.source = d.source AND m.dataset = d.name
            {where}
            GROUP BY d.source, d.name
            ORDER BY d.source, d.name
            """,
            tuple(parameters),
        )

    def upsert_dataset_schema(
        self,
        source: str,
        dataset: str,
        *,
        schema_ipc: bytes,
        schema_hash: str,
    ) -> None:
        """Persist the current canonical schema after canonical files commit."""

        with self.connect() as db:
            db.execute(
                """
                insert into dataset_schemas(
                    source,dataset,schema_ipc,schema_hash,updated_at
                ) values (?, ?, ?, ?, ?)
                on conflict(source,dataset) do update set
                    schema_ipc=excluded.schema_ipc,
                    schema_hash=excluded.schema_hash,
                    updated_at=excluded.updated_at
                """,
                (source, dataset, schema_ipc, schema_hash, _now()),
            )

    def commit_dataset_metadata(
        self,
        source: str,
        dataset: str,
        *,
        manifests: Iterable[dict[str, Any]],
        schema_ipc: bytes,
        schema_hash: str,
        replace_manifests: bool = False,
    ) -> None:
        """Commit changed manifests and the canonical schema atomically."""

        rows = list(manifests)
        now = _now()
        with self.connect() as db:
            db.execute("begin immediate")
            if replace_manifests:
                db.execute(
                    "delete from partition_manifest where source=? and dataset=?",
                    (source, dataset),
                )
            if rows:
                db.executemany(
                    """
                    insert into partition_manifest(
                        source,dataset,partition_path,partition_values,row_count,
                        file_size_bytes,min_time,max_time,content_hash,schema_hash,
                        updated_at
                    ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    on conflict(source,dataset,partition_path) do update set
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
                            json.dumps(
                                row["partition_values"],
                                sort_keys=True,
                                default=str,
                            ),
                            int(row["row_count"]),
                            int(row["file_size_bytes"]),
                            row.get("min_time"),
                            row.get("max_time"),
                            row["content_hash"],
                            row["schema_hash"],
                            now,
                        )
                        for row in rows
                    ],
                )
            db.execute(
                """
                insert into dataset_schemas(
                    source,dataset,schema_ipc,schema_hash,updated_at
                ) values (?, ?, ?, ?, ?)
                on conflict(source,dataset) do update set
                    schema_ipc=excluded.schema_ipc,
                    schema_hash=excluded.schema_hash,
                    updated_at=excluded.updated_at
                """,
                (source, dataset, schema_ipc, schema_hash, now),
            )

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
                        json.dumps(
                            row["partition_values"], sort_keys=True, default=str
                        ),
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

    def replace_manifests(
        self, source: str, dataset: str, manifests: Iterable[dict[str, Any]]
    ) -> None:
        rows = list(manifests)
        now = _now()
        with self.connect() as db:
            db.execute(
                "delete from partition_manifest where source = ? and dataset = ?",
                (source, dataset),
            )
            if rows:
                db.executemany(
                    """
                    insert into partition_manifest(
                        source, dataset, partition_path, partition_values, row_count,
                        file_size_bytes, min_time, max_time, content_hash, schema_hash, updated_at
                    )
                    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            row["source"],
                            row["dataset"],
                            str(row["partition_path"]),
                            json.dumps(
                                row["partition_values"], sort_keys=True, default=str
                            ),
                            int(row["row_count"]),
                            int(row["file_size_bytes"]),
                            row.get("min_time"),
                            row.get("max_time"),
                            row["content_hash"],
                            row["schema_hash"],
                            now,
                        )
                        for row in rows
                    ],
                )

    def remove_manifests(
        self, source: str, dataset: str, partition_paths: Iterable[str]
    ) -> list[dict[str, Any]]:
        """Remove selected manifest rows while no dataset writer is active."""

        paths = tuple(dict.fromkeys(str(path) for path in partition_paths))
        if not paths:
            return []
        placeholders = ",".join("?" for _ in paths)
        with self.connect() as db:
            db.execute("begin immediate")
            lease = db.execute(
                "select 1 from update_leases where source=? and dataset=?",
                (source, dataset),
            ).fetchone()
            if lease is not None:
                raise RuntimeError(f"Dataset update is active: {source}/{dataset}")
            rows = db.execute(
                "select * from partition_manifest where source=? and dataset=? "
                f"and partition_path in ({placeholders}) order by partition_path",
                (source, dataset, *paths),
            ).fetchall()
            db.execute(
                "delete from partition_manifest where source=? and dataset=? "
                f"and partition_path in ({placeholders})",
                (source, dataset, *paths),
            )
        return [
            {
                **dict(row),
                "partition_values": json.loads(str(row["partition_values"])),
            }
            for row in rows
        ]

    def manifest(
        self, source: str | None = None, dataset: str | None = None
    ) -> list[dict[str, Any]]:
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
        empty_count: int = 0,
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
                    request_count, success_count, empty_count, failure_count, rows_downloaded,
                    rows_committed, error_message
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    empty_count,
                    failure_count,
                    rows_downloaded,
                    rows_committed,
                    error_message,
                ),
            )

    def begin_run(
        self,
        *,
        run_id: str,
        source: str,
        dataset: str,
        mode: str,
        owner_id: str | None = None,
    ) -> None:
        """Create an ingestion run before any scope is claimed."""

        now = _now()
        with self.connect() as db:
            db.execute(
                """
                insert into ingestion_runs(
                    run_id, source, dataset, mode, started_at, status, owner_id
                ) values (?, ?, ?, ?, ?, 'running', ?)
                """,
                (run_id, source, dataset, mode, now, owner_id),
            )

    def finalize_run(
        self,
        *,
        run_id: str,
        status: str,
        request_count: int,
        success_count: int,
        empty_count: int,
        failure_count: int,
        rows_downloaded: int,
        rows_committed: int,
        error_message: str | None = None,
    ) -> None:
        """Finalize a run even when the update scheduler raises."""

        with self.connect() as db:
            db.execute(
                """
                update ingestion_runs set
                    finished_at=?, status=?, request_count=?, success_count=?,
                    empty_count=?, failure_count=?, rows_downloaded=?, rows_committed=?,
                    error_message=?
                where run_id=?
                """,
                (
                    _now(),
                    status,
                    int(request_count),
                    int(success_count),
                    int(empty_count),
                    int(failure_count),
                    int(rows_downloaded),
                    int(rows_committed),
                    error_message,
                    run_id,
                ),
            )

    def record_rejected(
        self,
        *,
        run_id: str,
        source: str,
        dataset: str,
        reason: str,
        row_count: int,
    ) -> None:
        with self.connect() as db:
            db.execute(
                """
                insert into rejected_summary(run_id, source, dataset, reason, row_count, created_at)
                values (?, ?, ?, ?, ?, ?)
                """,
                (run_id, source, dataset, reason, int(row_count), _now()),
            )

    def rejected(
        self, source: str | None = None, dataset: str | None = None
    ) -> list[dict[str, Any]]:
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
            f"select * from rejected_summary{where} order by created_at desc",
            params,
        )

    def record_api_calls(self, calls: Iterable[dict[str, Any]]) -> None:
        rows = list(calls)
        if not rows:
            return
        now = _now()
        with self.connect() as db:
            self._insert_api_calls(db, rows, recorded_at=now)
            for run_id in {str(row["run_id"]) for row in rows}:
                run_rows = [row for row in rows if str(row["run_id"]) == run_id]
                db.execute(
                    """
                    update ingestion_runs set request_count=request_count+?,
                        rows_downloaded=rows_downloaded+?
                    where run_id=? and status='running'
                    """,
                    (
                        len(run_rows),
                        sum(
                            int(row.get("row_count", 0))
                            for row in run_rows
                            if row.get("status") == "success"
                        ),
                        run_id,
                    ),
                )

    @staticmethod
    def _insert_api_calls(
        db: sqlite3.Connection,
        rows: Iterable[dict[str, Any]],
        *,
        recorded_at: str,
    ) -> None:
        db.executemany(
            """
            insert into api_calls(
                run_id, source, dataset, request_key, asset_id, request_params,
                status, result_kind, row_count, retry_count, started_at, finished_at,
                error_message, scope_id, request_kind
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row["run_id"],
                    row["source"],
                    row["dataset"],
                    str(row["request_key"]),
                    row.get("asset_id"),
                    zlib.compress(
                        json.dumps(
                            row["request_params"], sort_keys=True, default=str
                        ).encode("utf-8"),
                        level=1,
                    ),
                    row["status"],
                    row.get("result_kind") or _api_result_kind(row),
                    int(row.get("row_count", 0)),
                    int(row.get("retry_count", 0)),
                    recorded_at,
                    recorded_at,
                    row.get("error_message"),
                    row.get("scope_id"),
                    row.get("request_kind"),
                )
                for row in rows
            ],
        )

    def synchronize_update_scopes(self, scopes: Iterable[dict[str, Any]]) -> None:
        """Insert ledger identities and invalidate rows whose spec changed."""

        rows = list(scopes)
        if not rows:
            return
        now = _now()
        with self.connect() as db:
            identities = {
                (str(row["source"]), str(row["dataset"]), str(row["spec_hash"]))
                for row in rows
            }
            for source, dataset, spec_hash in identities:
                db.execute(
                    "delete from provider_scope_checks where scope_id in ("
                    "select id from update_scopes where source=? and dataset=? "
                    "and spec_hash != ?)",
                    (source, dataset, spec_hash),
                )
            db.executemany(
                """
                insert into update_scopes(
                    source,dataset,scope_kind,scope_key,variant_hash,status,
                    initial_start,spec_hash,created_at,updated_at
                ) values (?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?)
                on conflict(source,dataset,scope_kind,scope_key,variant_hash)
                do update set
                    initial_start=excluded.initial_start,
                    status=case
                        when update_scopes.spec_hash != excluded.spec_hash then 'pending'
                        else update_scopes.status
                    end,
                    checked_through=case
                        when update_scopes.spec_hash != excluded.spec_hash then null
                        else update_scopes.checked_through
                    end,
                    last_error=case
                        when update_scopes.spec_hash != excluded.spec_hash then null
                        else update_scopes.last_error
                    end,
                    active_run_id=case
                        when update_scopes.spec_hash != excluded.spec_hash then null
                        else update_scopes.active_run_id
                    end,
                    spec_hash=excluded.spec_hash,
                    updated_at=excluded.updated_at
                where update_scopes.spec_hash != excluded.spec_hash
                   or update_scopes.initial_start is not excluded.initial_start
                """,
                [
                    (
                        row["source"],
                        row["dataset"],
                        row["scope_kind"],
                        row["scope_key"],
                        row["variant_hash"],
                        row.get("initial_start"),
                        row["spec_hash"],
                        now,
                        now,
                    )
                    for row in rows
                ],
            )

    def update_scopes(
        self,
        *,
        source: str | None = None,
        dataset: str | None = None,
        status: str | Iterable[str] | None = None,
        scope_kind: str | None = None,
        scope_keys: Iterable[str] | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if source is not None:
            clauses.append("source=?")
            params.append(source)
        if dataset is not None:
            clauses.append("dataset=?")
            params.append(dataset)
        if scope_kind is not None:
            clauses.append("scope_kind=?")
            params.append(scope_kind)
        if status is not None:
            statuses = [status] if isinstance(status, str) else list(status)
            if not statuses:
                return []
            clauses.append(f"status in ({','.join('?' for _ in statuses)})")
            params.extend(statuses)
        if scope_keys is not None:
            keys = list(scope_keys)
            if not keys:
                return []
            clauses.append(f"scope_key in ({','.join('?' for _ in keys)})")
            params.extend(keys)
        where = f" where {' and '.join(clauses)}" if clauses else ""
        return self._rows(
            f"select * from update_scopes{where} "
            "order by source,dataset,scope_key,variant_hash",
            params,
        )

    def remove_obsolete_update_scopes(
        self, *, source: str, dataset: str, spec_hash: str
    ) -> int:
        """Remove identities that can no longer be reconstructed from the spec."""

        with self.connect() as db:
            db.execute(
                "delete from provider_scope_checks where scope_id in ("
                "select id from update_scopes where source=? and dataset=? "
                "and spec_hash != ? and status != 'running')",
                (source, dataset, spec_hash),
            )
            cursor = db.execute(
                "delete from update_scopes where source=? and dataset=? "
                "and spec_hash != ? and status != 'running'",
                (source, dataset, spec_hash),
            )
            return int(cursor.rowcount)

    def claim_update_scopes(
        self, scope_ids: Iterable[int], *, run_id: str
    ) -> list[int]:
        ids = list(dict.fromkeys(int(scope_id) for scope_id in scope_ids))
        if not ids:
            return []
        now = _now()
        placeholders = ",".join("?" for _ in ids)
        with self.connect() as db:
            db.execute("begin immediate")
            claimable = [
                int(row["id"])
                for row in db.execute(
                    f"select id from update_scopes where id in ({placeholders}) "
                    "and status in ('pending','failed','success','empty')",
                    ids,
                ).fetchall()
            ]
            if claimable:
                claimed_placeholders = ",".join("?" for _ in claimable)
                db.execute(
                    f"update update_scopes set status='running',active_run_id=?,"
                    f"attempt_count=attempt_count+1,last_attempt_at=?,updated_at=? "
                    f"where id in ({claimed_placeholders})",
                    (run_id, now, now, *claimable),
                )
            return claimable

    def transition_update_scopes(
        self,
        transitions: Iterable[dict[str, Any]],
        *,
        run_id: str,
        committed_rows: int = 0,
    ) -> None:
        """Commit scope outcomes in one metadata transaction."""

        rows = list(transitions)
        if not rows:
            return
        now = _now()
        with self.connect() as db:
            for row in rows:
                status = str(row["status"])
                if status not in {"success", "failed", "invalid"}:
                    raise ValueError(f"Unsupported scope transition: {status}")
                cursor = db.execute(
                    """
                    update update_scopes set
                        status=?, checked_through=case
                            when ?='success' then coalesce(?,data_max_time,checked_through)
                            else checked_through
                        end,
                        data_max_time=case when ?='success' then coalesce(?,data_max_time)
                            else data_max_time end,
                        row_count=case when ?='success' then ? else row_count end,
                        last_success_at=case when ?='success' then ? else last_success_at end,
                        last_revision_check_at=null,
                        recheck_after=null, last_error=?, active_run_id=null,
                        commit_run_id=case when ?='success' then ? else commit_run_id end,
                        updated_at=?
                    where id=? and active_run_id=?
                    """,
                    (
                        status,
                        status,
                        row.get("data_max_time"),
                        status,
                        row.get("data_max_time"),
                        status,
                        int(row.get("row_count", 0)),
                        status,
                        now,
                        row.get("last_error"),
                        status,
                        run_id,
                        now,
                        int(row["scope_id"]),
                        run_id,
                    ),
                )
                if cursor.rowcount != 1:
                    raise RuntimeError(
                        f"Scope {row['scope_id']} is not claimed by ingestion run {run_id}"
                    )
                if status == "success" and row.get("provider_checked_through"):
                    self._upsert_provider_scope_check(
                        db,
                        scope_id=int(row["scope_id"]),
                        checked_through=str(row["provider_checked_through"]),
                        recheck_after=row.get("provider_recheck_after"),
                        result="nonempty",
                        checked_at=now,
                    )
            success_count = sum(row["status"] == "success" for row in rows)
            if success_count:
                db.execute(
                    """
                    update ingestion_runs set success_count=success_count+?,
                        rows_committed=rows_committed+?
                    where run_id=? and status='running'
                    """,
                    (success_count, int(committed_rows), run_id),
                )

    def record_empty_scope_result(
        self,
        *,
        calls: Iterable[dict[str, Any]],
        scope_id: int | None,
        run_id: str,
        checked_through: str | None,
        recheck_after: str | None,
    ) -> None:
        """Atomically persist one validated empty provider result.

        Local coverage columns are deliberately untouched. A process interruption can
        therefore expose either the complete empty outcome or none of it.
        """

        rows = list(calls)
        if not rows:
            raise ValueError("An empty result must include at least one API audit row")
        now = _now()
        with self.connect() as db:
            db.execute("begin immediate")
            if scope_id is not None:
                claimed = db.execute(
                    "select id from update_scopes where id=? and status='running' "
                    "and active_run_id=?",
                    (scope_id, run_id),
                ).fetchone()
                if claimed is None:
                    raise RuntimeError(
                        f"Scope {scope_id} is not claimed by ingestion run {run_id}"
                    )
                if checked_through is None:
                    raise ValueError(
                        "An incremental empty result needs checked_through"
                    )
            self._insert_api_calls(db, rows, recorded_at=now)
            if scope_id is not None:
                assert checked_through is not None
                self._upsert_provider_scope_check(
                    db,
                    scope_id=scope_id,
                    checked_through=checked_through,
                    recheck_after=recheck_after,
                    result="empty",
                    checked_at=now,
                )
                cursor = db.execute(
                    """
                    update update_scopes set status='empty',last_error=null,
                        active_run_id=null,updated_at=?
                    where id=? and active_run_id=?
                    """,
                    (now, scope_id, run_id),
                )
            cursor = db.execute(
                """
                update ingestion_runs set request_count=request_count+?,
                    empty_count=empty_count+1
                where run_id=? and status='running'
                """,
                (len(rows), run_id),
            )
            if cursor.rowcount != 1:
                raise RuntimeError(f"Ingestion run {run_id} is not running")

    @staticmethod
    def _upsert_provider_scope_check(
        db: sqlite3.Connection,
        *,
        scope_id: int,
        checked_through: str,
        recheck_after: object,
        result: str,
        checked_at: str,
    ) -> None:
        db.execute(
            """
            insert into provider_scope_checks(
                scope_id,checked_through,last_checked_at,recheck_after,last_result
            ) values (?, ?, ?, ?, ?)
            on conflict(scope_id) do update set
                checked_through=case
                    when provider_scope_checks.checked_through < excluded.checked_through
                    then excluded.checked_through
                    else provider_scope_checks.checked_through
                end,
                last_checked_at=excluded.last_checked_at,
                recheck_after=excluded.recheck_after,
                last_result=excluded.last_result
            """,
            (scope_id, checked_through, checked_at, recheck_after, result),
        )

    def provider_scope_checks(
        self, *, source: str | None = None, dataset: str | None = None
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if source is not None:
            clauses.append("s.source=?")
            params.append(source)
        if dataset is not None:
            clauses.append("s.dataset=?")
            params.append(dataset)
        where = f" where {' and '.join(clauses)}" if clauses else ""
        return self._rows(
            "select c.*,s.source,s.dataset,s.scope_kind,s.scope_key,s.variant_hash "
            "from provider_scope_checks c join update_scopes s on s.id=c.scope_id"
            f"{where} order by s.source,s.dataset,s.scope_key,s.variant_hash",
            params,
        )

    def update_scopes_with_checks(
        self, *, source: str, dataset: str, scope_kind: str
    ) -> list[dict[str, Any]]:
        """Return update scopes and provider observations in one indexed query."""

        return self._rows(
            """
            select s.*,
                c.checked_through as provider_checked_through,
                c.last_checked_at as provider_last_checked_at,
                c.recheck_after as provider_recheck_after,
                c.last_result as provider_last_result
            from update_scopes s
            left join provider_scope_checks c on c.scope_id=s.id
            where s.source=? and s.dataset=? and s.scope_kind=?
            order by s.scope_key,s.variant_hash
            """,
            (source, dataset, scope_kind),
        )

    def reset_update_scopes(
        self, scope_ids: Iterable[int], *, clear_watermark: bool = False
    ) -> int:
        ids = list(dict.fromkeys(int(scope_id) for scope_id in scope_ids))
        if not ids:
            return 0
        placeholders = ",".join("?" for _ in ids)
        with self.connect() as db:
            checked_through = ",checked_through=null" if clear_watermark else ""
            if clear_watermark:
                db.execute(
                    "delete from provider_scope_checks where scope_id in "
                    f"({placeholders})",
                    ids,
                )
            cursor = db.execute(
                f"update update_scopes set status='pending'{checked_through},"
                f"last_error=null,active_run_id=null,recheck_after=null,updated_at=? "
                f"where id in ({placeholders}) and status in "
                "('failed','invalid','empty','success')",
                (_now(), *ids),
            )
            return int(cursor.rowcount)

    def acquire_update_leases(
        self,
        leases: Iterable[tuple[str, str, str]],
        *,
        ttl_seconds: int = 300,
        owner_id: str | None = None,
    ) -> None:
        rows = list(leases)
        if not rows:
            return
        now = datetime.now(UTC)
        expires = (now + timedelta(seconds=ttl_seconds)).isoformat()
        with self.connect() as db:
            db.execute("begin immediate")
            db.execute(
                "delete from update_leases where lease_expires_at <= ?",
                (now.isoformat(),),
            )
            for source, dataset, run_id in rows:
                conflict = db.execute(
                    "select run_id from update_leases where source=? and dataset=?",
                    (source, dataset),
                ).fetchone()
                if conflict is not None and conflict["run_id"] != run_id:
                    raise RuntimeError(
                        f"Dataset update already active: {source}/{dataset}"
                    )
            db.executemany(
                """
                insert into update_leases(
                    source,dataset,run_id,owner_id,heartbeat_at,lease_expires_at
                )
                values (?, ?, ?, ?, ?, ?)
                on conflict(source,dataset) do update set
                    run_id=excluded.run_id,owner_id=excluded.owner_id,
                    heartbeat_at=excluded.heartbeat_at,
                    lease_expires_at=excluded.lease_expires_at
                """,
                [
                    (source, dataset, run_id, owner_id, now.isoformat(), expires)
                    for source, dataset, run_id in rows
                ],
            )

    def abandon_update_owner(self, owner_id: str, *, reason: str) -> dict[str, int]:
        """Release one workflow owner's unfinished writes after forced termination."""

        now = _now()
        with self.connect() as db:
            db.execute("begin immediate")
            run_ids = {
                str(row["run_id"])
                for row in db.execute(
                    "select run_id from ingestion_runs where owner_id=? and status='running'",
                    (owner_id,),
                ).fetchall()
            }
            run_ids.update(
                str(row["run_id"])
                for row in db.execute(
                    "select run_id from update_leases where owner_id=?", (owner_id,)
                ).fetchall()
            )
            scope_count = 0
            if run_ids:
                placeholders = ",".join("?" for _ in run_ids)
                cursor = db.execute(
                    f"update update_scopes set status='failed',active_run_id=null,"
                    f"last_error=?,updated_at=? where status='running' "
                    f"and active_run_id in ({placeholders})",
                    (reason, now, *sorted(run_ids)),
                )
                scope_count = int(cursor.rowcount)
                db.execute(
                    f"update ingestion_runs set status='cancelled',finished_at=?,"
                    f"error_message=? where status='running' and run_id in ({placeholders})",
                    (now, reason, *sorted(run_ids)),
                )
            leases = db.execute(
                "delete from update_leases where owner_id=?", (owner_id,)
            ).rowcount
            return {
                "runs": len(run_ids),
                "scopes": scope_count,
                "leases": int(leases),
            }

    def refresh_update_lease(self, *, run_id: str, ttl_seconds: int = 300) -> None:
        now = datetime.now(UTC)
        with self.connect() as db:
            db.execute(
                "update update_leases set heartbeat_at=?,lease_expires_at=? where run_id=?",
                (
                    now.isoformat(),
                    (now + timedelta(seconds=ttl_seconds)).isoformat(),
                    run_id,
                ),
            )

    def release_update_leases(self, run_ids: Iterable[str]) -> None:
        ids = list(run_ids)
        if not ids:
            return
        placeholders = ",".join("?" for _ in ids)
        with self.connect() as db:
            db.execute(
                f"delete from update_leases where run_id in ({placeholders})", ids
            )

    def active_update_leases(self) -> list[dict[str, Any]]:
        return self._rows(
            "select * from update_leases where lease_expires_at > ? order by source,dataset",
            (_now(),),
        )

    def recover_stale_running_scopes(self) -> int:
        now = _now()
        with self.connect() as db:
            db.execute("begin immediate")
            stale_runs = [
                str(row["active_run_id"])
                for row in db.execute(
                    """
                    select distinct active_run_id from update_scopes
                    where status='running' and not exists (
                        select 1 from update_leases
                        where update_leases.run_id=update_scopes.active_run_id
                          and update_leases.lease_expires_at > ?
                    )
                    """,
                    (now,),
                ).fetchall()
                if row["active_run_id"] is not None
            ]
            cursor = db.execute(
                """
                update update_scopes set status='failed',active_run_id=null,
                    last_error='writer lease expired',updated_at=?
                where status='running' and not exists (
                    select 1 from update_leases
                    where update_leases.run_id=update_scopes.active_run_id
                      and update_leases.lease_expires_at > ?
                )
                """,
                (now, now),
            )
            db.execute("delete from update_leases where lease_expires_at <= ?", (now,))
            if stale_runs:
                placeholders = ",".join("?" for _ in stale_runs)
                db.execute(
                    f"update ingestion_runs set status='cancelled',finished_at=?,"
                    f"error_message='writer lease expired' where status='running' "
                    f"and run_id in ({placeholders})",
                    (now, *stale_runs),
                )
            return int(cursor.rowcount)

    def dataset_spec_hash(self, source: str, dataset: str) -> str:
        rows = self._rows(
            "select spec_hash from datasets where source=? and name=?",
            (source, dataset),
        )
        if not rows:
            raise KeyError(f"Unknown dataset: {source}/{dataset}")
        return str(rows[0]["spec_hash"])

    def runs(self, limit: int = 20) -> list[dict[str, Any]]:
        return self._rows(
            "select * from ingestion_runs order by started_at desc limit ?",
            (limit,),
        )

    def _rows(self, sql: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
        with self.connect() as db:
            rows = [dict(row) for row in db.execute(sql, tuple(params)).fetchall()]
        for row in rows:
            request_params = row.get("request_params")
            if isinstance(request_params, bytes):
                row["request_params"] = zlib.decompress(request_params).decode("utf-8")
        return rows

    def _initialize(self) -> None:
        with self.connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            existing_tables = {
                str(row["name"])
                for row in db.execute(
                    "select name from sqlite_master where type='table'"
                ).fetchall()
            }
            if existing_tables:
                schema_version = None
                if "metadata_state" in existing_tables:
                    row = db.execute(
                        "select value from metadata_state where key='schema_version'"
                    ).fetchone()
                    schema_version = None if row is None else str(row["value"])
                if schema_version != self.SCHEMA_VERSION:
                    raise ConfigurationError(
                        "Incompatible data-lake metadata schema "
                        f"({schema_version or 'unversioned'}). Stop all workers, delete "
                        "the old lake, and create a fresh lake; automatic migration is "
                        "intentionally disabled."
                    )
            db.executescript(
                """
                create table if not exists sources (
                    name text primary key,
                    adapter text not null,
                    configured integer not null default 0,
                    enabled integer not null default 1,
                    options_json text,
                    created_at text not null,
                    updated_at text not null
                );
                create table if not exists datasets (
                    name text not null,
                    source text not null,
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
                    empty_count integer not null default 0,
                    failure_count integer not null default 0,
                    rows_downloaded integer not null default 0,
                    rows_committed integer not null default 0,
                    error_message text,
                    owner_id text
                );
                create table if not exists api_calls (
                    run_id text not null,
                    source text not null,
                    dataset text not null,
                    request_key text not null,
                    asset_id text,
                    request_params blob not null,
                    status text not null,
                    result_kind text not null check(result_kind in (
                        'nonempty','empty','transport_failure','invalid','cancelled'
                    )),
                    row_count integer not null default 0,
                    retry_count integer not null default 0,
                    started_at text not null,
                    finished_at text,
                    error_message text,
                    scope_id integer,
                    request_kind text
                );
                create table if not exists update_scopes (
                    id integer primary key autoincrement,
                    source text not null,
                    dataset text not null,
                    scope_kind text not null,
                    scope_key text not null,
                    variant_hash text not null,
                    status text not null check(status in (
                        'pending','running','success','empty','failed','invalid'
                    )),
                    initial_start text,
                    checked_through text,
                    data_max_time text,
                    row_count integer not null default 0,
                    attempt_count integer not null default 0,
                    last_attempt_at text,
                    last_success_at text,
                    last_revision_check_at text,
                    recheck_after text,
                    last_error text,
                    spec_hash text not null,
                    active_run_id text,
                    commit_run_id text,
                    created_at text not null,
                    updated_at text not null,
                    unique(source,dataset,scope_kind,scope_key,variant_hash)
                );
                create index if not exists idx_update_scopes_eligibility
                    on update_scopes(
                        source,dataset,scope_kind,spec_hash,status,scope_key
                    );
                create table if not exists provider_scope_checks (
                    scope_id integer primary key,
                    checked_through text not null,
                    last_checked_at text not null,
                    recheck_after text,
                    last_result text not null check(last_result in ('empty','nonempty'))
                );
                create table if not exists update_leases (
                    source text not null,
                    dataset text not null,
                    run_id text not null unique,
                    owner_id text,
                    heartbeat_at text not null,
                    lease_expires_at text not null,
                    primary key(source, dataset)
                );
                create table if not exists metadata_state (
                    key text primary key,
                    value text not null,
                    updated_at text not null
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
                create table if not exists dataset_schemas (
                    source text not null,
                    dataset text not null,
                    schema_ipc blob not null,
                    schema_hash text not null,
                    updated_at text not null,
                    primary key(source,dataset)
                );
                create table if not exists rejected_summary (
                    run_id text not null,
                    source text not null,
                    dataset text not null,
                    reason text not null,
                    row_count integer not null,
                    created_at text not null
                );
                """
            )
            if not existing_tables:
                now = _now()
                db.execute(
                    """
                    insert into metadata_state(key,value,updated_at) values
                        ('schema_version',?,?)
                    """,
                    (
                        self.SCHEMA_VERSION,
                        now,
                    ),
                )


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _spec_payload(spec: DatasetSpec) -> dict[str, Any]:
    payload = asdict(spec)
    if payload["source_api"] is None:
        payload.pop("source_api")
    if payload["request_discovery"] is None:
        payload.pop("request_discovery")
    return payload


def _api_result_kind(row: dict[str, Any]) -> str:
    status = str(row["status"])
    if status == "invalid":
        return "invalid"
    if status == "cancelled":
        return "cancelled"
    if status != "success":
        return "transport_failure"
    return "empty" if int(row.get("row_count", 0)) == 0 else "nonempty"


def _redact_options(options: dict[str, Any]) -> dict[str, Any]:
    redacted = dict(options)
    for key in list(redacted):
        if (
            "token" in key.lower()
            or "secret" in key.lower()
            or "password" in key.lower()
        ):
            redacted[key] = "<redacted>"
    return redacted
