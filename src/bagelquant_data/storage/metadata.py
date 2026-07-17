"""SQLite operational metadata."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.exceptions import ConfigurationError


class MetadataStore:
    """SQLite metadata store using WAL mode."""

    _BUSY_TIMEOUT_MS = 30_000
    UPDATE_STATE_VERSION = "1"

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
                "delete from datasets where source = ? and name = ?", (source, dataset)
            )

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

    def begin_run(self, *, run_id: str, source: str, dataset: str, mode: str) -> None:
        """Create an ingestion run before any scope is claimed."""

        now = _now()
        with self.connect() as db:
            db.execute(
                """
                insert into ingestion_runs(
                    run_id, source, dataset, mode, started_at, status
                ) values (?, ?, ?, ?, ?, 'running')
                """,
                (run_id, source, dataset, mode, now),
            )

    def finalize_run(
        self,
        *,
        run_id: str,
        status: str,
        request_count: int,
        success_count: int,
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
                    failure_count=?, rows_downloaded=?, rows_committed=?,
                    error_message=?
                where run_id=?
                """,
                (
                    _now(),
                    status,
                    int(request_count),
                    int(success_count),
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
                    status, row_count, retry_count, started_at, finished_at, error_message,
                    scope_id, request_kind
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        self, transitions: Iterable[dict[str, Any]], *, run_id: str
    ) -> None:
        """Commit scope outcomes in one metadata transaction."""

        rows = list(transitions)
        if not rows:
            return
        now = _now()
        with self.connect() as db:
            for row in rows:
                status = str(row["status"])
                if status not in {"success", "empty", "failed", "invalid"}:
                    raise ValueError(f"Unsupported scope transition: {status}")
                db.execute(
                    """
                    update update_scopes set
                        status=?, checked_through=case
                            when ? is null then checked_through
                            when checked_through is null or checked_through < ? then ?
                            else checked_through
                        end,
                        data_max_time=coalesce(?,data_max_time), row_count=?,
                        last_success_at=case when ? in ('success','empty') then ? else last_success_at end,
                        last_revision_check_at=coalesce(?,last_revision_check_at),
                        recheck_after=?, last_error=?, active_run_id=null,
                        commit_run_id=case when ?='success' then ? else commit_run_id end,
                        updated_at=?
                    where id=? and active_run_id=?
                    """,
                    (
                        status,
                        row.get("checked_through"),
                        row.get("checked_through"),
                        row.get("checked_through"),
                        row.get("data_max_time"),
                        int(row.get("row_count", 0)),
                        status,
                        now,
                        row.get("last_revision_check_at"),
                        row.get("recheck_after"),
                        row.get("last_error"),
                        status,
                        run_id,
                        now,
                        int(row["scope_id"]),
                        run_id,
                    ),
                )

    def reset_update_scopes(self, scope_ids: Iterable[int]) -> int:
        ids = list(dict.fromkeys(int(scope_id) for scope_id in scope_ids))
        if not ids:
            return 0
        placeholders = ",".join("?" for _ in ids)
        with self.connect() as db:
            cursor = db.execute(
                f"update update_scopes set status='pending',checked_through=null,"
                f"last_error=null,active_run_id=null,recheck_after=null,updated_at=? "
                f"where id in ({placeholders}) and status in "
                "('failed','invalid','empty','success')",
                (_now(), *ids),
            )
            return int(cursor.rowcount)

    def acquire_update_leases(
        self, leases: Iterable[tuple[str, str, str]], *, ttl_seconds: int = 300
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
                insert into update_leases(source,dataset,run_id,heartbeat_at,lease_expires_at)
                values (?, ?, ?, ?, ?)
                on conflict(source,dataset) do update set
                    run_id=excluded.run_id,heartbeat_at=excluded.heartbeat_at,
                    lease_expires_at=excluded.lease_expires_at
                """,
                [
                    (source, dataset, run_id, now.isoformat(), expires)
                    for source, dataset, run_id in rows
                ],
            )

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
            return int(cursor.rowcount)

    def update_state_ready(self) -> bool:
        rows = self._rows(
            "select value from metadata_state where key='update_state_version'"
        )
        return bool(rows and rows[0]["value"] == self.UPDATE_STATE_VERSION)

    def mark_update_state_ready(self) -> None:
        with self.connect() as db:
            db.execute(
                "insert into metadata_state(key,value,updated_at) values ('update_state_version',?,?)"
                " on conflict(key) do update set value=excluded.value,updated_at=excluded.updated_at",
                (self.UPDATE_STATE_VERSION, _now()),
            )

    def bootstrap_daily_success(self, scope_ids: Iterable[int]) -> int:
        ids = list(dict.fromkeys(int(scope_id) for scope_id in scope_ids))
        if not ids:
            return 0
        placeholders = ",".join("?" for _ in ids)
        now = _now()
        with self.connect() as db:
            cursor = db.execute(
                f"update update_scopes set status='success',checked_through=scope_key,"
                f"data_max_time=scope_key,last_success_at=?,commit_run_id='bootstrap',"
                f"last_error=null,active_run_id=null,updated_at=? "
                f"where id in ({placeholders})",
                (now, now, *ids),
            )
            return int(cursor.rowcount)

    def bootstrap_asset_data_max(
        self, *, source: str, dataset: str, maxima: dict[str, str]
    ) -> None:
        """Retain observed asset maxima without asserting checked coverage."""

        if not maxima:
            return
        with self.connect() as db:
            db.executemany(
                """
                update update_scopes set status='pending',checked_through=null,
                    data_max_time=?,active_run_id=null,updated_at=?
                where source=? and dataset=? and scope_kind='asset' and scope_key=?
                """,
                [
                    (maximum, _now(), source, dataset, asset_id)
                    for asset_id, maximum in maxima.items()
                ],
            )

    def complete_update_state_bootstrap(self) -> None:
        """Record ledger v1 and remove legacy inferred-state tables atomically."""

        with self.connect() as db:
            db.execute("begin immediate")
            for table in ("pending_update_jobs", "update_coverage", "audit_watermarks"):
                db.execute(f"drop table if exists {table}")
            db.execute(
                """
                insert into metadata_state(key,value,updated_at)
                values ('update_state_version',?,?)
                on conflict(key) do update set
                    value=excluded.value,updated_at=excluded.updated_at
                """,
                (self.UPDATE_STATE_VERSION, _now()),
            )

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
            return [dict(row) for row in db.execute(sql, tuple(params)).fetchall()]

    def _initialize(self) -> None:
        with self.connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            existing_tables = {
                str(row["name"])
                for row in db.execute(
                    "select name from sqlite_master where type='table'"
                ).fetchall()
            }
            had_legacy_update_state = bool(
                existing_tables
                & {"pending_update_jobs", "update_coverage", "audit_watermarks"}
            )
            existing_columns = {
                row["name"]
                for row in db.execute("pragma table_info(datasets)").fetchall()
            }
            if "category" in existing_columns or "source_dataset" in existing_columns:
                raise ConfigurationError(
                    "This lake uses the pre-simplification dataset schema. Delete and recreate the lake root."
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
                    on update_scopes(source,dataset,status,scope_key);
                create table if not exists update_leases (
                    source text not null,
                    dataset text not null,
                    run_id text not null unique,
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
            _ensure_column(db, "sources", "options_json", "text")
            _ensure_column(db, "sources", "enabled", "integer not null default 1")
            _ensure_column(db, "api_calls", "scope_id", "integer")
            _ensure_column(db, "api_calls", "request_kind", "text")
            if not had_legacy_update_state:
                db.execute(
                    """
                    insert into metadata_state(key,value,updated_at)
                    values ('update_state_version',?,?)
                    on conflict(key) do nothing
                    """,
                    (self.UPDATE_STATE_VERSION, _now()),
                )


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _spec_payload(spec: DatasetSpec) -> dict[str, Any]:
    return {field: getattr(spec, field) for field in spec.__dataclass_fields__}


def _ensure_column(
    db: sqlite3.Connection, table: str, field: str, definition: str
) -> None:
    fields = {
        row["name"] for row in db.execute(f"pragma table_info({table})").fetchall()
    }
    if field not in fields:
        db.execute(f"alter table {table} add column {field} {definition}")


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
