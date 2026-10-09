"""SQLite operational metadata."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import zlib
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import get_ident, local
from typing import Any

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.exceptions import ConfigurationError
from bagelquant_data.storage.paths import validate_generation
from bagelquant_data.storage.snapshot import copy_database, file_signature


_READ_VIEWS: ContextVar[tuple[tuple[Path, int, sqlite3.Connection], ...]] = ContextVar("data_read_views", default=())


def _probe_file_signature(path: Path) -> tuple[int, int, int, int, int] | None:
    return file_signature(path)


def _probe_schema(path: Path, *, immutable: bool) -> tuple[set[str], dict[str, str]]:
    uri = path.as_uri() + "?mode=ro" + ("&immutable=1" if immutable else "")
    db = sqlite3.connect(uri, uri=True)
    try:
        tables = {row[0] for row in db.execute("select name from sqlite_master where type='table'")}
        state = (
            {str(row[0]): str(row[1]) for row in db.execute("select key,value from data_meta_state")}
            if "data_meta_state" in tables else {}
        )
        return tables, state
    finally:
        db.close()


class DataMetaStore:
    """SQLite metadata store using WAL mode."""

    _BUSY_TIMEOUT_MS = 30_000
    SCHEMA_VERSION = "7"

    def __init__(self, data_meta_path: str | Path, *, read_only: bool = False, runtime: bool = False) -> None:
        self.data_meta_path = Path(data_meta_path)
        self.read_only = read_only
        if read_only and not self.data_meta_path.is_file():
            raise FileNotFoundError("Read-only Data metadata must already exist")
        if read_only and self.data_meta_path.stat().st_size == 0:
            raise ConfigurationError(
                "Read-only Data metadata has no initialized schema"
            )
        self.check_compatibility(self.data_meta_path, runtime=runtime)
        self._thread_state = local()
        if not read_only:
            self.data_meta_path.parent.mkdir(parents=True, exist_ok=True)
            self._initialize()

    def ensure_writable(self) -> None:
        """Reject mutations before any file or metadata write starts."""
        if self.read_only:
            raise PermissionError("Data lake is read-only")

    def bind_lake(self, lake_path: Path) -> None:
        """Validate that this metadata authority belongs to the selected lake."""
        import os
        from uuid import uuid4

        rows = self._rows("select value from data_meta_state where key='lake_location'")
        if rows:
            recorded = (self.data_meta_path.resolve().parent / str(rows[0]["value"])).resolve()
            if recorded != lake_path.resolve():
                raise ConfigurationError(
                    "Data metadata and lake paths refer to different lakes"
                )
            return
        self.ensure_writable()
        location = Path(
            os.path.relpath(lake_path.resolve(), self.data_meta_path.resolve().parent)
        ).as_posix()
        with self.connect() as db:
            db.execute("begin immediate")
            current = db.execute(
                "select value from data_meta_state where key='lake_location'"
            ).fetchone()
            if current is not None:
                if (
                    self.data_meta_path.resolve().parent / str(current[0])
                ).resolve() != lake_path.resolve():
                    raise ConfigurationError(
                        "Data metadata and lake paths refer to different lakes"
                    )
            else:
                db.executemany(
                    "insert into data_meta_state(key,value,updated_at) values(?,?,?)",
                    [
                        ("lake_id", uuid4().hex, _now()),
                        ("lake_location", location, _now()),
                    ],
                )

    @classmethod
    def _inspect_schema(cls, data_meta_path: Path) -> tuple[set[str], dict[str, str]]:
        """Inspect committed schema without touching the original SQLite sidecars.

        A normal read-only SQLite open may create or update WAL/SHM files. An
        immutable open avoids those writes but ignores WAL. Read a stable private
        DB/WAL snapshot when WAL is present; only a WAL-free database can be read
        directly as immutable. Normal Data connections retain SQLite WAL locking.
        Active/hot rollback journals are refused without attempting recovery;
        invalidated zero-header PERSIST journals do not protect pending pages.
        """
        path = data_meta_path.resolve()
        wal_path = path.with_name(path.name + "-wal")
        journal_path = path.with_name(path.name + "-journal")
        probe_paths = (path, wal_path, journal_path)
        for _ in range(3):
            before = tuple(_probe_file_signature(candidate) for candidate in probe_paths)
            try:
                if before[2] is not None:
                    with journal_path.open("rb") as stream:
                        if any(stream.read(8)):
                            raise sqlite3.OperationalError("Data metadata has an active rollback journal")
                if before[1] is None or before[1][2] == 0:
                    tables, state = _probe_schema(path, immutable=True)
                else:
                    with TemporaryDirectory(prefix="bagelquant-data-schema-") as directory:
                        snapshot = Path(directory) / path.name
                        copy_database(path, snapshot)
                        tables, state = _probe_schema(snapshot, immutable=False)
            except (OSError, sqlite3.Error):
                if before != tuple(_probe_file_signature(candidate) for candidate in probe_paths):
                    continue
                raise
            if before == tuple(_probe_file_signature(candidate) for candidate in probe_paths):
                break
        else:
            raise ConfigurationError("Data metadata changed during schema inspection; retry when it is stable")
        return tables, state

    @classmethod
    def check_compatibility(cls, data_meta_path: Path, *, runtime: bool = False) -> None:
        """Reject an incompatible schema before any original storage writes."""
        if not data_meta_path.is_file() or data_meta_path.stat().st_size == 0:
            return
        tables, state = cls._runtime_schema(data_meta_path) if runtime else cls._inspect_schema(data_meta_path)
        if tables and (
            state.get("schema_version") != cls.SCHEMA_VERSION
            or "declaration_batch_receipts" not in tables
        ):
            raise ConfigurationError(
                "Incompatible data-lake metadata schema; back up and rebuild the lake explicitly. Automatic migration is disabled."
            )

    @classmethod
    def _runtime_schema(cls, path: Path) -> tuple[set[str], dict[str, str]]:
        """Use ordinary WAL coordination for initialized runtime reads, without copies."""
        journal = path.with_name(path.name + "-journal")
        if journal.is_file():
            with journal.open("rb") as stream:
                if any(stream.read(8)):
                    raise sqlite3.OperationalError("Data metadata has an active rollback journal")
        return _probe_schema(path.resolve(), immutable=False)

    @classmethod
    def inspect(cls, data_meta_path: Path, lake_path: Path, *, runtime: bool = False) -> dict[str, object]:
        """Return schema and lake binding readiness without opening the lake."""
        if not data_meta_path.exists() or (
            data_meta_path.is_file() and data_meta_path.stat().st_size == 0
        ):
            return {"status": "uninitialized", "reason": "metadata_missing", "schema_version": None}
        if not data_meta_path.is_file():
            return {"status": "incompatible", "reason": "metadata_not_file", "schema_version": None}
        try:
            tables, state = cls._runtime_schema(data_meta_path) if runtime else cls._inspect_schema(data_meta_path)
        except (OSError, sqlite3.Error, ConfigurationError):
            return {"status": "incompatible", "reason": "metadata_unreadable", "schema_version": None}
        version = state.get("schema_version")
        if version != cls.SCHEMA_VERSION or "declaration_batch_receipts" not in tables:
            return {"status": "incompatible", "reason": "metadata_schema_incompatible", "schema_version": version}
        location = state.get("lake_location")
        if (
            not state.get("lake_id") or location is None
            or not lake_path.is_dir()
            or (data_meta_path.resolve().parent / location).resolve() != lake_path.resolve()
        ):
            return {"status": "incompatible", "reason": "lake_binding_invalid", "schema_version": version}
        return {"status": "ready", "reason": None, "schema_version": version}

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        """Yield one transactional connection and close owned connections."""
        path = self.data_meta_path.resolve()
        for view_path, thread, connection in reversed(_READ_VIEWS.get()):
            if view_path == path and thread == get_ident():
                yield connection
                return
        active = getattr(self._thread_state, "writer_connection", None)
        if isinstance(active, sqlite3.Connection):
            if getattr(self._thread_state, "atomic_transaction", False):
                yield active
                return
            with active as connection:
                yield connection
            return
        connection = self._new_connection()
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    @contextmanager
    def read_view(self) -> Iterator[sqlite3.Connection]:
        """Share one operation-local SQLite snapshot across same-thread readers."""
        path = self.data_meta_path.resolve()
        for view_path, thread, connection in reversed(_READ_VIEWS.get()):
            if view_path == path and thread == get_ident():
                yield connection
                return
        connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout={self._BUSY_TIMEOUT_MS}")
        connection.execute("begin")
        token = _READ_VIEWS.set((*_READ_VIEWS.get(), (path, get_ident(), connection)))
        try:
            yield connection
        finally:
            _READ_VIEWS.reset(token)
            connection.close()

    @contextmanager
    def atomic_transaction(self) -> Iterator[sqlite3.Connection]:
        """Share one transaction across declaration helpers without nested commits."""
        self.ensure_writable()
        if getattr(self._thread_state, "writer_connection", None) is not None:
            raise ConfigurationError("An atomic declaration transaction cannot be nested")
        connection = self._new_connection()
        self._thread_state.writer_connection = connection
        self._thread_state.atomic_transaction = True
        try:
            with connection:
                connection.execute("begin immediate")
                yield connection
        finally:
            del self._thread_state.writer_connection
            del self._thread_state.atomic_transaction
            connection.close()

    def _new_connection(self) -> sqlite3.Connection:
        connection = (
            sqlite3.connect(self.data_meta_path.resolve().as_uri() + "?mode=ro", uri=True)
            if self.read_only
            else sqlite3.connect(self.data_meta_path)
        )
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout={self._BUSY_TIMEOUT_MS}")
        connection.execute("PRAGMA foreign_keys=ON")
        if not self.read_only:
            connection.execute("PRAGMA synchronous=FULL")
        return connection

    @contextmanager
    def writer_session(self) -> Iterator[sqlite3.Connection]:
        """Reuse one scheduler-thread connection while preserving transactions."""

        self.ensure_writable()
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
        self.ensure_writable()
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
                    active=1,
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
        self.ensure_writable()
        with self.connect() as db:
            db.execute(
                "update sources set active=0, enabled=0, updated_at=? where name=?",
                (_now(), name),
            )

    def set_source_enabled(self, name: str, enabled: bool) -> None:
        self.ensure_writable()
        with self.connect() as db:
            db.execute(
                "update sources set enabled = ?, updated_at = ? where name = ?",
                (int(enabled), _now(), name),
            )

    def list_sources(self) -> list[dict[str, Any]]:
        rows = self._rows("select * from sources where active=1 order by name")
        for row in rows:
            if row.get("options_json"):
                options = json.loads(str(row["options_json"]))
                row["options"] = _redact_options(options)
            row.pop("options_json", None)
        return rows

    def upsert_dataset(self, spec: DatasetSpec) -> None:
        self.ensure_writable()
        with self.connect() as db:
            self._write_dataset(db, spec)

    def _write_dataset(self, db: sqlite3.Connection, spec: DatasetSpec) -> None:
        now = _now()
        payload = json.dumps(_spec_payload(spec), sort_keys=True, default=str)
        spec_hash = hashlib.blake2b(payload.encode("utf-8"), digest_size=16).hexdigest()
        db.execute(
            """
            insert into datasets(
                name, source, enabled, spec_hash, spec_json, created_at, updated_at
            )
            values (?, ?, ?, ?, ?, ?, ?)
            on conflict(source, name) do update set
                spec_hash=excluded.spec_hash,
                spec_json=excluded.spec_json,
                active=1,
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
        self.ensure_writable()
        with self.connect() as db:
            db.execute(
                "update datasets set enabled = ?, updated_at = ? where source = ? and name = ?",
                (int(enabled), _now(), source, dataset),
            )

    def remove_dataset(self, source: str, dataset: str) -> None:
        """Archive a definition without deleting immutable historical evidence."""
        self.ensure_writable()
        with self.connect() as db:
            db.execute(
                "update datasets set active=0,enabled=0,updated_at=? where source=? and name=?",
                (_now(), source, dataset),
            )

    def list_datasets(self, source: str | None = None) -> list[dict[str, Any]]:
        if source is None:
            return self._rows(
                "select * from datasets where active=1 order by source, name"
            )
        return self._rows(
            "select * from datasets where source = ? and active=1 order by name",
            (source,),
        )

    def get_dataset(self, source: str, dataset: str) -> dict[str, Any] | None:
        rows = self._rows(
            "select * from datasets where source = ? and name = ? and active=1",
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
        include_inactive: bool = False,
    ) -> list[dict[str, Any]]:
        """Aggregate exact manifest-backed status in one SQLite query."""

        selected = None if datasets is None else tuple(dict.fromkeys(datasets))
        clauses: list[str] = [] if include_inactive else ["d.active=1"]
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

        self.ensure_writable()
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
        version_commit: dict[str, Any] | None = None,
        scope_transitions: list[dict[str, Any]] | None = None,
        run_id: str | None = None,
        committed_rows: int = 0,
    ) -> None:
        """Atomically publish files, schema, durable batch evidence and visibility."""
        self.ensure_writable()
        rows = list(manifests)
        now = _now()
        with self.connect() as db:
            db.execute("begin immediate")
            if version_commit is not None:
                self.assert_dataset_parent(
                    db,
                    source,
                    dataset,
                    version_commit["parent"],
                    version_commit["definition_hash"],
                    run_id,
                )
                seq = int(version_commit["seq"])
                for batch in version_commit["batches"]:
                    registered = db.execute(
                        "select content_hash from version_batches where commit_seq=? and partition_path=?",
                        (seq, batch["partition_path"]),
                    ).fetchone()
                    if registered is None or registered[0] != batch["content_hash"]:
                        raise RuntimeError(
                            "Prepared recovery batch is missing or inconsistent"
                        )
                cursor = db.execute(
                    "update version_commits set status='committed' where seq=? and source=? and dataset=? and status='prepared'",
                    (seq, source, dataset),
                )
                if cursor.rowcount != 1:
                    raise RuntimeError(
                        "Version commit is not prepared for this dataset"
                    )
                if version_commit.get("check"):
                    _insert_version_check(db, **version_commit["check"])
            if replace_manifests:
                db.execute(
                    "delete from partition_manifest where source=? and dataset=?",
                    (source, dataset),
                )
            self._write_manifests(db, rows, now)
            db.execute(
                "insert into dataset_schemas(source,dataset,schema_ipc,schema_hash,updated_at) values(?,?,?,?,?) "
                "on conflict(source,dataset) do update set schema_ipc=excluded.schema_ipc,"
                "schema_hash=excluded.schema_hash,updated_at=excluded.updated_at",
                (source, dataset, schema_ipc, schema_hash, now),
            )
            if scope_transitions:
                if run_id is None:
                    raise ValueError(
                        "Coverage publication requires an ingestion run ID"
                    )
                self._transition_scopes(
                    db, scope_transitions, run_id=run_id, committed_rows=committed_rows
                )

    def _write_manifests(
        self, db: sqlite3.Connection, rows: list[dict[str, Any]], now: str
    ) -> None:
        for row in rows:
            generation = str(row["generation_path"])
            validate_generation(str(row["partition_path"]), generation)
            values = (
                row["source"],
                row["dataset"],
                str(row["partition_path"]),
                generation,
                json.dumps(row["partition_values"], sort_keys=True, default=str),
                int(row["row_count"]),
                int(row["file_size_bytes"]),
                row.get("min_time"),
                row.get("max_time"),
                row["content_hash"],
                row["schema_hash"],
                now,
            )
            db.execute(
                "insert into partition_manifest(source,dataset,partition_path,generation_path,partition_values,"
                "row_count,file_size_bytes,min_time,max_time,content_hash,schema_hash,updated_at) "
                "values(?,?,?,?,?,?,?,?,?,?,?,?) on conflict(source,dataset,partition_path) do update set "
                "generation_path=excluded.generation_path,partition_values=excluded.partition_values,"
                "row_count=excluded.row_count,file_size_bytes=excluded.file_size_bytes,min_time=excluded.min_time,"
                "max_time=excluded.max_time,content_hash=excluded.content_hash,schema_hash=excluded.schema_hash,"
                "updated_at=excluded.updated_at",
                values,
            )
            db.execute(
                "insert or ignore into partition_generations(source,dataset,partition_path,generation_path,content_hash,schema_hash) values(?,?,?,?,?,?)",
                (
                    row["source"],
                    row["dataset"],
                    row["partition_path"],
                    generation,
                    row["content_hash"],
                    row["schema_hash"],
                ),
            )

    @staticmethod
    def assert_dataset_parent(
        db: sqlite3.Connection,
        source: str,
        dataset: str,
        parent: dict[str, str],
        definition_hash: str,
        run_id: str | None,
    ) -> None:
        current = {
            row[0]: row[1]
            for row in db.execute(
                "select partition_path,generation_path from partition_manifest where source=? and dataset=?",
                (source, dataset),
            )
        }
        definition = db.execute(
            "select spec_hash from datasets where source=? and name=? and active=1",
            (source, dataset),
        ).fetchone()
        if current != parent or definition is None or definition[0] != definition_hash:
            raise RuntimeError("Dataset changed while preparing the publication")
        lease = db.execute(
            "select run_id from update_leases where source=? and dataset=?",
            (source, dataset),
        ).fetchone()
        if lease is None or lease[0] != run_id:
            raise RuntimeError("Dataset writer no longer owns publication permission")

    @contextmanager
    def dataset_writer(self, source: str, dataset: str, run_id: str) -> Iterator[None]:
        """Exclude foreign publishers while preserving an enclosing update lease."""
        self.ensure_writable()
        with self.connect() as db:
            db.execute("begin immediate")
            now = datetime.now(UTC)
            existing = db.execute(
                "select run_id from update_leases where source=? and dataset=?",
                (source, dataset),
            ).fetchone()
            if existing is not None and existing[0] != run_id:
                raise RuntimeError(f"Dataset update already active: {source}/{dataset}")
            owns = existing is None
            if owns:
                db.execute(
                    "insert into update_leases(source,dataset,run_id,owner_id,heartbeat_at,lease_expires_at) values(?,?,?,?,?,?) on conflict(source,dataset) do update set run_id=excluded.run_id,owner_id=excluded.owner_id,heartbeat_at=excluded.heartbeat_at,lease_expires_at=excluded.lease_expires_at",
                    (
                        source,
                        dataset,
                        run_id,
                        run_id,
                        now.isoformat(),
                        (now + timedelta(seconds=300)).isoformat(),
                    ),
                )
        try:
            yield
        finally:
            if owns:
                self.release_update_leases([run_id])

    def upsert_manifest(self, **manifest: Any) -> None:
        self.upsert_manifests([manifest])

    def upsert_manifests(self, manifests: Iterable[dict[str, Any]]) -> None:
        self.ensure_writable()
        with self.connect() as db:
            self._write_manifests(db, list(manifests), _now())

    def replace_manifests(
        self, source: str, dataset: str, manifests: Iterable[dict[str, Any]]
    ) -> None:
        self.ensure_writable()
        with self.connect() as db:
            db.execute("begin immediate")
            db.execute(
                "delete from partition_manifest where source=? and dataset=?",
                (source, dataset),
            )
            self._write_manifests(db, list(manifests), _now())

    def dataset_snapshot(self, source: str, dataset: str) -> dict[str, Any]:
        """Pin schema, manifests and commit visibility in one SQLite read snapshot."""
        with self.connect() as db:
            if not db.in_transaction:
                db.execute("begin")
            manifests = [
                dict(row)
                for row in db.execute(
                    "select * from partition_manifest where source=? and dataset=? order by partition_path",
                    (source, dataset),
                )
            ]
            schema = db.execute(
                "select schema_ipc from dataset_schemas where source=? and dataset=?",
                (source, dataset),
            ).fetchone()
            commits = [
                dict(row)
                for row in db.execute(
                    "select c.*,coalesce((select sum(b.row_count) from version_batches b where b.commit_seq=c.seq),0) as row_count "
                    "from version_commits c where source=? and dataset=? and status='committed' order by seq",
                    (source, dataset),
                )
            ]
            batch_schemas = {
                int(row["commit_seq"]): bytes(row["schema_ipc"])
                for row in db.execute(
                    "select b.commit_seq,b.schema_ipc from version_batches b join version_commits c on c.seq=b.commit_seq "
                    "where c.source=? and c.dataset=? and c.status='committed' order by b.commit_seq,b.partition_path",
                    (source, dataset),
                )
            }
            checks = [
                dict(row)
                for row in db.execute(
                    "select * from version_checks where source=? and dataset=? order by id",
                    (source, dataset),
                )
            ]
            from bagelquant_data.storage.full_commit_checks import full_commit_checks
            seals = full_commit_checks(self, db, checks)
            excluded = ",".join(str(int(seal["binding"]["check"]["id"])) for seal in seals) or "-1"
            inline_count = db.execute(
                "select count(*) from version_checks c cross join version_check_records r on c.id=r.check_id "
                f"where c.source=? and c.dataset=? and c.id not in ({excluded})", (source, dataset)).fetchone()[0]
            if inline_count > db.getlimit(sqlite3.SQLITE_LIMIT_LENGTH) // 300:
                raise MemoryError("Partial/mixed witnesses exceed the bounded metadata snapshot size limit")
            record_checks = [
                dict(row)
                for row in db.execute(
                    "select r.* from version_checks c cross join version_check_records r on c.id=r.check_id "
                    f"where c.source=? and c.dataset=? and c.id not in ({excluded}) order by c.id,r.record_id",
                    (source, dataset),
                )
            ]
        return {
            "manifests": manifests,
            "schema_ipc": None if schema is None else bytes(schema[0]),
            "commits": commits,
            "batch_schemas": batch_schemas,
            "checks": checks,
            "record_checks": record_checks,
            "full_commit_checks": seals,
        }

    def known_generations(self, source: str, dataset: str) -> set[str]:
        return {
            str(row["generation_path"])
            for row in self._rows(
                "select generation_path from partition_generations where source=? and dataset=?",
                (source, dataset),
            )
        }

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
        self.ensure_writable()
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

        self.ensure_writable()
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
        metrics: dict[str, Any] | None = None,
    ) -> None:
        """Finalize a run even when the update scheduler raises."""

        self.ensure_writable()
        with self.connect() as db:
            db.execute(
                """
                update ingestion_runs set
                    finished_at=?, status=?, request_count=?, success_count=?,
                    empty_count=?, failure_count=?, rows_downloaded=?, rows_committed=?,
                    error_message=?, metrics_json=?
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
                    json.dumps(metrics or {}, sort_keys=True, allow_nan=False),
                    run_id,
                ),
            )

    def record_run_metrics(self, *, run_id: str, metrics: dict[str, Any]) -> None:
        """Checkpoint measured progress in the run's existing metadata authority."""
        self.ensure_writable()
        with self.connect() as db:
            db.execute("update ingestion_runs set metrics_json=? where run_id=? and status='running'",
                       (json.dumps(metrics, sort_keys=True, allow_nan=False), run_id))

    def record_rejected(
        self,
        *,
        run_id: str,
        source: str,
        dataset: str,
        reason: str,
        row_count: int,
    ) -> None:
        self.ensure_writable()
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
        self.ensure_writable()
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
                        sum(int(row.get("metrics", {}).get("request_attempts", 1)) for row in run_rows),
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
                error_message, scope_id, request_kind, metrics_json
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    json.dumps(row.get("metrics", {}), sort_keys=True, allow_nan=False),
                )
                for row in rows
            ],
        )

    def synchronize_update_scopes(self, scopes: Iterable[dict[str, Any]]) -> None:
        """Insert ledger identities and invalidate rows whose spec changed."""

        self.ensure_writable()
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

        self.ensure_writable()
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
        self.ensure_writable()
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
                    "and status in ('pending','failed','invalid','success','empty')",
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
        self.ensure_writable()
        rows = list(transitions)
        if not rows:
            return
        with self.connect() as db:
            self._transition_scopes(
                db, rows, run_id=run_id, committed_rows=committed_rows
            )

    def _transition_scopes(
        self,
        db: sqlite3.Connection,
        rows: list[dict[str, Any]],
        *,
        run_id: str,
        committed_rows: int = 0,
    ) -> None:
        """Publish coverage using the caller's existing metadata transaction."""
        now = _now()
        for row in rows:
            status = str(row["status"])
            if status not in {"success", "empty", "failed", "invalid"}:
                raise ValueError(f"Unsupported scope transition: {status}")
            cursor = db.execute(
                """
                update update_scopes set
                    status=?, checked_through=case
                        when ? in ('success','empty') then coalesce(?,data_max_time,checked_through)
                        else checked_through
                    end,
                    data_max_time=case when ? in ('success','empty') then coalesce(?,data_max_time)
                        else data_max_time end,
                    row_count=case when ? in ('success','empty') then ? else row_count end,
                    last_success_at=case when ? in ('success','empty') then ? else last_success_at end,
                    recheck_after=null, last_error=?, active_run_id=null,
                    commit_run_id=case when ? in ('success','empty') then ? else commit_run_id end,
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
            if status in {"success", "empty"} and row.get("provider_checked_through"):
                self._upsert_provider_scope_check(
                    db,
                    scope_id=int(row["scope_id"]),
                    checked_through=str(row["provider_checked_through"]),
                    recheck_after=row.get("provider_recheck_after"),
                    result="empty" if status == "empty" else "nonempty",
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

        self.ensure_writable()
        scope_results = (
            []
            if scope_id is None
            else [
                {
                    "scope_id": scope_id,
                    "checked_through": checked_through,
                    "recheck_after": recheck_after,
                }
            ]
        )
        self.record_empty_scope_results(
            calls=calls,
            scope_results=scope_results,
            run_id=run_id,
            empty_outcome_count=1,
        )

    def record_empty_scope_results(
        self,
        *,
        calls: Iterable[dict[str, Any]],
        scope_results: Iterable[dict[str, Any]],
        run_id: str,
        empty_outcome_count: int | None = None,
    ) -> None:
        """Atomically audit one physical call and persist its empty daily scopes."""

        self.ensure_writable()
        rows = list(calls)
        if not rows:
            raise ValueError("An empty result must include at least one API audit row")
        outcomes = list(scope_results)
        count = len(outcomes) if empty_outcome_count is None else empty_outcome_count
        if count <= 0:
            raise ValueError("An empty result must include at least one outcome")
        now = _now()
        with self.connect() as db:
            db.execute("begin immediate")
            for outcome in outcomes:
                scope_id = int(outcome["scope_id"])
                claimed = db.execute(
                    "select id from update_scopes where id=? and status='running' "
                    "and active_run_id=?",
                    (scope_id, run_id),
                ).fetchone()
                if claimed is None:
                    raise RuntimeError(
                        f"Scope {scope_id} is not claimed by ingestion run {run_id}"
                    )
                if outcome.get("checked_through") is None:
                    raise ValueError(
                        "An incremental empty result needs checked_through"
                    )
            self._insert_api_calls(db, rows, recorded_at=now)
            for outcome in outcomes:
                scope_id = int(outcome["scope_id"])
                checked_through = str(outcome["checked_through"])
                self._upsert_provider_scope_check(
                    db,
                    scope_id=scope_id,
                    checked_through=checked_through,
                    recheck_after=outcome.get("recheck_after"),
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
                if cursor.rowcount != 1:
                    raise RuntimeError(
                        f"Scope {scope_id} is not claimed by ingestion run {run_id}"
                    )
            cursor = db.execute(
                """
                update ingestion_runs set request_count=request_count+?,
                    empty_count=empty_count+?
                where run_id=? and status='running'
                """,
                (len(rows), count, run_id),
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
        self.ensure_writable()
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
        self.ensure_writable()
        rows = list(leases)
        if not rows:
            return
        now = datetime.now(UTC)
        expires = (now + timedelta(seconds=ttl_seconds)).isoformat()
        with self.connect() as db:
            db.execute("begin immediate")
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
                    (
                        source,
                        dataset,
                        run_id,
                        owner_id or run_id,
                        now.isoformat(),
                        expires,
                    )
                    for source, dataset, run_id in rows
                ],
            )

    def abandon_update_owner(self, owner_id: str, *, reason: str) -> dict[str, int]:
        """Release one workflow owner's unfinished writes after forced termination."""

        self.ensure_writable()
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
        self.ensure_writable()
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
        self.ensure_writable()
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
            "select *,lease_expires_at<=? as heartbeat_expired from update_leases order by source,dataset",
            (_now(),),
        )

    def dataset_spec_hash(self, source: str, dataset: str) -> str:
        rows = self._rows(
            "select spec_hash from datasets where source=? and name=?",
            (source, dataset),
        )
        if not rows:
            raise KeyError(f"Unknown dataset: {source}/{dataset}")
        return str(rows[0]["spec_hash"])

    def assert_definition(self, spec: DatasetSpec) -> None:
        row = self.get_dataset(spec.source, spec.name)
        expected = json.dumps(_spec_payload(spec), sort_keys=True, default=str)
        if row is None or row["spec_json"] != expected:
            raise RuntimeError(
                "Dataset definition changed before preparing the publication"
            )

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
            if "metrics_json" in row:
                row["metrics"] = json.loads(row.pop("metrics_json"))
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
                if "data_meta_state" in existing_tables:
                    row = db.execute(
                        "select value from data_meta_state where key='schema_version'"
                    ).fetchone()
                    schema_version = None if row is None else str(row["value"])
                if schema_version != self.SCHEMA_VERSION:
                    raise ConfigurationError(
                        "Incompatible data-lake metadata schema "
                        f"({schema_version or 'unversioned'}). Preserve the existing "
                        "lake and open a fresh path; automatic migration is "
                        "intentionally disabled."
                    )
            db.executescript(
                """
                create table if not exists version_commits (
                    seq integer primary key autoincrement,
                    source text not null, dataset text not null, run_id text not null,
                    ingested_at text not null, pit_date text not null,
                    mode text not null, status text not null, spec_hash text not null,
                    request_json text not null default '[]', input_receipt_id text,
                    baseline integer not null default 0
                );
                create index if not exists version_commits_dataset on version_commits(source,dataset,status,seq);
                create table if not exists version_batches (
                    commit_seq integer not null, partition_path text not null,
                    content_hash text not null, row_count integer not null,
                    schema_ipc blob not null, payload blob not null,
                    min_available text, max_available text,
                    min_observation text, max_observation text,
                    primary key(commit_seq,partition_path)
                );
                create table if not exists version_checks (
                    id integer primary key autoincrement,
                    source text not null, dataset text not null, run_id text not null,
                    checked_at text not null, pit_date text not null, baseline integer not null,
                    visible_commit integer,
                    request_json text not null, row_count integer not null, input_receipt_id text
                );
                create table if not exists version_check_records (
                    check_id integer not null, record_id text not null, payload_hash text not null,
                    version_commit integer not null, available_date text not null,
                    primary key(check_id,record_id)
                );
                create table if not exists dataset_initializations (
                    source text not null, dataset text not null, spec_hash text not null,
                    initial_start text not null, initial_end text not null,
                    status text not null, primary key(source,dataset)
                );
                create table if not exists sources (
                    name text primary key,
                    adapter text not null,
                    configured integer not null default 0,
                    enabled integer not null default 1,
                    active integer not null default 1,
                    options_json text,
                    created_at text not null,
                    updated_at text not null
                );
                create table if not exists datasets (
                    name text not null,
                    source text not null,
                    enabled integer not null default 1,
                    active integer not null default 1,
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
                    owner_id text,
                    metrics_json text not null default '{}'
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
                    request_kind text,
                    metrics_json text not null default '{}'
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
                create table if not exists data_meta_state (
                    key text primary key,
                    value text not null,
                    updated_at text not null
                );
                create table if not exists declaration_batch_receipts (
                    request_id text primary key,
                    plan_hash text not null,
                    receipt_json text not null,
                    created_at text not null
                );
                create table if not exists partition_manifest (
                    source text not null,
                    dataset text not null,
                    partition_path text not null,
                    generation_path text not null,
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
                create table if not exists partition_generations (
                    source text not null, dataset text not null, partition_path text not null,
                    generation_path text not null, content_hash text not null, schema_hash text not null,
                    primary key(source,dataset,generation_path)
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
                    insert or ignore into data_meta_state(key,value,updated_at) values
                        ('schema_version',?,?)
                    """,
                    (
                        self.SCHEMA_VERSION,
                        now,
                    ),
                )
            # Trailing metadata follows a potentially huge BLOB in the table.
            # Keep a derived covering index so metadata reads need no overflow
            # traversal. Index/statistics publication is atomic and idempotent.
            if db.execute("select 1 from sqlite_master where type='index' and name='version_batches_metadata'").fetchone() is None:
                if not db.in_transaction:
                    db.execute("begin immediate")
                # Another writable opener may have created it while we waited.
                if db.execute("select 1 from sqlite_master where type='index' and name='version_batches_metadata'").fetchone() is None:
                    db.execute(
                        "create index if not exists version_batches_metadata on version_batches("
                        "commit_seq,partition_path,content_hash,row_count,schema_ipc,"
                        "min_available,max_available,min_observation,max_observation,length(payload))"
                    )
                    db.execute("analyze version_batches")


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _insert_version_check(
    db: sqlite3.Connection,
    *,
    source: str,
    dataset: str,
    run_id: str,
    checked_at: str,
    pit_date: str,
    baseline: bool,
    request_json: str,
    row_count: int,
    records: Iterable[tuple[Any, ...]],
    input_receipt_id: str | None = None,
) -> None:
    """Record immutable unchanged-content witnesses in the publication transaction."""
    check_id = db.execute(
        "insert into version_checks(source,dataset,run_id,checked_at,pit_date,baseline,visible_commit,request_json,row_count,input_receipt_id) "
        "values(?,?,?,?,?,?,(select max(seq) from version_commits where source=? and dataset=? and status='committed'),?,?,?)",
        (
            source,
            dataset,
            run_id,
            checked_at,
            pit_date,
            int(baseline),
            source,
            dataset,
            request_json,
            row_count,
            input_receipt_id,
        ),
    ).lastrowid
    if check_id is None:
        raise RuntimeError("Failed to allocate an unchanged-content witness")
    db.executemany(
        "insert into version_check_records(check_id,record_id,payload_hash,version_commit,available_date) values(?,?,?,?,?)",
        (
            (
                check_id,
                record_id,
                payload_hash,
                int(version_commit),
                str(available_date),
            )
            for record_id, payload_hash, version_commit, available_date in records
        ),
    )


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
