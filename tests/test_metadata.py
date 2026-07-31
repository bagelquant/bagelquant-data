from __future__ import annotations

import sqlite3

import pytest

from bagelquant_data.core import ConfigurationError
from bagelquant_data.storage.metadata import MetadataStore


def test_metadata_store_initializes_wal_mode(tmp_path) -> None:
    metadata = MetadataStore(tmp_path / "metadata" / "lake.db")

    with metadata.connect() as db:
        journal_mode = db.execute("PRAGMA journal_mode").fetchone()[0]

    assert journal_mode == "wal"


def test_metadata_store_rejects_incompatible_unversioned_schema(tmp_path) -> None:
    path = tmp_path / "metadata" / "lake.db"
    path.parent.mkdir()
    with sqlite3.connect(path) as db:
        db.execute("create table datasets (category text not null)")

    with pytest.raises(
        ConfigurationError, match="Incompatible data-lake metadata schema"
    ):
        MetadataStore(path)


def test_metadata_store_rejects_schema_v2_without_migration(tmp_path) -> None:
    path = tmp_path / "metadata" / "lake.db"
    path.parent.mkdir()
    with sqlite3.connect(path) as db:
        db.execute("create table metadata_state (key text primary key, value text)")
        db.execute(
            "insert into metadata_state(key,value) values ('schema_version','2')"
        )

    with pytest.raises(
        ConfigurationError, match="Incompatible data-lake metadata schema"
    ):
        MetadataStore(path)


def test_record_api_calls_inserts_batch_and_single_call_compatibility(tmp_path) -> None:
    metadata = MetadataStore(tmp_path / "metadata" / "lake.db")

    metadata.record_api_calls(
        [
            {
                "run_id": "run-1",
                "source": "tushare",
                "dataset": "income",
                "request_key": "0",
                "asset_id": "000001.SZ",
                "request_params": {"ts_code": "000001.SZ"},
                "status": "success",
                "row_count": 3,
                "retry_count": 0,
            },
            {
                "run_id": "run-1",
                "source": "tushare",
                "dataset": "income",
                "request_key": "1",
                "asset_id": "000002.SZ",
                "request_params": {"ts_code": "000002.SZ"},
                "status": "failed",
                "row_count": 0,
                "retry_count": 2,
                "error_message": "limit",
            },
        ]
    )
    metadata.record_api_call(
        run_id="run-1",
        source="tushare",
        dataset="income",
        request_key="2",
        asset_id="000003.SZ",
        request_params={"ts_code": "000003.SZ"},
        status="success",
        row_count=1,
    )

    rows = metadata._rows(
        """
        select request_key, asset_id, request_params, status, result_kind,
            row_count, retry_count, error_message
        from api_calls
        order by request_key
        """
    )

    assert rows == [
        {
            "request_key": "0",
            "asset_id": "000001.SZ",
            "request_params": '{"ts_code": "000001.SZ"}',
            "status": "success",
            "result_kind": "nonempty",
            "row_count": 3,
            "retry_count": 0,
            "error_message": None,
        },
        {
            "request_key": "1",
            "asset_id": "000002.SZ",
            "request_params": '{"ts_code": "000002.SZ"}',
            "status": "failed",
            "result_kind": "transport_failure",
            "row_count": 0,
            "retry_count": 2,
            "error_message": "limit",
        },
        {
            "request_key": "2",
            "asset_id": "000003.SZ",
            "request_params": '{"ts_code": "000003.SZ"}',
            "status": "success",
            "result_kind": "nonempty",
            "row_count": 1,
            "retry_count": 0,
            "error_message": None,
        },
    ]


def test_upsert_manifests_inserts_and_updates_batch(tmp_path) -> None:
    metadata = MetadataStore(tmp_path / "metadata" / "lake.db")

    metadata.upsert_manifests(
        [
            {
                "source": "tushare",
                "dataset": "income",
                "partition_path": "year=2024/bucket=0/data.parquet",
                "partition_values": {"year": 2024, "bucket": 0},
                "row_count": 10,
                "file_size_bytes": 100,
                "min_time": "2024-01-01",
                "max_time": "2024-03-31",
                "content_hash": "hash-1",
                "schema_hash": "schema-1",
            },
            {
                "source": "tushare",
                "dataset": "income",
                "partition_path": "year=2024/bucket=1/data.parquet",
                "partition_values": {"year": 2024, "bucket": 1},
                "row_count": 20,
                "file_size_bytes": 200,
                "min_time": "2024-01-01",
                "max_time": "2024-03-31",
                "content_hash": "hash-2",
                "schema_hash": "schema-1",
            },
        ]
    )
    metadata.upsert_manifest(
        source="tushare",
        dataset="income",
        partition_path="year=2024/bucket=0/data.parquet",
        partition_values={"year": 2024, "bucket": 0},
        row_count=11,
        file_size_bytes=110,
        min_time="2024-01-01",
        max_time="2024-06-30",
        content_hash="hash-1b",
        schema_hash="schema-1",
    )

    rows = metadata.manifest("tushare", "income")

    assert [
        (row["partition_path"], row["row_count"], row["content_hash"]) for row in rows
    ] == [
        ("year=2024/bucket=0/data.parquet", 11, "hash-1b"),
        ("year=2024/bucket=1/data.parquet", 20, "hash-2"),
    ]


def test_update_scopes_claim_transition_and_reset(tmp_path) -> None:
    metadata = MetadataStore(tmp_path / "metadata" / "lake.db")
    metadata.synchronize_update_scopes(
        [
            {
                "source": "tushare",
                "dataset": "daily",
                "scope_kind": "date",
                "scope_key": "2025-01-02",
                "variant_hash": "variant",
                "initial_start": "2025-01-02",
                "spec_hash": "spec",
            }
        ]
    )
    row = metadata.update_scopes()[0]
    assert metadata.claim_update_scopes([row["id"]], run_id="run") == [row["id"]]
    metadata.transition_update_scopes(
        [{"scope_id": row["id"], "status": "failed", "last_error": "rate limit"}],
        run_id="run",
    )
    failed = metadata.update_scopes(status="failed")[0]
    assert failed["attempt_count"] == 1
    assert failed["last_error"] == "rate limit"
    assert metadata.reset_update_scopes([row["id"]]) == 1
    assert metadata.update_scopes()[0]["status"] == "pending"


def test_scope_resynchronization_does_not_touch_unchanged_rows(tmp_path) -> None:
    metadata = MetadataStore(tmp_path / "metadata" / "lake.db")
    scope = {
        "source": "tushare",
        "dataset": "daily",
        "scope_kind": "date",
        "scope_key": "2025-01-02",
        "variant_hash": "variant",
        "initial_start": "2025-01-02",
        "spec_hash": "spec",
    }
    metadata.synchronize_update_scopes([scope])
    before = metadata.update_scopes()[0]

    metadata.synchronize_update_scopes([scope])
    after = metadata.update_scopes()[0]

    assert after["updated_at"] == before["updated_at"]


def test_writer_session_reuses_one_physical_connection(
    tmp_path, monkeypatch
) -> None:
    metadata = MetadataStore(tmp_path / "metadata" / "lake.db")
    opened = 0
    original = metadata._new_connection

    def counted_connect() -> sqlite3.Connection:
        nonlocal opened
        opened += 1
        return original()

    monkeypatch.setattr(metadata, "_new_connection", counted_connect)
    with metadata.writer_session():
        metadata.synchronize_update_scopes(
            [
                {
                    "source": "tushare",
                    "dataset": "daily",
                    "scope_kind": "date",
                    "scope_key": "2025-01-02",
                    "variant_hash": "variant",
                    "initial_start": "2025-01-02",
                    "spec_hash": "spec",
                }
            ]
        )
        assert metadata.update_scopes()[0]["status"] == "pending"

    assert opened == 1


def test_dataset_leases_are_atomic_and_stale_running_scopes_recover(tmp_path) -> None:
    metadata = MetadataStore(tmp_path / "metadata" / "lake.db")
    metadata.synchronize_update_scopes(
        [
            {
                "source": "tushare",
                "dataset": "daily",
                "scope_kind": "date",
                "scope_key": "2025-01-02",
                "variant_hash": "variant",
                "initial_start": "2025-01-02",
                "spec_hash": "spec",
            }
        ]
    )
    scope_id = int(metadata.update_scopes()[0]["id"])
    metadata.acquire_update_leases([("tushare", "daily", "run-1")])
    with pytest.raises(RuntimeError, match="already active"):
        metadata.acquire_update_leases([("tushare", "daily", "run-2")])
    metadata.claim_update_scopes([scope_id], run_id="run-1")
    with metadata.connect() as db:
        db.execute(
            "update update_leases set lease_expires_at='2000-01-01T00:00:00+00:00'"
        )

    assert metadata.recover_stale_running_scopes() == 1
    row = metadata.update_scopes()[0]
    assert row["status"] == "failed"
    assert row["last_error"] == "writer lease expired"


def test_empty_result_transaction_rolls_back_as_one_unit(tmp_path, monkeypatch) -> None:
    metadata = MetadataStore(tmp_path / "metadata" / "lake.db")
    metadata.synchronize_update_scopes(
        [
            {
                "source": "tushare",
                "dataset": "income",
                "scope_kind": "asset",
                "scope_key": "000001.SZ",
                "variant_hash": "variant",
                "initial_start": "2025-01-01",
                "spec_hash": "spec",
            }
        ]
    )
    scope_id = int(metadata.update_scopes()[0]["id"])
    metadata.begin_run(
        run_id="run-empty",
        source="tushare",
        dataset="income",
        mode="by_asset",
    )
    metadata.claim_update_scopes([scope_id], run_id="run-empty")

    def fail_provider_check(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        raise sqlite3.OperationalError("fault injection")

    monkeypatch.setattr(metadata, "_upsert_provider_scope_check", fail_provider_check)
    with pytest.raises(sqlite3.OperationalError, match="fault injection"):
        metadata.record_empty_scope_result(
            calls=[
                {
                    "run_id": "run-empty",
                    "source": "tushare",
                    "dataset": "income",
                    "request_key": "0",
                    "request_params": {"ts_code": "000001.SZ"},
                    "status": "success",
                    "result_kind": "empty",
                    "row_count": 0,
                }
            ],
            scope_id=scope_id,
            run_id="run-empty",
            checked_through="2025-01-31",
            recheck_after="2025-02-28",
        )

    assert metadata._rows("select * from api_calls") == []
    assert metadata.provider_scope_checks() == []
    assert metadata.update_scopes()[0]["status"] == "running"
    run = metadata._rows("select * from ingestion_runs where run_id='run-empty'")[0]
    assert run["empty_count"] == 0
    assert run["request_count"] == 0


def test_forced_owner_cleanup_preserves_empty_and_retries_only_inflight(
    tmp_path,
) -> None:
    metadata = MetadataStore(tmp_path / "metadata" / "lake.db")
    metadata.synchronize_update_scopes(
        {
            "source": "tushare",
            "dataset": "income",
            "scope_kind": "asset",
            "scope_key": asset,
            "variant_hash": "variant",
            "initial_start": "2025-01-01",
            "spec_hash": "spec",
        }
        for asset in ("A", "B")
    )
    scopes = metadata.update_scopes()
    owner_id = "workflow:42"
    metadata.begin_run(
        run_id="run-owner",
        source="tushare",
        dataset="income",
        mode="by_asset",
        owner_id=owner_id,
    )
    metadata.acquire_update_leases(
        [("tushare", "income", "run-owner")], owner_id=owner_id
    )
    metadata.claim_update_scopes([int(scopes[0]["id"])], run_id="run-owner")
    metadata.record_empty_scope_result(
        calls=[
            {
                "run_id": "run-owner",
                "source": "tushare",
                "dataset": "income",
                "request_key": "0",
                "request_params": {"ts_code": "A"},
                "status": "success",
                "result_kind": "empty",
                "row_count": 0,
            }
        ],
        scope_id=int(scopes[0]["id"]),
        run_id="run-owner",
        checked_through="2025-01-31",
        recheck_after="2025-02-28",
    )
    metadata.claim_update_scopes([int(scopes[1]["id"])], run_id="run-owner")

    cleaned = metadata.abandon_update_owner(owner_id, reason="forced termination")

    assert cleaned == {"runs": 1, "scopes": 1, "leases": 1}
    rows = metadata.update_scopes()
    assert [row["status"] for row in rows] == ["empty", "failed"]
    assert metadata.active_update_leases() == []
    run = metadata._rows("select * from ingestion_runs where run_id='run-owner'")[0]
    assert run["status"] == "cancelled"
    assert run["empty_count"] == 1
    assert metadata.abandon_update_owner(owner_id, reason="again") == {
        "runs": 0,
        "scopes": 0,
        "leases": 0,
    }
