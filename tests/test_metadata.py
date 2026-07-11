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


def test_metadata_store_rejects_pre_simplification_schema(tmp_path) -> None:
    path = tmp_path / "metadata" / "lake.db"
    path.parent.mkdir()
    with sqlite3.connect(path) as db:
        db.execute("create table datasets (category text not null)")

    with pytest.raises(ConfigurationError, match="pre-simplification"):
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
        select request_key, asset_id, request_params, status, row_count, retry_count, error_message
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
            "row_count": 3,
            "retry_count": 0,
            "error_message": None,
        },
        {
            "request_key": "1",
            "asset_id": "000002.SZ",
            "request_params": '{"ts_code": "000002.SZ"}',
            "status": "failed",
            "row_count": 0,
            "retry_count": 2,
            "error_message": "limit",
        },
        {
            "request_key": "2",
            "asset_id": "000003.SZ",
            "request_params": '{"ts_code": "000003.SZ"}',
            "status": "success",
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


def test_pending_update_jobs_upsert_and_resolve(tmp_path) -> None:
    metadata = MetadataStore(tmp_path / "metadata" / "lake.db")
    values = {
        "job_key": "job-1",
        "source": "tushare",
        "dataset": "daily",
        "update_type": "by_daily",
        "request_params": {"date": "2025-01-02"},
        "asset_id": None,
        "error_message": "rate limit",
    }

    metadata.record_failed_update_job(**values)
    metadata.record_failed_update_job(**values)

    rows = metadata.pending_update_jobs(source="tushare", dataset="daily")
    assert rows[0]["request_params"] == {"date": "2025-01-02"}
    assert rows[0]["failure_count"] == 2

    metadata.resolve_update_job("job-1")
    assert metadata.pending_update_jobs() == []
