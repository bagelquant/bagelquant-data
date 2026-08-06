from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec
from bagelquant_data.core import DestructiveOperationError


def _daily_lake(tmp_path: Path) -> tuple[DataLake, Path]:
    lake = DataLake.open(tmp_path)
    lake.ingest(
        DatasetSpec(
            "daily",
            "by_daily",
            calendar="trade_cal",
            field_mappings={"trade_date": "time", "ts_code": "asset_id"},
        ),
        pl.DataFrame(
            {
                "trade_date": ["20250102", "20250102"],
                "ts_code": ["000001.SZ", "000002.SZ"],
                "close": [11.37, 8.42],
            }
        ),
    )
    path = (
        tmp_path
        / "lake"
        / "custom"
        / "daily"
        / "year=2025"
        / "month=01"
        / "data.parquet"
    )
    return lake, path


def test_validate_dataset_reports_key_schema_and_partition_errors(tmp_path) -> None:
    lake, path = _daily_lake(tmp_path)
    original = pl.read_parquet(path)
    corrupt = pl.concat(
        [
            original,
            original.head(1),
            original.head(1).with_columns(
                pl.lit(None, dtype=pl.String).alias("asset_id")
            ),
            original.head(1).with_columns(
                pl.date(2025, 2, 3).alias("time"),
                pl.col("close").cast(pl.String),
            ),
        ],
        how="diagonal_relaxed",
    )
    corrupt.write_parquet(path)

    report = lake.admin.validate_dataset("daily", source="custom", deep=True)

    codes = {issue["code"] for issue in report["issues"]}
    assert not report["valid"]
    assert {
        "manifest_mismatch",
        "canonical_schema_mismatch",
        "null_key",
        "duplicate_key",
        "partition_value_mismatch",
    } <= codes


def test_quarantine_is_recoverable_and_removes_manifest(tmp_path) -> None:
    lake, path = _daily_lake(tmp_path)
    relative = "year=2025/month=01/data.parquet"

    report = lake.admin.quarantine_partitions(
        "daily",
        source="custom",
        partition_paths=[relative],
        reason="test corruption",
        repair_id="repair-test",
        confirm=True,
    )

    assert not path.exists()
    assert report["quarantined"] == [relative]
    assert report["removed_manifests"] == [relative]
    assert Path(report["journal"]).is_file()
    assert Path(report["journal"]).parent == Path(report["quarantine_root"])
    assert (
        tmp_path
        / ".health-repair-quarantine"
        / "repair-test"
        / "custom"
        / "daily"
        / "year=2025"
        / "month=01"
        / "data.parquet"
    ).is_file()
    assert lake.metadata.manifest("custom", "daily") == []


def test_quarantine_rolls_back_file_move_when_metadata_fails(
    tmp_path, monkeypatch
) -> None:
    lake, path = _daily_lake(tmp_path)

    def fail(*_args, **_kwargs):
        raise RuntimeError("metadata unavailable")

    monkeypatch.setattr(lake.metadata, "remove_manifests", fail)

    with pytest.raises(RuntimeError, match="metadata unavailable"):
        lake.admin.quarantine_partitions(
            "daily",
            source="custom",
            partition_paths=["year=2025/month=01/data.parquet"],
            reason="test rollback",
            confirm=True,
        )

    assert path.is_file()
    assert len(lake.metadata.manifest("custom", "daily")) == 1


def test_quarantine_requires_confirmation_and_rejects_unsafe_paths(tmp_path) -> None:
    lake, _ = _daily_lake(tmp_path)

    with pytest.raises(DestructiveOperationError, match="confirm=True"):
        lake.admin.quarantine_partitions(
            "daily",
            source="custom",
            partition_paths=["year=2025/month=01/data.parquet"],
            reason="test confirmation",
        )
    with pytest.raises(DestructiveOperationError, match="Unsafe partition"):
        lake.admin.quarantine_partitions(
            "daily",
            source="custom",
            partition_paths=["../outside.parquet"],
            reason="test safety",
            confirm=True,
        )


def test_validate_dataset_reports_orphan_without_adopting_it(tmp_path) -> None:
    lake, path = _daily_lake(tmp_path)
    orphan = path.parent / "orphan.parquet"
    pl.read_parquet(path).write_parquet(orphan)

    report = lake.admin.validate_dataset("daily", source="custom", deep=True)

    assert any(issue["code"] == "orphan_file" for issue in report["issues"])
    assert all(
        row["partition_path"] != "year=2025/month=01/orphan.parquet"
        for row in lake.metadata.manifest("custom", "daily")
    )
