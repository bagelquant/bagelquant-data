from __future__ import annotations

from pathlib import Path
from time import perf_counter

import polars as pl

from bagelquant_data import DataLake, DatasetSpec


def _daily_lake(tmp_path: Path) -> tuple[DataLake, Path]:
    lake = DataLake.open(
        data_meta_path=(tmp_path) / "data_meta.sqlite", lake_path=(tmp_path) / "lake"
    )
    lake.raw.ingest(
        DatasetSpec(
            "daily",
            "by_date",
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
    row = lake.raw.manifest("daily", source="custom")[0]
    path = lake._paths.dataset_root("custom", "daily") / row["generation_path"]
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

    report = lake.integrity.scan("daily", source="custom", deep=True)

    codes = {issue["code"] for issue in report["issues"]}
    assert not report["valid"]
    assert {
        "manifest_mismatch",
        "canonical_schema_mismatch",
        "null_key",
        "duplicate_key",
        "partition_value_mismatch",
    } <= codes


def test_validate_dataset_reports_orphan_without_adopting_it(tmp_path) -> None:
    lake, path = _daily_lake(tmp_path)
    orphan = path.parent / "orphan.parquet"
    pl.read_parquet(path).write_parquet(orphan)

    report = lake.integrity.scan("daily", source="custom", deep=True)

    assert any(issue["code"] == "orphan_file" for issue in report["issues"])
    assert all(
        row["partition_path"] != "year=2025/month=01/orphan.parquet"
        for row in lake._data_meta.manifest("custom", "daily")
    )


def test_bulk_dataset_status_uses_exact_manifest_facts_and_one_read(
    tmp_path, monkeypatch
) -> None:
    lake, _ = _daily_lake(tmp_path)
    lake.raw.register(DatasetSpec("empty", "general"))
    status_calls = 0
    real_statuses = lake._data_meta.dataset_statuses

    def counted_statuses(*, source=None, datasets=None):
        nonlocal status_calls
        status_calls += 1
        return real_statuses(source=source, datasets=datasets)

    monkeypatch.setattr(lake._data_meta, "dataset_statuses", counted_statuses)

    statuses = lake.raw.status_many(source="custom")

    assert status_calls == 1
    assert [status["dataset"] for status in statuses] == ["daily", "empty"]
    daily = statuses[0]
    assert daily["partition_count"] == 1
    assert daily["row_count"] == 2
    assert daily["minimum_time"] == "2025-01-02"
    assert daily["maximum_time"] == "2025-01-02"
    assert statuses[1]["row_count"] == 0
    assert statuses[1]["minimum_time"] is None
    assert statuses[1]["maximum_time"] is None


def test_bulk_shallow_validation_matches_individual_results(tmp_path) -> None:
    lake, path = _daily_lake(tmp_path)
    lake.raw.ingest(
        DatasetSpec("general", "general"),
        pl.DataFrame({"time": ["2025-01-02"], "value": [1.0]}),
    )
    path.unlink()
    general_root = tmp_path / "lake" / "raw" / "custom" / "general"
    orphan = general_root / "orphan.parquet"
    pl.DataFrame({"time": ["2025-01-03"], "value": [2.0]}).write_parquet(orphan)
    expected = {
        dataset: {
            (issue["code"], issue["path"])
            for issue in lake.integrity.scan(dataset, source="custom", deep=False)[
                "issues"
            ]
        }
        for dataset in ("daily", "general")
    }

    report = lake.integrity.scan_many(["daily", "general"], source="custom", deep=False)

    actual = {
        dataset_report["dataset"]: {
            (issue["code"], issue["path"]) for issue in dataset_report["issues"]
        }
        for dataset_report in report["datasets"]
    }
    assert not report["valid"]
    assert report["dataset_count"] == 2
    assert report["files_scanned"] == 0
    assert actual == expected


def test_bulk_catalog_and_shallow_validation_scale_to_thirty_thousand_partitions(
    tmp_path,
) -> None:
    lake = DataLake.open(
        data_meta_path=(tmp_path) / "data_meta.sqlite", lake_path=(tmp_path) / "lake"
    )
    names = [f"dataset_{index:03d}" for index in range(100)]
    for name in names:
        lake.raw.register(DatasetSpec(name, "general"))
    lake._data_meta.upsert_manifests(
        {
            "generation_path": f"year={2000 + partition // 12}/month={partition % 12 + 1:02d}/data-{partition:032x}.parquet",
            "source": "custom",
            "dataset": name,
            "partition_path": f"year={2000 + partition // 12}/month={partition % 12 + 1:02d}/data.parquet",
            "partition_values": {
                "year": 2000 + partition // 12,
                "month": partition % 12 + 1,
            },
            "row_count": 10,
            "file_size_bytes": 100,
            "min_time": "2025-01-01",
            "max_time": "2025-12-31",
            "content_hash": f"content-{name}-{partition}",
            "schema_hash": "schema",
        }
        for name in names
        for partition in range(300)
    )

    status_started = perf_counter()
    statuses = lake.raw.status_many(source="custom")
    status_elapsed = perf_counter() - status_started
    legacy_started = perf_counter()
    legacy = {
        name: lake.integrity.scan(name, source="custom", deep=False) for name in names
    }
    legacy_elapsed = perf_counter() - legacy_started
    batch_started = perf_counter()
    batch = lake.integrity.scan_many(names, source="custom", deep=False)
    batch_elapsed = perf_counter() - batch_started

    assert len(statuses) == 100
    assert sum(int(row["partition_count"]) for row in statuses) == 30_000
    assert sum(int(row["row_count"]) for row in statuses) == 300_000
    assert status_elapsed < 1.0
    assert batch["dataset_count"] == 100
    assert {row["dataset"]: row["issue_counts"] for row in batch["datasets"]} == {
        name: report["issue_counts"] for name, report in legacy.items()
    }
    assert batch_elapsed <= legacy_elapsed * 0.30
