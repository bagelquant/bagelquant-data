from __future__ import annotations

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec
from bagelquant_data.core import DatasetNotFoundError
from bagelquant_data.core.hashing import stable_bucket


def _record_scan_paths(
    monkeypatch,
) -> list[tuple[str, ...]]:
    calls: list[tuple[str, ...]] = []
    original = pl.scan_parquet

    def recording_scan(
        source: str | list[str], *args: object, **kwargs: object
    ) -> pl.LazyFrame:
        paths = (source,) if isinstance(source, str) else tuple(source)
        calls.append(paths)
        return original(source, *args, **kwargs)

    monkeypatch.setattr(pl, "scan_parquet", recording_scan)
    return calls


def test_daily_query_prunes_to_intersecting_month(tmp_path, monkeypatch) -> None:
    lake = DataLake.open(tmp_path)
    spec = DatasetSpec(
        "daily",
        "by_daily",
        calendar="trade_cal",
        field_mappings={"trade_date": "time", "ts_code": "asset_id"},
    )
    lake.ingest(
        spec,
        pl.DataFrame(
            {
                "trade_date": ["20250102", "20250203", "20250304"],
                "ts_code": ["A", "A", "A"],
                "close": [1.0, 2.0, 3.0],
            }
        ),
    )
    calls = _record_scan_paths(monkeypatch)

    frame = lake.query.query(
        "daily", source="custom", start="2025-02-01", end="2025-02-28"
    ).collect()

    assert frame["close"].to_list() == [2.0]
    assert sum(len(paths) for paths in calls) == 1


def test_asset_query_prunes_to_one_bucket_per_intersecting_year(
    tmp_path, monkeypatch
) -> None:
    target = "000001.SZ"
    other = next(
        f"{value:06d}.SZ"
        for value in range(2, 10_000)
        if stable_bucket(f"{value:06d}.SZ", 32) != stable_bucket(target, 32)
    )
    lake = DataLake.open(tmp_path)
    spec = DatasetSpec(
        "income",
        "by_asset",
        asset_list="stock_basic",
        field_mappings={"ann_date": "time", "ts_code": "asset_id"},
    )
    lake.ingest(
        spec,
        pl.DataFrame(
            {
                "ann_date": ["20240630", "20250630", "20240630", "20250630"],
                "ts_code": [target, target, other, other],
                "value": [1.0, 2.0, 3.0, 4.0],
            }
        ),
    )
    lake.admin.status.rebuild_manifest("income", source="custom")
    calls = _record_scan_paths(monkeypatch)

    frame = lake.query.query(
        "income",
        source="custom",
        start="2024-01-01",
        end="2025-12-31",
        assets=[target],
    ).collect()

    assert frame.height == 2
    assert sum(len(paths) for paths in calls) == 2
    assert all(
        f"bucket={stable_bucket(target, 32):02d}" in path
        for paths in calls
        for path in paths
    )


def test_out_of_range_query_returns_typed_empty_lazy_frame(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    spec = DatasetSpec(
        "daily",
        "by_daily",
        calendar="trade_cal",
        field_mappings={"trade_date": "time", "ts_code": "asset_id"},
    )
    lake.ingest(
        spec,
        pl.DataFrame({"trade_date": ["20250102"], "ts_code": ["A"], "close": [1.0]}),
    )

    frame = lake.query.query(
        "daily",
        source="custom",
        start="2030-01-01",
        end="2030-01-31",
        fields=["time", "close"],
    ).collect()

    assert frame.is_empty()
    assert frame.schema == pl.Schema({"time": pl.Date, "close": pl.Float64})


def test_query_fails_when_manifested_file_is_missing(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    spec = DatasetSpec(
        "daily",
        "by_daily",
        calendar="trade_cal",
        field_mappings={"trade_date": "time", "ts_code": "asset_id"},
    )
    lake.ingest(
        spec,
        pl.DataFrame({"trade_date": ["20250102"], "ts_code": ["A"], "close": [1.0]}),
    )
    manifest = lake.metadata.manifest("custom", "daily")[0]
    path = lake.paths.dataset_root("custom", "daily") / str(manifest["partition_path"])
    path.unlink()

    with pytest.raises(DatasetNotFoundError, match="references missing files"):
        lake.query.query("daily", source="custom").collect()
