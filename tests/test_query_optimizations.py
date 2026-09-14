from __future__ import annotations

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec
from bagelquant_data.core import DatasetNotFoundError


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
        "daily", source="custom", observation_start="2025-02-01", observation_end="2025-02-28"
    ).collect()

    assert frame["close"].to_list() == [2.0]
    assert sum(len(paths) for paths in calls) == 1




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

    with pytest.raises(DatasetNotFoundError, match="references missing partition"):
        lake.query.query("daily", source="custom").collect()
