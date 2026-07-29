from __future__ import annotations

from types import SimpleNamespace

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec
from bagelquant_data.core import ValidationError
from bagelquant_data.storage.atomic import atomic_write_parquet


class DailySource:
    name = "custom"

    def fetch(self, dataset: str, request: dict[str, object]) -> pl.DataFrame:
        value = str(request["date"])
        return pl.DataFrame(
            {"trade_date": [value.replace("-", "")], "ts_code": ["000001.SZ"]}
        )


class AssetSource:
    name = "custom"

    def fetch(self, dataset: str, request: dict[str, object]) -> pl.DataFrame:
        asset = str(request["id"])
        return pl.DataFrame(
            {
                "ann_date": ["20240630", "20250630"],
                "ts_code": [asset, asset],
                "value": [1.0, 2.0],
            }
        )


def _daily_lake(tmp_path, dates: list[str]) -> DataLake:
    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(DailySource())
    lake.ingest(
        DatasetSpec("trade_cal", "general"),
        pl.DataFrame({"time": dates, "is_open": [1] * len(dates)}),
    )
    lake.admin.datasets.register(
        DatasetSpec(
            "daily",
            "by_daily",
            calendar="trade_cal",
            field_mappings={"trade_date": "time", "ts_code": "asset_id"},
        )
    )
    return lake


def test_scheduler_bounds_in_flight_and_reports_timings(tmp_path) -> None:
    lake = _daily_lake(tmp_path, [f"202501{day:02d}" for day in range(1, 11)])

    report = lake.update.dataset(
        "daily",
        source="custom",
        end="2025-01-10",
        workers=2,
        max_in_flight=3,
        progress=False,
    )

    assert report.peak_in_flight <= 3
    assert report.elapsed_seconds > 0
    assert report.fetch_seconds >= 0
    assert report.metadata_seconds >= 0


def test_one_batch_rewrites_one_shared_partition(tmp_path) -> None:
    lake = _daily_lake(tmp_path, [f"202501{day:02d}" for day in range(1, 5)])

    report = lake.update.dataset(
        "daily",
        source="custom",
        end="2025-01-04",
        batch_size=100,
        progress=False,
    )

    assert report.commit_count == 1
    assert report.partitions_rewritten == 1
    assert lake.query.query("daily", source="custom").collect().height == 4


def test_default_buffer_does_not_commit_every_hundred_requests(tmp_path) -> None:
    dates = (
        pl.date_range(
            pl.date(2025, 1, 1),
            pl.date(2025, 5, 30),
            interval="1d",
            eager=True,
        )
        .dt.strftime("%Y%m%d")
        .to_list()
    )
    lake = _daily_lake(tmp_path, dates)

    report = lake.update.dataset(
        "daily",
        source="custom",
        end="2025-05-30",
        progress=False,
    )

    assert report.request_count == 150
    assert report.commit_count == 1
    assert report.partitions_rewritten == 5


def test_by_asset_default_rewrites_each_touched_partition_once(tmp_path) -> None:
    assets = [f"{index:06d}.SZ" for index in range(150)]
    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(AssetSource())
    lake.ingest(
        DatasetSpec(
            "stock_basic",
            "general",
            field_mappings={"ts_code": "asset_id"},
        ),
        pl.DataFrame(
            {
                "ts_code": assets,
                "list_date": ["20240101"] * len(assets),
            }
        ),
    )
    lake.admin.datasets.register(
        DatasetSpec(
            "income",
            "by_asset",
            asset_list="stock_basic",
            field_mappings={"ann_date": "time", "ts_code": "asset_id"},
        )
    )

    report = lake.update.dataset(
        "income",
        source="custom",
        start="2024-01-01",
        end="2025-12-31",
        workers=8,
        progress=False,
    )

    files = lake.admin.status.files("income", source="custom")
    assert report.request_count == 150
    assert report.commit_count == 1
    assert report.partitions_rewritten == len(files)


def test_atomic_validation_does_not_replace_existing_file(
    tmp_path, monkeypatch
) -> None:
    path = tmp_path / "data.parquet"
    original = pl.DataFrame({"value": [1]})
    atomic_write_parquet(original, path)
    monkeypatch.setattr(
        "bagelquant_data.storage.atomic.pq.ParquetFile",
        lambda _: SimpleNamespace(
            metadata=SimpleNamespace(num_rows=999, num_columns=1),
            schema_arrow=original.to_arrow().schema,
            close=lambda: None,
        ),
    )

    with pytest.raises(ValidationError):
        atomic_write_parquet(pl.DataFrame({"value": [2]}), path)

    assert pl.read_parquet(path).equals(original)
