from __future__ import annotations

from typing import cast

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec
from bagelquant_data.core import DuplicateResolutionError, stable_bucket


def daily_spec() -> DatasetSpec:
    return DatasetSpec(
        name="daily",
        source="custom",
        source_dataset="daily",
        category="market",
        field_mapping={"ts_code": "ts_code", "trade_date": "trade_date"},
        required_columns=("asset_id", "time"),
        primary_key=("asset_id", "time"),
        asset_column="ts_code",
        time_column="trade_date",
        partition_strategy="year_month",
        deduplication="primary_key_last",
        sort_columns=("time", "asset_id"),
    )


def test_ingest_and_extract_single_value_panel(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    lake.ingest_frame(
        daily_spec(),
        pl.DataFrame(
            {
                "trade_date": ["20250103", "20250102"],
                "ts_code": ["000001.SZ", "000002.SZ"],
                "close": [11.37, 18.40],
                "open": [11.20, 18.10],
            }
        ),
    )

    close = cast(pl.DataFrame, lake.query.field("daily", "close", source="custom", collect=True))

    assert close.columns == ["time", "asset_id", "close"]
    assert close["asset_id"].to_list() == ["000002.SZ", "000001.SZ"]
    assert lake.status.dataset("daily", source="custom")["row_count"] == 2


def test_duplicate_field_requires_resolution(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    spec = daily_spec()
    spec = DatasetSpec(
        **{
            **{field: getattr(spec, field) for field in spec.__dataclass_fields__},
            "deduplication": "none",
        }
    )
    lake.ingest_frame(
        spec,
        pl.DataFrame(
            {
                "trade_date": ["20250103", "20250103"],
                "ts_code": ["000001.SZ", "000001.SZ"],
                "close": [11.37, 11.38],
            }
        ),
    )

    with pytest.raises(DuplicateResolutionError):
        lake.query.field("daily", "close", source="custom", collect=True)


def test_stable_bucket_is_deterministic() -> None:
    assert stable_bucket("000001.SZ", 32) == stable_bucket("000001.SZ", 32)
