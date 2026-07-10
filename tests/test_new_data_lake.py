from __future__ import annotations

from dataclasses import fields

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec
from bagelquant_data.core import DatasetSpecError, ValidationError, incremental_key


def test_dataset_spec_is_a_plain_minimal_dataclass() -> None:
    spec = DatasetSpec("balancesheet", "by_asset", asset_list="stock_basic", primary_key_extra=("period",))

    assert [field.name for field in fields(DatasetSpec)] == ["name", "update_type", "source", "calendar", "asset_list", "primary_key_extra"]
    assert incremental_key(spec) == ("time", "asset_id", "period")
    assert not hasattr(spec, "primary_key")
    assert not hasattr(DatasetSpec, "from_mapping")


def test_manager_validates_references_and_toml(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    with pytest.raises(DatasetSpecError, match="calendar"):
        lake.admin.datasets.register(DatasetSpec("daily", "by_daily"))
    with pytest.raises(DatasetSpecError, match="asset_list"):
        lake.admin.datasets.register(DatasetSpec("income", "by_asset"))

    path = tmp_path / "daily.toml"
    path.write_text('name = "daily"\nupdate_type = "by_daily"\ncalendar = "trade_cal"\n')
    assert lake.admin.datasets.register_toml(path).calendar == "trade_cal"


def test_general_and_incremental_ingestion(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    lake.ingest(DatasetSpec("stock_basic", "general"), pl.DataFrame({"code": ["A", "A", "B"]}))
    assert lake.query.query_general("stock_basic", source="custom", fields=["code"]).collect()["code"].to_list() == ["A", "B"]

    with pytest.raises(ValidationError, match="asset_id"):
        lake.ingest(DatasetSpec("daily", "by_daily", calendar="trade_cal"), pl.DataFrame({"time": ["2025-01-01"]}))
