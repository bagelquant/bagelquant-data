from __future__ import annotations

import polars as pl

from bagelquant_data.datasource.base import DataRequest
from bagelquant_data.datasource.registry import DataSourceRegistry
from bagelquant_data.loader import Loader


class Source:
    name = "demo"

    def read(self, request: DataRequest) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "time": ["2024-01-01"],
                "asset_id": ["a"],
                "close": [1.0],
            }
        )

    def exists(self, dataset: str) -> bool:
        return True

    def describe(self, dataset: str):
        return {"dataset": dataset}


def test_loader_returns_polars_dataset() -> None:
    registry = DataSourceRegistry()
    registry.register(Source())

    loaded = Loader(registry=registry).source("demo").load("daily")

    assert isinstance(loaded.data, pl.DataFrame)
    assert loaded.data.columns == ["time", "asset_id", "close"]


def test_loader_panel_returns_time_asset_id_value() -> None:
    registry = DataSourceRegistry()
    registry.register(Source())

    panel = (
        Loader(registry=registry)
        .source("demo")
        .load_panel(
            "daily",
            field="close",
            universe=["a"],
            start_date="2024-01-01",
            end_date="2024-01-01",
            calendar=["2024-01-01"],
        )
    )

    assert panel.data.columns == ["time", "asset_id", "value"]
    assert panel.data.to_dicts()[0]["asset_id"] == "a"
