from __future__ import annotations

import polars as pl

from bagelquant_data.lake import DataLakeManager, LocalDataLake
from bagelquant_data.loader import Loader

lake = LocalDataLake(".bagelquant-data-lake")
manager = DataLakeManager(lake)

custom = pl.DataFrame(
    {
        "time": ["2024-01-02", "2024-01-03"],
        "asset_id": ["demo", "demo"],
        "close": [1.0, 1.1],
    }
)
manager.add("custom", "daily", custom)

loaded = (
    Loader(lake=lake, registry=manager.registry)
    .source("custom")
    .load_panel_field(
        "custom_daily_close",
        start_date="2024-01-01",
        end_date="2024-01-31",
        universe=["demo"],
        calendar=["2024-01-02", "2024-01-03"],
    )
)

print(loaded.data.to_dicts())
print(manager.list_tables("custom"))
manager.delete("custom", "daily")
