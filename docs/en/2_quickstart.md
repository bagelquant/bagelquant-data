# Quickstart

```python
import polars as pl
from bagelquant_data import DataLake, DatasetSpec

lake = DataLake.open("data")
daily = DatasetSpec(
    "daily",
    "by_daily",
    calendar="trade_cal",
    field_mappings={"trade_date": "time", "ts_code": "asset_id"},
)
lake.ingest(
    daily,
    pl.DataFrame({"trade_date": ["20250102"], "ts_code": ["000001.SZ"], "close": [11.25]}),
)

close = lake.query.query("daily", source="custom", fields=["time", "asset_id", "close"])
print(close.collect())
```

Use `uv sync` to install the project and `uv run pytest` to run its tests.
