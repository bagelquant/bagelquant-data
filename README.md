# BagelQuant Data

`bagelquant-data` is the Polars-native data access and local lake package for
the BagelQuant ecosystem.

Public datasets are `polars.DataFrame` objects. Panel-like data uses consistent
keys across the project:

- `time`
- `asset_id`

Provider-specific names such as `trade_date`, `cal_date`, `ts_code`, and
`symbol` are normalized at source/lake boundaries.

```python
import polars as pl

from bagelquant_data.lake import LocalDataLake

lake = LocalDataLake(".bagelquant-data-lake")
lake.write(
    "custom",
    "daily",
    pl.DataFrame(
        {
            "time": ["2024-01-02"],
            "asset_id": ["AAA"],
            "close": [100.0],
        }
    ),
    mode="overwrite",
)

close = lake.read_panel_field("custom_daily_close")
print(close)  # time, asset_id, value
```

Tushare support remains available. The Tushare client returns pandas objects, but
the adapter converts them to Polars before returning public data.

## Development

```bash
uv run ruff check .
uv run pytest
```
