# BagelQuant Data

`bagelquant-data` is the lean Polars-native data package for local BagelQuant
research workflows. Its current core is intentionally small:

- read provider data into `polars.DataFrame` objects
- normalize provider columns to `time` and `asset_id`
- write/read local parquet lake snapshots
- plan and execute resumable Tushare lake updates
- load long-form panel fields as `time`, `asset_id`, `value`

Provider-specific names such as `trade_date`, `cal_date`, `f_ann_date`, and
`ts_code` are normalized at provider and lake boundaries.

```python
import polars as pl

from bagelquant_data.lake import LocalDataLake

lake = LocalDataLake(".bagelquant-data-lake")
lake.write(
    "custom",
    "daily",
    pl.DataFrame(
        {
            "trade_date": ["2024-01-02"],
            "ts_code": ["000001.SZ"],
            "close": [100.0],
        }
    ),
    mode="overwrite",
)

close = lake.read_panel_field("custom_daily_close")
print(close)  # time, asset_id, value
```

See [docs/tushare-lake-workflow.md](docs/tushare-lake-workflow.md) for the
maintained ingestion workflow.

## Development

```bash
uv run ruff check .
uv run pytest
```
