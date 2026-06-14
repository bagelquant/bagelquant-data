# Tushare Lake Workflow

This package keeps one supported ingestion path: Tushare data into a local
Polars/parquet lake.

## Configure

Install the optional provider dependencies and set a token:

```bash
uv sync --extra tushare
$env:TUSHARE_TOKEN="your-token"
```

You can also save the token with the interactive manager:

```bash
uv run python manage_data_lake.py
```

Choose the token action, then register the Tushare tables you want to maintain.

## Update

Run the updater to refresh registered tables:

```bash
uv run python update_tushare_lake.py
```

The updater refreshes missing reference tables, scans the local API call log,
prints the pending calls, and resumes from successful or empty calls already
recorded in the lake.

Price tables are written by trading day. Fundamental tables are written by
announcement year. Public reads return normalized `time` and `asset_id` columns.

## Load

```python
from bagelquant_data.lake import LocalDataLake
from bagelquant_data.loader import Loader

lake = LocalDataLake(".bagelquant-data-lake")
panel = (
    Loader(lake=lake)
    .source("tushare")
    .load_panel_field(
        "tushare_daily_close",
        start_date="2024-01-01",
        end_date="2024-01-31",
        universe=["000001.SZ"],
        calendar=["2024-01-02"],
    )
)
```

`panel.data` is a long-form Polars DataFrame with `time`, `asset_id`, and
`value`.
