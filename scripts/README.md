# BagelQuant Data Scripts

Install Tushare support first:

```bash
uv sync --extra tushare
```

## First-Time Setup

Save the Tushare token into the local lake metadata DB and register bundled dataset specs:

```bash
uv run python scripts/manage_lake.py --root data --token "$TUSHARE_TOKEN" --test-source
```

Later runs do not need the token again:

```bash
uv run python scripts/manage_lake.py --root data
```

The token is stored in `data/metadata/lake.db` for this local lake and is redacted from source listings.

## Update All Tushare Datasets

```bash
uv run python scripts/update_lake.py --root data
```

For each dataset, the script reads the local `maximum_time` from the lake manifest and updates inclusively from that date to `--end`. If a dataset has no local data, it uses `--start`; if `--start` is omitted, the fallback is `2000-01-01`. If `--end` is omitted, updates end at today.

By default the update order is:

```text
stock_basic, trade_cal, daily, daily_basic, adj_factor,
income, balancesheet, cashflow, forecast, express
```

`stock_basic` is updated before financial datasets so the updater can derive the asset universe for APIs that require `ts_code`.
`trade_cal` is updated before market datasets so `lake.update` can expand market date ranges into one API call per open trading day.

## Common Update Examples

Update selected market datasets for a date range:

```bash
uv run python scripts/update_lake.py --root data --datasets daily daily_basic --start 2024-01-01 --end 2024-12-31
```

Update financial data for selected assets:

```bash
uv run python scripts/update_lake.py --root data --datasets income balancesheet cashflow --assets 000001.SZ 600000.SH
```

Tune parallel API fetching and retry behavior:

```bash
uv run python scripts/update_lake.py --root data --workers 2 --max-retries 3 --retry-backoff-seconds 60
```

The default retry backoff is 60 seconds so Tushare's per-minute limit can reset before the next attempt. Shorten it only for sources or accounts where a shorter retry window is safe.

Disable progress bars for logs or CI:

```bash
uv run python scripts/update_lake.py --root data --no-progress
```

## Pagination

Datasets can declare offset pagination in YAML through `request_options`, for example:

```yaml
request_options:
  pagination: offset
  page_size: 5000
  limit_param: limit
  offset_param: offset
  offset_start: 0
```

Each page is retried independently and counted in progress/failure metadata. The updater stops when a page returns fewer rows than `page_size`.

## Query Examples

```bash
uv run python scripts/extract_data.py --root data --start 2020-01-01 --end 2026-06-15
uv run python scripts/financial_data.py --root data --asset 000001.SZ
```
