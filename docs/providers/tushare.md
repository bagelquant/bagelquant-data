# Tushare

Tushare is the first V1 provider adapter.

Install the optional dependency:

```bash
uv sync --extra tushare
```

Provide a token explicitly:

```python
from bagelquant_data.datasource import TushareDataSource

source = TushareDataSource(token="your-token")
```

Or through the environment:

```bash
export TUSHARE_TOKEN=your-token
```

Supported V1 datasets:

- `stock_basic`
- `trade_cal`
- `daily`
- `index_daily`
- `generic` with `options={"api_name": "..."}`

Tushare dates are normalized to `YYYYMMDD`. Tokens are never included in
`describe()` output.

When Tushare data is ingested into `LocalDataLake`, tables such as `daily` are
stored under the `tushare` source namespace and partitioned by `trade_date`
year/month/day.

## All Universe

The Tushare `All` universe is sourced from `stock_basic`. Refreshes read list
statuses `L`, `D`, and `P`, then de-duplicate by `ts_code`, so delisted and
paused stocks remain available and survivorship bias is avoided. Asset ids are
stored as `tushare_<ts_code>`, for example `tushare_000300.SH`.

## Update Strategy

- `stock_basic` refreshes the All universe from listed, delisted, and paused
  stocks.
- Updates can be scanned first from local lake state, producing a report of
  pending tables, effective start dates, and executable jobs.
- `daily` and `index_daily` are fetched day by day to avoid Tushare row limits.
- Existing price dates are skipped before provider calls and stored at day
  granularity, so appending a new trading day does not rewrite older days.
- Fundamental tables create one job per local `stock_basic.ts_code`, starting
  from that asset's latest local `f_ann_date`; boundary rows are de-duplicated
  locally.
- VIP fundamental tables such as `income_vip` are fetched by reporting season
  with `period`, stored by year/quarter, and skipped when the quarter already
  exists locally.
- Default update range is `2000-01-01` through today.
- Provider requests can run concurrently with the `workers` setting.
