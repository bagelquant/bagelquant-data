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
year/month.

## All Universe

The Tushare `All` universe is sourced from `stock_basic`. Refreshes read list
statuses `L`, `D`, and `P`, then de-duplicate by `ts_code`, so delisted and
paused stocks remain available and survivorship bias is avoided. Asset ids are
stored as `tushare_<ts_code>`, for example `tushare_000300.SH`.

## Update Strategy

- `stock_basic` refreshes the All universe from listed, delisted, and paused
  stocks.
- `daily` and `index_daily` are fetched day by day to avoid Tushare row limits.
- Fundamental tables are fetched id by id using `ts_code`.
- VIP fundamental tables such as `income_vip` are fetched by reporting season
  with `period`, so they do not loop through every `ts_code`.
- Fundamental refreshes are incremental, starting after the latest local
  `f_ann_date` for each id or after the latest local season for VIP tables.
- Default update range is `2000-01-01` through today.
- Provider requests can run concurrently with the `workers` setting.
