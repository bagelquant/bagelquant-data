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

When Tushare data is ingested into `LocalDataLake`, provider columns are
normalized to the package conventions: `trade_date`, `cal_date`, and
`f_ann_date` become `time`, while `ts_code` becomes `asset_id`. Price tables
such as `daily` are stored under the `tushare` source namespace and partitioned
by `time` at year/month/day granularity. Fundamental tables such as `income`
use `f_ann_date` as `time` and are partitioned by announcement year.

## Reference Resources

The Tushare `All` universe is sourced from `stock_basic`. Refreshes read list
statuses `L`, `D`, and `P`, then de-duplicate by `ts_code`, so delisted and
paused stocks remain available and survivorship bias is avoided. Asset ids are
stored as `tushare_<ts_code>`, for example `tushare_000300.SH`.

Tushare table updates also use a local trading calendar from `trade_cal`.
The default calendar reads `cal_date` and uses `is_open == 1` for trading days.
Reference resources are refreshed separately from normal table updates, so an
incremental price refresh does not overwrite `stock_basic` or `trade_cal`.

Sources may define multiple universes. For example, an index workflow can add
an `index_basic` universe and bind index-oriented tables to `index_basic.ts_code`
instead of the default stock universe.

## Update Strategy

- `stock_basic` refreshes the All universe from listed, delisted, and paused
  stocks; `trade_cal` refreshes the source trading calendar.
- Universes and trading calendars are updated as reference resources, separate
  from normal table scans and executions.
- Updates can be scanned first from compact update-record system tables,
  producing a report of pending tables, effective start dates, and executable
  jobs.
- `daily` and `index_daily` are fetched day by day over open trading dates from
  the associated trading calendar to avoid Tushare row limits.
- Existing price dates are skipped through the Tushare API call log and stored
  at day granularity, so appending a new trading day does not rewrite older
  days.
- Fundamental tables create one job per code in the associated update universe table,
  starting from that asset's latest local or logged date. Rows are stored by
  `f_ann_date` year and de-duplicated locally.
- VIP fundamental tables such as `income_vip` are fetched by reporting season
  with `period` and follow the same announcement-year storage convention unless
  a table-specific strategy is configured.
- Default update range is `2000-01-01` through today.
- Provider requests can run concurrently with the `workers` setting.
