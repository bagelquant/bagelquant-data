# Script Instructions

This repository has two root-level interactive scripts. Each script is started
with one command, then asks for the inputs it needs.

The scripts separate each information block with:

```text
--------------------
```

## Update Tushare Data

Before running updates, register the Tushare tables with `manage_data_lake.py`.
You can also set the Tushare token there. The updater always updates every
enabled table in that local registry.

Run:

```bash
uv run python update_tushare_lake.py
```

The script will ask for:

- lake path, default `.bagelquant-data-lake`
- start date, default `2000-01-01`
- end date, default today
- whether to preview only

The script reads the saved Tushare token from `.bagelquant-data-local.json`, or
falls back to `TUSHARE_TOKEN`. It reads the registered Tushare update tables,
prints the update plan before downloading anything, and stops if no tables are
registered. If preview mode is off, it asks for confirmation before refreshing
missing references and running Tushare updates. `stock_basic` and `trade_cal`
are not refreshed when they already exist locally. During the update it prints
progress for each API call.

## Manage the Local Lake

Run:

```bash
uv run python manage_data_lake.py
```

The script will ask for the lake path, then show an action menu:

```text
1. Add Tushare table to update list
2. List Tushare update tables
3. Remove Tushare table from update list
4. List sources
5. List tables
6. List snapshots
7. List fields
8. List assets
9. Inspect Tushare API call log
10. Delete source, table, or snapshot
11. Set Tushare token
12. Show local config status
13. Refresh stock_basic and trade_cal
q. Quit
```

Choose an action by entering its number. The script will then ask for any
additional inputs needed for that action, such as source, table, snapshot,
status filter, or row limit.

To update `daily`, `adj_factor`, `balancesheet`, `income`, and `cashflow`, use
action 1 five times and enter each table name. When asked for table type, choose
`1` to infer automatically. The package will infer `price` for `daily` and
`adj_factor`, and `fundamental` for the financial statement tables.

Use action 11 to save the Tushare token locally. The token is written to
`.bagelquant-data-local.json`, which is ignored by git.

When adding a Tushare table, the manager checks whether `stock_basic` and
`trade_cal` exist locally. If either is missing, it automatically refreshes both
reference tables, using the saved token. `stock_basic` is refreshed for all
Tushare list statuses: listed (`L`), delisted (`D`), and paused (`P`).

Use action 13 any time you want to replace-refresh local `stock_basic` and
`trade_cal` manually.

## Update Logic

`update_tushare_lake.py` writes every Tushare API call to
`tushare/__api_call_log`. Future scans use successful and empty log rows to find
the latest available local date, then update from that point. Failed rows are
kept for audit but do not advance the update cursor.
