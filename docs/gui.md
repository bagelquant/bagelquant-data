# Streamlit GUI

The Streamlit GUI is a supported V1 control surface for local data lake
operations. It is intentionally thin: the app calls `LocalDataLake`,
`DataLakeManager`, `Loader`, and provider adapters rather than owning data
logic itself.

Run it with:

```bash
uv sync --extra gui --extra tushare
uv run streamlit run src/bagelquant_data/gui/app.py
```

## Configuration

The GUI stores state in `.bagelquant-data-gui.yaml` by default:

- lake root
- configured sources
- configured universes and trading calendars
- configured table update targets
- shared update start date and worker count
- Tushare token, when configured in the GUI

Tushare credentials are resolved from the configured source token first, then
`TUSHARE_TOKEN`, then Streamlit secrets.

## Workflows

Use **Lake Setup** to inspect sources, tables, snapshots, and data item ids.
Data item ids can be filtered by source and table.

Use **Data Sources** to configure Tushare tables from the local Tushare catalog.
The catalog stores API names, Chinese descriptions, categories, and default
kinds from the official Tushare docs. Tushare sources automatically include a
`stock_basic` universe and a `trade_cal` trading calendar. Universes and
calendars are identified by their selected table names; add extra universes,
such as `index_basic`, when different tables should update against a different
code list.

Click **Update universes/calendars** to refresh reference resources. Click
**Scan updates** to scan normal enabled tables only and review which tables need
work, the effective start date, pending items, and job count. Click
**Confirm update** to execute exactly the reported jobs. Non-general tables must
select an enabled update universe before they can be scanned. If a source has one
enabled trading calendar, the GUI uses it automatically; if a source has
multiple enabled calendars, each non-general table must select one. The GUI
shows the current table, date/asset/period work item, completed work count, and
recent rows written. The update end date defaults to today and the default
worker count is 8.

Use **Retrieve Data** to pick a qualified panel field id such as
`tushare_daily_close`, preview a date-by-asset panel, and generate copyable
Python code for direct panel reads, loader panel agreements, and optional
downstream `bagelquant-core` conversion.
