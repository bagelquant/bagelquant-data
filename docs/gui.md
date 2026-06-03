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
kinds from the official Tushare docs. Tushare sources automatically include
`stock_basic` as the required first table with kind `general`. Click
**Update data lake** to manually refresh all enabled tables. The update end date
defaults to today and the default worker count is 8.

Use **Retrieve Data** to preview lake data and generate copyable Python code for
direct lake reads, lake-first loader reads, panel agreements, and optional
downstream `bagelquant-core` conversion.
