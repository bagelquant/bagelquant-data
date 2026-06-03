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

The GUI stores non-secret state in `.bagelquant-data-gui.yaml` by default:

- lake root
- configured sources
- configured table update targets
- user universes
- periodic update jobs

Tushare credentials are resolved only from `TUSHARE_TOKEN` or Streamlit secrets.
Tokens are never written to YAML.

## Workflows

Use **Lake Setup** to inspect sources, tables, snapshots, asset ids, data item
ids, and user universes.

Use **Data Sources** to configure Tushare tables such as `daily`, `index_daily`,
`income`, and `income_vip`.

Use **Retrieve Data** to preview lake data and generate copyable Python code for
direct lake reads, lake-first loader reads, panel agreements, and optional
downstream `bagelquant-core` conversion.

Use **Update Data Lake** to update one table immediately or run configured
periodic jobs that are due. In V1, periodic jobs are manual-trigger only.
