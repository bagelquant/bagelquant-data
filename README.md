# BagelQuant Data

Unified data access for the BagelQuant ecosystem.

`bagelquant-data` ingests provider data into a local source-separated data lake,
standardizes access, tracks metadata, and produces reproducible contracts for
downstream systems. It is infrastructure, not a research library.

## Mission

- ingest data from multiple providers
- provide a unified access interface
- manage metadata and data contracts
- orchestrate loading and transformation
- integrate with data lake backends through interfaces
- serve standardized outputs to downstream systems

This package does not define Panel internals, factor research, portfolio
construction, graph execution, backtesting, or analytics.

## Install

```bash
uv sync --all-groups
```

Install Tushare support:

```bash
uv sync --extra tushare
```

Install the local data lake GUI:

```bash
uv sync --extra gui --extra tushare
uv run streamlit run src/bagelquant_data/gui/app.py
```

## Quick Start

```python
from bagelquant_data.datasource import DataRequest, DataSourceRegistry, TushareDataSource
from bagelquant_data.lake import DataLakeManager, LocalDataLake
from bagelquant_data.loader import Loader

registry = DataSourceRegistry()
registry.register(TushareDataSource(token="your-token"))

lake = LocalDataLake(".bagelquant-data-lake")
manager = DataLakeManager(lake, registry=registry)

manager.update(
    "tushare",
    DataRequest(
        dataset="daily",
        filters={"ts_code": "000001.SZ"},
        start_date="2024-01-01",
        end_date="2024-01-31",
    ),
)

daily = Loader(registry=registry, lake=lake).source("tushare").load(
    "daily",
)

daily.data.head()
```

When a lake is configured, `Loader` reads the local lake first. Use
`refresh=True` to fetch the provider and write a new local snapshot.

## Panel Agreements

`bagelquant-data` does not import `bagelquant-core`. When data should become a
core `Panel`, loaders return a neutral agreement:

```python
agreement = Loader(registry=registry, lake=lake).source("tushare").load_panel(
    dataset="daily",
    field="close",
    universe=["000001.SZ", "600000.SH"],
    start_date="2024-01-01",
    end_date="2024-12-31",
    region="CN",
)
```

Downstream code can convert it explicitly:

```python
from bagelquant_core import Domain, Panel

domain = Domain(**agreement.domain_spec.to_core_kwargs())
panel = Panel.from_domain(
    agreement.frame,
    domain,
    name=agreement.dataset_name,
    metadata=agreement.metadata,
)
```

## Tushare Tokens

Token resolution order:

1. `TushareDataSource(token=...)`
2. `TUSHARE_TOKEN`
3. `Settings(tushare_token=...)`

Tokens are not returned by `describe()` and are redacted from `repr()`.

## Lake Management

The local lake is separated by source:

```text
.bagelquant-data-lake/
  tushare/
    daily/
      _catalog.json
      year=2024/
        month=01/
          _catalog.json
          snapshots/
```

Every table is normalized with:

- index name `date` for panel-like data
- columns `create_time` and `delete_flag`
- source asset ids in `__asset_ids`
- source data item ids in `__data_item_ids`

Reference tables that are not panel-like, such as `stock_basic`, keep their
ordinary row index.

Manage datasets directly:

```python
manager.add("custom", "prices", frame)
manager.edit("custom", "prices", corrected_frame)
manager.delete("custom", "prices")
manager.list_sources()
manager.list_tables("tushare")
lake.read("tushare", "daily", year=2024, month=1)
```

Periodic updates are represented as jobs that an external scheduler, cron, app,
or service can call:

```python
job = manager.periodic_update(
    "tushare-daily",
    source_name="tushare",
    request=DataRequest(dataset="daily", filters={"ts_code": "000001.SZ"}),
    schedule=UpdateSchedule(every=1, unit="days"),
)
manager.run_due()
```

## Universes

Each source has an `All` universe. User-defined universes must be subsets of
`All`:

```python
manager.define_universe("tushare", "banks", ["000001.SZ", "600000.SH"])
manager.universe("tushare", "banks")
```

Provider updates always refresh `All`; universe subsets are for user retrieval
and filtering.

## Streamlit GUI

The V1 GUI manages the local lake from a Streamlit app:

```bash
uv run streamlit run src/bagelquant_data/gui/app.py
```

It stores non-secret settings in `.bagelquant-data-gui.yaml` by default:

- lake root
- configured sources and tables
- user-defined universes
- periodic update jobs

Tushare tokens are read only from `TUSHARE_TOKEN` or Streamlit secrets. The GUI
does not persist tokens. Periodic jobs are configured in YAML and run only when
the user clicks "Run due jobs" in V1.

## Tushare Updates

Tushare `All` is built from `stock_basic`, including listed and off-market
stocks returned by the provider. Price-like tables such as `daily` and
`index_daily` are fetched day by day to avoid provider row limits. Fundamental
tables are fetched id by id through `ts_code` and use the existing local table
to request only incremental changes after the latest `f_ann_date`. VIP
fundamental tables such as `income_vip` are fetched by reporting season with
`period`, so they do not loop through every stock.

Defaults:

- `start_date="2000-01-01"`
- `end_date=today`
- threaded provider reads through `workers`

## Development

```bash
uv sync --all-groups --extra tushare --extra gui
uv run ruff check .
uv run pyright
uv run pytest
uv run mkdocs build --strict
```

## License

Apache License 2.0
