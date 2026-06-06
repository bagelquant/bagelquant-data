# BagelQuant Data

Backend data access for the BagelQuant ecosystem.

`bagelquant-data` ingests provider data into a local source-separated data lake,
standardizes reads, tracks metadata, and returns reproducible data objects for
downstream systems. It is infrastructure, not a research library, and it does
not ship a GUI.

## Mission

- ingest data from multiple providers
- provide a unified backend API
- manage local data lake snapshots and catalogs
- orchestrate provider updates
- manage metadata and data contracts
- serve standardized pandas outputs to downstream systems

This package does not define Panel internals, factor research, portfolio
construction, graph execution, backtesting, or analytics.

## Install

Install the package and development dependencies:

```bash
uv sync --all-groups
```

Install Tushare support:

```bash
uv sync --extra tushare
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
    fields=("close",),
    start_date="2024-01-01",
    end_date="2024-01-31",
)

print(daily.data.head())
```

When a lake is configured, `Loader` reads local snapshots first. Use
`refresh=True` to fetch the provider and write a new local snapshot.

## Module Responsibilities

- `bagelquant_data.datasource`: provider adapters, `DataRequest`, and source
  registration.
- `bagelquant_data.lake`: local storage, immutable snapshots, catalogs, direct
  lake reads, and provider update planning/execution.
- `bagelquant_data.loader`: lake-first retrieval, provider fallback, lineage
  metadata, and panel-shaped outputs.
- `bagelquant_data.metadata`: dataset identity, schemas, contracts, and lineage.
- `bagelquant_data.transform`: stateless pandas transformation pipelines.
- `bagelquant_data.cache`: optional cache interfaces that do not change dataset
  identity.

## Lake Management

The local lake is separated by source, table, partition, and snapshot:

```text
.bagelquant-data-lake/
  tushare/
    daily/
      _catalog.json
      year=2024/
        month=01/
          _catalog.json
          snapshots/
            20240131T120000000000Z/
              data.parquet
              metadata.json
```

Every stored table receives lifecycle columns `create_time` and `delete_flag`.
Panel-like data uses a `date` index. Reference tables that are not panel-like,
such as `stock_basic`, keep their ordinary row index.

Manage local datasets directly:

```python
manager.add("custom", "prices", frame)
manager.edit("custom", "prices", corrected_frame)
manager.delete("custom", "prices")

print(manager.list_sources())
print(manager.list_tables("tushare"))
print(manager.snapshots("tushare", "daily"))
```

Read with projection and date filters:

```python
close = lake.read(
    "tushare",
    "daily",
    columns=("close",),
    start_date="2024-01-01",
    end_date="2024-01-31",
)
```

Inspect source catalogs:

```python
asset_ids = lake.asset_ids("tushare")
fields = lake.fields("tushare")
panel_field_ids = lake.panel_field_ids("tushare")
```

## Tushare Updates

Token resolution order:

1. `TushareDataSource(token=...)`
2. `TUSHARE_TOKEN`
3. `Settings(tushare_token=...)`

Tokens are not returned by `describe()` and are redacted from `repr()`.

Refresh reference resources first:

```python
manager.update_tushare_stock_basic()
manager.update_tushare_trading_calendar(start_date="2000-01-01")
```

Scan provider updates before execution:

```python
from bagelquant_data.lake import TushareTableUpdateSpec, TushareTradingCalendarRef

report = manager.scan_tushare_updates(
    specs=(
        TushareTableUpdateSpec(
            table="daily",
            kind="price",
            trading_calendar=TushareTradingCalendarRef(
                name="trade_cal",
                table="trade_cal",
                date_column="cal_date",
                open_column="is_open",
            ),
        ),
    ),
    start_date="2024-01-01",
    end_date="2024-12-31",
)

refs = manager.execute_tushare_update_report(report, workers=4)
```

`update_tushare_all(...)` remains available as a convenience wrapper for one
table. New code should prefer `scan_tushare_updates(specs=...)` because it keeps
table kind, update universe, and trading calendar bindings together.

Tushare `stock_basic` is refreshed from listed, delisted, and paused stocks to
avoid survivorship bias. Price tables such as `daily` and `index_daily` are
planned from compact update-record rows and fetched day by day over open trading
dates. Fundamental tables update per asset, and VIP fundamental tables update by
reporting season.

## Retrieval

Load a dataset with lake-first behavior:

```python
loaded = Loader(registry=registry, lake=lake).source("tushare").load(
    "daily",
    fields=("open", "close"),
    start_date="2024-01-01",
    end_date="2024-01-31",
)
```

Retrieve a panel-shaped object without importing `bagelquant-core`:

```python
retrieved = Loader(registry=registry, lake=lake).source("tushare").load_panel(
    dataset="daily",
    field="close",
    universe=["000001.SZ", "600000.SH"],
    start_date="2024-01-01",
    end_date="2024-12-31",
)
```

Read a qualified lake field directly as a date-by-asset panel:

```python
panel = lake.read_panel_field(
    "tushare_daily_close",
    start_date="2024-01-01",
    end_date="2024-12-31",
)
```

Downstream code can adapt `RetrievedPanel` explicitly:

```python
from bagelquant_core import Domain, Panel

domain = Domain(calendar=retrieved.calendar, universe=retrieved.universe)
panel = Panel.from_domain(
    retrieved.data,
    domain,
    name=retrieved.dataset_name,
    metadata=retrieved.metadata,
)
```

See `examples/backend_data_lake_workflow.py` for a complete backend workflow.

## Development

```bash
uv sync --all-groups --extra tushare
uv run ruff check .
uv run pytest
uv run mkdocs build --strict
```

## License

Apache License 2.0
