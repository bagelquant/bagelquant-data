# Backend API

`bagelquant-data` is operated through Python APIs. The main workflow is:

```text
DataSourceRegistry
  -> DataSource
  -> DataLakeManager
  -> LocalDataLake
  -> Loader / lake.read / lake.read_panel_field
```

## Setup

```python
from bagelquant_data.datasource import DataSourceRegistry, TushareDataSource
from bagelquant_data.lake import DataLakeManager, LocalDataLake

registry = DataSourceRegistry()
registry.register(TushareDataSource(token="your-token"))

lake = LocalDataLake(".bagelquant-data-lake")
manager = DataLakeManager(lake, registry=registry)
```

## Data Lake Management

`LocalDataLake` owns filesystem storage. It writes immutable Parquet snapshots
under source/table partitions and maintains JSON catalog pointers.

```python
manager.add("custom", "prices", frame)
manager.edit("custom", "prices", corrected_frame)
manager.delete("custom", "prices")

sources = manager.list_sources()
tables = manager.list_tables("custom")
snapshots = manager.snapshots("custom", "prices")
```

Use `LocalDataLake.read` for direct backend reads:

```python
data = lake.read(
    "tushare",
    "daily",
    columns=("close",),
    start_date="2024-01-01",
    end_date="2024-01-31",
)
```

## Provider Updates

`DataLakeManager.update` performs a simple provider read and writes one local
snapshot.

```python
from bagelquant_data.datasource import DataRequest

manager.update(
    "tushare",
    DataRequest(
        dataset="daily",
        filters={"ts_code": "000001.SZ"},
        start_date="2024-01-01",
        end_date="2024-01-31",
    ),
)
```

For Tushare production-style updates, refresh references, scan, then execute the
confirmed report:

```python
from bagelquant_data.lake import TushareTableUpdateSpec, TushareTradingCalendarRef

manager.update_tushare_stock_basic()
manager.update_tushare_trading_calendar(start_date="2000-01-01")

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

manager.execute_tushare_update_report(report, workers=4)
```

The report is the review boundary. It contains plans and executable jobs, so
callers can inspect pending work before running provider reads.

## Retrieval

`Loader` returns `LoadedDataset` objects with data, identity, lineage, and
metadata. With a lake configured, it reads local snapshots first and uses the
provider only for bootstrap or explicit refresh.

```python
from bagelquant_data.loader import Loader

loaded = Loader(registry=registry, lake=lake).source("tushare").load(
    "daily",
    fields=("open", "close"),
    start_date="2024-01-01",
    end_date="2024-01-31",
)
```

For panel-shaped research inputs, use `load_panel` or `load_panel_field`.
These APIs return plain Polars objects and do not import downstream packages.

```python
retrieved = Loader(registry=registry, lake=lake).source("tushare").load_panel(
    dataset="daily",
    field="close",
    universe=["000001.SZ", "600000.SH"],
    start_date="2024-01-01",
    end_date="2024-12-31",
)

panel = lake.read_panel_field(
    "tushare_daily_close",
    start_date="2024-01-01",
    end_date="2024-12-31",
)
```

## Function Reference

- `DataRequest(dataset, fields=(), filters={}, start_date=None, end_date=None,
  version=None, snapshot=None, options={})`: provider read request.
- `DataSourceRegistry.register(source)`: register a provider adapter.
- `DataSourceRegistry.resolve(name)`: retrieve a registered provider.
- `DataLakeManager.update(source, request, mode="overwrite")`: fetch provider
  data and write a lake snapshot.
- `DataLakeManager.scan_tushare_updates(...)`: build a dry-run Tushare update
  report.
- `DataLakeManager.execute_tushare_update_report(report, workers=4)`: execute
  report jobs and write snapshots.
- `LocalDataLake.read(source, dataset, columns=None, start_date=None,
  end_date=None, year=None, month=None, snapshot=None)`: read local data.
- `LocalDataLake.read_panel_field(qualified_id, start_date, end_date)`: shape a
  qualified field id into a long-form `time`, `asset_id`, `value` panel.
- `Loader.source(name).load(...)`: load a dataset as `LoadedDataset`.
- `Loader.source(name).load_panel(...)`: load and shape a `RetrievedPanel`.
