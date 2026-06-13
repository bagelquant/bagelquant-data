# Quick Start

`bagelquant-data` is the provider-neutral data layer for BagelQuant. It manages
provider adapters, local lake snapshots, metadata, and panel-shaped retrieval.

## Install

```bash
uv add bagelquant-data
```

Use the optional Tushare dependency when loading from Tushare:

```bash
uv add "bagelquant-data[tushare]"
```

## Register A Provider

```python
from bagelquant_data.datasource import DataSourceRegistry, TushareDataSource

registry = DataSourceRegistry()
registry.register(TushareDataSource(token="your-token"))
```

## Create A Local Lake

```python
from bagelquant_data.lake import DataLakeManager, LocalDataLake

lake = LocalDataLake(".bagelquant-data-lake")
manager = DataLakeManager(lake, registry=registry)
```

## Write Or Refresh Data

For custom data, write a Polars frame directly:

```python
import polars as pl

prices = pl.DataFrame(
    {
        "time": ["2024-01-02", "2024-01-02"],
        "asset_id": ["000001.SZ", "600000.SH"],
        "close": [9.83, 7.12],
    }
)
manager.add("custom", "prices", prices)
```

For provider data, request a dataset and persist the result as a local snapshot:

```python
from bagelquant_data.datasource import DataRequest

manager.update(
    "tushare",
    DataRequest(
        dataset="daily",
        fields=("ts_code", "trade_date", "close"),
        start_date="2024-01-01",
        end_date="2024-01-31",
    ),
)
```

## Retrieve Data

```python
from bagelquant_data.loader import Loader

loaded = Loader(registry=registry, lake=lake).source("tushare").load(
    "daily",
    fields=("open", "close"),
    start_date="2024-01-01",
    end_date="2024-01-31",
)

frame = loaded.data
```

For research inputs, load a long-form panel field:

```python
retrieved = Loader(registry=registry, lake=lake).source("tushare").load_panel(
    dataset="daily",
    field="close",
    universe=["000001.SZ", "600000.SH"],
    start_date="2024-01-01",
    end_date="2024-12-31",
)
```

`RetrievedPanel` exposes `data` as a Polars frame with `time`, `asset_id`, and
`value` columns plus calendar and universe objects so downstream packages can
decide how to construct their own domain objects.

