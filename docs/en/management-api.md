# Management API

The management API is exposed from `DataLake.open(...)`.

```python
from bagelquant_data import DataLake

lake = DataLake.open("data")
```

The facade exposes:

- `lake.sources`
- `lake.datasets`
- `lake.update`
- `lake.query`
- `lake.finance`
- `lake.status`

## Source Management

Register a source adapter:

```python
from bagelquant_data.sources.tushare import TushareSource

lake.sources.register(TushareSource(name="tushare"))
```

Configure a source:

```python
lake.sources.configure("tushare", token="...")
```

The Tushare convenience method delegates to the generic configuration method:

```python
lake.sources.configure_tushare(token="...")
```

List registered sources:

```python
sources = lake.sources.list()
```

Get a registered adapter:

```python
tushare = lake.sources.get("tushare")
```

Test a source connection:

```python
lake.sources.test("tushare")
```

Remove a source registration:

```python
lake.sources.remove("tushare")
```

Removing a source registration does not delete canonical data.

## Dataset Management

Dataset behavior is declared by `DatasetSpec` objects or YAML files.

Add a spec object:

```python
from bagelquant_data import DatasetSpec

spec = DatasetSpec(
    name="daily",
    source="custom",
    source_dataset="daily",
    category="market",
    field_mapping={"ts_code": "ts_code", "trade_date": "trade_date"},
    required_columns=("asset_id", "time"),
    primary_key=("asset_id", "time"),
    asset_column="ts_code",
    time_column="trade_date",
    partition_strategy="year_month",
    deduplication="primary_key_last",
    sort_columns=("time", "asset_id"),
)

lake.datasets.add(spec)
```

Add a YAML spec:

```python
lake.datasets.add_from_yaml(
    "src/bagelquant_data/sources/tushare/datasets/daily.yaml"
)
```

Get a dataset:

```python
spec = lake.datasets.get("daily", source="tushare")
```

List datasets:

```python
all_datasets = lake.datasets.list()
tushare_datasets = lake.datasets.list("tushare")
```

Enable or disable a dataset:

```python
lake.datasets.enable("daily", source="tushare")
lake.datasets.disable("daily", source="tushare")
```

Remove a dataset registration without deleting data:

```python
lake.datasets.remove("daily", source="tushare")
```

Delete canonical data only with explicit confirmation:

```python
lake.datasets.remove(
    "daily",
    source="tushare",
    delete_data=True,
    confirm=True,
)
```

## Status And Inspection

Summary:

```python
summary = lake.status.summary()
```

Dataset status:

```python
status = lake.status.dataset("income", source="tushare")
```

Partition manifest:

```python
partitions = lake.status.partitions("income", source="tushare")
```

Recent ingestion runs:

```python
runs = lake.status.runs(limit=20)
```

Failed runs:

```python
failures = lake.status.failures(dataset="income", source="tushare")
```

Files known to the manifest:

```python
files = lake.status.files("income", source="tushare")
```

Normal status calls use SQLite manifest metadata. They are designed to be cheap and do not need to scan every Parquet file.

## Canonical Record Inspection

Use `lake.query.records(...)` for human inspection:

```python
records = lake.query.records(
    "income",
    source="tushare",
    limit=10,
)
```

This is not the main research extraction API. Use `lake.query.field(...)` for single-value panels.
