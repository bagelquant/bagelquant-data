# Management API

The management surface lives under `lake.admin`.

```python
from bagelquant_data import DataLake

lake = DataLake.open("data")
admin = lake.admin
```

`lake.admin` groups source management, dataset management, status inspection, rejected-row summaries, and manifest repair.

## Sources

Register and configure a source adapter:

```python
from bagelquant_data.sources.tushare import TushareSource

lake.admin.sources.add(TushareSource(name="tushare"))
lake.admin.sources.edit("tushare", token="...")
```

Inspect and test sources:

```python
sources = lake.admin.sources.list()
tushare = lake.admin.sources.get("tushare")
lake.admin.sources.test("tushare")
```

Enable, disable, or delete a source registration:

```python
lake.admin.sources.disable("tushare")
lake.admin.sources.enable("tushare")
lake.admin.sources.delete("tushare")
```

Deleting a source registration does not delete canonical data.

## Datasets

Dataset behavior is declared with the compact registration API or YAML files.

```python
spec = lake.admin.datasets.add("daily", "by_daily", reference="trade_cal")
```

Load a YAML spec:

```python
lake.admin.datasets.add_from_yaml("datasets/examples/custom_daily.yaml")
```

Inspect, edit, enable, disable, or delete dataset registrations:

```python
spec = lake.admin.datasets.get("daily", source="custom")
datasets = lake.admin.datasets.list("custom")

lake.admin.datasets.edit(updated_spec)
lake.admin.datasets.disable("daily", source="custom")
lake.admin.datasets.enable("daily", source="custom")
lake.admin.datasets.delete("daily", source="custom")
```

Canonical data is deleted only when explicitly requested and confirmed:

```python
lake.admin.datasets.delete(
    "daily",
    source="custom",
    delete_data=True,
    confirm=True,
)
```

## Status

Status reads SQLite metadata and manifests by default.

```python
summary = lake.admin.summary()
runs = lake.admin.runs(limit=20)
failures = lake.admin.failures(dataset="daily", source="custom")
rejected = lake.admin.rejected("daily", source="custom")
```

Dataset and file-level status is available through the focused status manager:

```python
dataset_status = lake.admin.status.dataset("daily", source="custom")
partitions = lake.admin.status.partitions("daily", source="custom")
files = lake.admin.status.files("daily", source="custom")
```

## Manifest Repair

Validate that manifest rows point to existing files:

```python
validation = lake.admin.validate_manifest("daily", source="custom")
```

Rebuild a dataset manifest from canonical Parquet files:

```python
rebuilt = lake.admin.rebuild_manifest("daily", source="custom")
```

`rebuild_manifest` scans canonical files and replaces only that dataset's manifest rows. It does not rewrite canonical Parquet data.
