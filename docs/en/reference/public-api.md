# Public API

`bagelquant-data` is operated through Python APIs. The package intentionally
returns Polars objects and plain metadata instead of importing downstream
research packages.

## Top-Level Exports

```python
from bagelquant_data import (
    DataContract,
    DataLakeManager,
    DataRequest,
    DataSource,
    DataSourceRegistry,
    DatasetSchema,
    FieldSchema,
    LoadedDataset,
    Loader,
    LocalDataLake,
    RetrievedPanel,
    Transform,
    TushareTableUpdateSpec,
    default_registry,
)
```

## Provider Registry

- `DataSource`: base provider adapter protocol.
- `DataRequest`: dataset, fields, filters, date range, version, snapshot, and options.
- `DataSourceRegistry.register(source)`: register an adapter.
- `DataSourceRegistry.resolve(name)`: retrieve a registered adapter.
- `default_registry`: process-level convenience registry.

## Data Lake

- `LocalDataLake(root)`: local filesystem snapshot backend.
- `DataLakeManager(lake, registry=None)`: high-level mutation and update API.
- `DataLakeManager.add(source, dataset, frame)`: add custom data.
- `DataLakeManager.edit(source, dataset, frame)`: replace a dataset snapshot.
- `DataLakeManager.delete(source, dataset)`: delete a dataset pointer.
- `LocalDataLake.read(...)`: read projected and date-filtered data.
- `LocalDataLake.read_panel_field(...)`: shape a qualified field id into a panel frame.

## Loader

- `Loader(registry=None, lake=None)`: lake-first retrieval facade.
- `Loader.source(name).load(...)`: return `LoadedDataset`.
- `Loader.source(name).load_panel(...)`: return `RetrievedPanel`.
- `LoadedDataset.data`: Polars dataset payload.
- `RetrievedPanel.data`: long-form `time`, `asset_id`, `value` frame for one field.

## Metadata And Contracts

- `DataContract`: provider or dataset contract.
- `DatasetSchema`: dataset-level schema metadata.
- `FieldSchema`: field-level metadata.
- `Transform`: stateless DataFrame transformation pipeline.

## Tushare Helpers

- `TushareDataSource`: provider adapter exposed from `bagelquant_data.datasource`.
- `TushareTableUpdateSpec`: production-style table update specification.
- `DataLakeManager.scan_tushare_updates(...)`: build a dry-run update report.
- `DataLakeManager.execute_tushare_update_report(report, workers=4)`: execute reviewed jobs.

