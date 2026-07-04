# BagelQuant Data Documentation

BagelQuant Data is a Polars-native, source-agnostic local data lake framework for quantitative research. It stores canonical research data in Parquet, stores operational state in SQLite, and exposes a stable Python API centered on `DataLake.open(...)`.

The framework is intentionally built around two ideas:

- Source adapters fetch data, but they do not define the core lake architecture.
- Dataset specifications define behavior, so normal datasets can be added without changing storage, query, finance, or management code.

## Documentation Map

- [Architecture](architecture.md): system layers, storage zones, metadata, canonical fields, and point-in-time semantics.
- [Configuration](configuration.md): lake root, credentials, package extras, and local environment setup.
- [Management API](management-api.md): source registration, dataset registration, status, inspection, and destructive operations.
- [Extraction API](extraction-api.md): raw records, single-field long panels, multiple fields, reference data, and observation grids.
- [Financial API](financial-api.md): point-in-time alignment and generic composable financial transforms.
- [Incremental Updates](incremental-updates.md): update flow, staging, canonical commits, manifests, and current V1 limitations.
- [Tushare Source](tushare.md): bundled Tushare adapter, credentials, dataset specs, and canonical time mappings.
- [Adding A Source](adding-a-source.md): how to implement a new source adapter.
- [Adding A Dataset](adding-a-dataset.md): how to write dataset YAML and choose strategies.
- [Implementation Plan](implementation-plan.md): design goals and roadmap notes.

## First Example

```python
import polars as pl

from bagelquant_data import DataLake, DatasetSpec

lake = DataLake.open("data")

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

lake.ingest_frame(
    spec,
    pl.DataFrame(
        {
            "trade_date": ["20250102", "20250103"],
            "ts_code": ["000001.SZ", "000001.SZ"],
            "open": [11.20, 11.31],
            "close": [11.25, 11.37],
        }
    ),
)

close = lake.query.field("daily", "close", source="custom", collect=True)
print(close)
```

The output is a long panel with exactly three columns:

```text
time        asset_id    close
2025-01-02  000001.SZ   11.25
2025-01-03  000001.SZ   11.37
```

## Design Boundaries

BagelQuant Data does not preserve old lake layouts or old public APIs. The current framework is a clean API and storage design. It also does not provide hardcoded financial indicators such as `eps_ttm()` or `roe_ttm()`. Instead, it provides generic building blocks such as trailing aggregation, ratio calculation, stock averaging, and point-in-time alignment.

Operational workflows are intentionally outside this package. Use `manage-data-lake` to configure or update a lake and use `factor-model` for factor lifecycle and testing workflows. This package should remain a reusable library with source adapters, storage/query APIs, and small API examples only.

## Module Reference

- `bagelquant_data.core`: dataset specs, request context, validation, hashing, registries, and source protocols.
- `bagelquant_data.storage`: atomic Parquet writes, metadata/manifests, lake paths, rejected rows, and staging files.
- `bagelquant_data.pipeline`: ingestion, canonical commit, and incremental update orchestration.
- `bagelquant_data.management`: the `DataLake` facade and source/dataset/status managers.
- `bagelquant_data.query`: raw, records, field, reference, and observation-grid query services.
- `bagelquant_data.finance`: point-in-time alignment and reusable financial transforms.
- `bagelquant_data.sources.tushare`: Tushare authentication, client construction, and source adapter.
- `bagelquant_data.cli`: a thin status/list wrapper around the Python API.

## Development Checks

Run the local verification suite with:

```bash
uv run pytest
uv run pyright
```
