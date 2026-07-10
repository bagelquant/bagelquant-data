# BagelQuant Data Documentation

BagelQuant Data is a Polars-native, source-agnostic data lake package for quantitative research. It stores canonical research data in Parquet, stores mutable operational metadata in SQLite, and exposes one root object through `DataLake.open(...)`.

The package has three primary capabilities:

- `lake.admin`: manage sources, dataset specs, manifests, health, rejected rows, repair, and deletion.
- `lake.update`: plan and run dataset updates with batching, pagination, retries, and run observability.
- `lake.query`: query price panels, PIT fundamentals, events, reference data, and raw canonical records.

## Documentation Map

- [Architecture](architecture.md): package layers, storage zones, canonical records, manifests, and PIT semantics.
- [Configuration](configuration.md): lake roots, dependencies, credentials, and local checks.
- [Management API](management-api.md): source and dataset management, status, rejected rows, and manifest repair.
- [Incremental Updates](incremental-updates.md): lake-owned planning, fetch, staging, commit, update types, and run metadata.
- [Extraction API](extraction-api.md): raw records, panels, prices, events, references, and observation grids.
- [Financial API](financial-api.md): PIT fundamental extraction and generic financial transforms.
- [Tushare Source](tushare.md): bundled Tushare adapter and parameter mapping.
- [Adding A Source](adding-a-source.md): source adapter protocol and registration.
- [Adding A Dataset](adding-a-dataset.md): compact dataset registration and update types.

## First Example

```python
import polars as pl

from bagelquant_data import DataLake

lake = DataLake.open("data")

spec = lake.admin.datasets.add("daily", "by_daily", reference="trade_cal")
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

print(lake.admin.summary())
close = lake.query.price("daily", "close", source="custom", collect=True)
print(close)
```

`lake.query.price(...)` returns a long panel:

```text
time | asset_id | close
```

## Module Reference

- `bagelquant_data.core`: dataset specs, request context, validation, hashing, registries, and source protocols.
- `bagelquant_data.storage`: atomic Parquet writes, metadata/manifests, lake paths, rejected rows, and staging files.
- `bagelquant_data.pipeline`: ingestion, canonical commit, and update orchestration.
- `bagelquant_data.management`: `DataLake`, `LakeAdmin`, `LakeUpdater`, and source/dataset/status managers.
- `bagelquant_data.query`: `LakeQuery`, raw records, panels, prices, fundamentals, events, references, and observation grids.
- `bagelquant_data.finance`: reusable PIT and financial transforms used by `LakeQuery`.
- `bagelquant_data.sources.tushare`: Tushare authentication, client construction, and source adapter.
- `bagelquant_data.cli`: thin status/list commands around the Python API.

## Development Checks

```bash
uv run pytest
uv run pyright
uv run ruff check .
```
