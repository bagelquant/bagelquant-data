# Architecture

BagelQuant Data is a source-agnostic data lake package. Source adapters fetch provider data, while dataset specs control normalization, validation, deduplication, update type, and query contracts.

## Layers

- `core`: dataset specs, source protocols, registries, request context, normalization contracts, deduplication, validation, hashing, and exceptions.
- `sources`: source adapters. `sources/tushare` is the bundled implementation.
- `storage`: lake paths, atomic Parquet writes, SQLite metadata, staging files, rejected records, and manifests.
- `pipeline`: ingestion, update orchestration, validation, and canonical commits.
- `management`: `DataLake`, `LakeAdmin`, `LakeUpdater`, and source/dataset/status managers.
- `query`: `LakeQuery`, raw records, panels, prices, fundamentals, events, references, and observation grids.
- `finance`: PIT and financial transforms used by `LakeQuery`.
- `cli`: status and list commands around the Python API.

## Public Root

```python
from bagelquant_data import DataLake

lake = DataLake.open("data")
lake.admin.summary()
lake.update.dataset("daily", source="tushare")
lake.query.price("daily", "close", source="tushare")
```

The three primary surfaces are:

- `lake.admin` for data lake management.
- `lake.update` for update execution.
- `lake.query` for research access.

## Storage Zones

```text
data/
    lake/
    staging/
    rejected/
    metadata/
        lake.db
    tmp/
```

`lake/` contains validated canonical Parquet files.

`staging/` contains temporary source responses during ingestion. Staging is cleaned after commit attempts.

`rejected/` contains records rejected during normalization.

`metadata/lake.db` stores sources, datasets, runs, API calls, partition manifests, and rejected summaries.

`tmp/` is reserved for local working files owned by the package.

## Canonical Records

Canonical records are row-oriented. Non-reference datasets must expose:

- `asset_id`: canonical asset identifier.
- `time`: observation time or information availability time.

Point-in-time fundamental datasets also use:

- `period`: economic or accounting period represented by the record.

Normalizers preserve source columns when possible and add canonical fields such as `source`, `source_dataset`, `asset_id`, `time`, and `period`.

## Dataset Kinds

`DatasetSpec.data_kind` declares the intended query behavior:

- `price`: price or market panel data.
- `fundamental`: PIT financial/fundamental records.
- `event`: append-only event records.
- `reference`: row-oriented reference data.
- `generic`: other canonical records.

## Query Contracts

`lake.query.raw(...)` returns canonical row-oriented records.

`lake.query.field(...)`, `lake.query.panel(...)`, and `lake.query.price(...)` return one value field as:

```text
time | asset_id | value_column
```

`lake.query.fundamental(...)` returns PIT fundamental events, or latest available values when an observation grid is supplied.

`lake.query.events(...)` returns event-style canonical records and can filter by `event_type`.

`lake.query.reference(...)` returns row-oriented reference data.

## Point-In-Time Semantics

`time` and `period` are separate.

`time` is when information became available to a researcher.

`period` is the economic or accounting period represented by the record.

PIT alignment never exposes a record at an observation date earlier than its canonical `time`.

## Update Types And Layout

`DatasetSpec.update_type` selects both update planning and canonical layout:

- `general`: whole-dataset replacement in `data.parquet`.
- `by_daily`: calendar-driven missing-date updates in `year=YYYY/month=MM/data.parquet`.
- `by_id`: reference-ID updates in `year=YYYY/batch=BB/data.parquet`.

`by_id` stable batches use Blake2b so batch assignment is deterministic across Python processes.

## Manifests And Atomic Writes

Every canonical write updates SQLite manifest rows with partition path, partition values, row count, file size, min/max time, content hash, schema hash, and update time.

Canonical file replacement writes a temporary Parquet file, reads it back, validates row count, atomically replaces the destination path, and then updates the manifest.

Manifest status calls use SQLite metadata. `lake.admin.rebuild_manifest(...)` can reconstruct manifest rows from canonical Parquet files.
