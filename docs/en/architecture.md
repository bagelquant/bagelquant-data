# Architecture

BagelQuant Data is organized as a source-agnostic framework. Tushare is the first bundled source, but the core modules do not import Tushare-specific code.

## Layer Overview

The package is split into clear layers:

- `core`: dataset specifications, source protocols, registries, request context, normalization contracts, partitioning, deduplication, validation, hashing, and exceptions.
- `sources`: source adapters. `sources/tushare` is the first implementation.
- `storage`: local filesystem layout, Parquet writes, atomic replacement, SQLite metadata, staging, rejected records, and manifests.
- `pipeline`: ingestion, update orchestration, validation, and commit logic.
- `query`: raw canonical records, single-field panels, reference data, records inspection, and observation grids.
- `finance`: generic point-in-time and financial transformations.
- `management`: public `DataLake` facade and managers for sources, datasets, status, and updates.
- `cli`: a thin command-line wrapper around the Python API.

## Storage Zones

Opening a lake creates the following layout:

```text
data/
    lake/
    staging/
    rejected/
    metadata/
        lake.db
    tmp/
```

`data/lake` contains validated canonical Parquet files. Only canonical lake data is exposed through `lake.query` and `lake.finance`.

`data/staging` contains temporary source responses written during ingestion. Staging files are allowed to be small because they are not the analytical storage format. Successful commits remove staging fragments for the run.

`data/rejected` contains records that cannot safely enter canonical storage. Examples include missing canonical identifiers, invalid dates, or malformed source responses.

`data/metadata/lake.db` is SQLite operational state. It stores registered sources, registered datasets, ingestion runs, partition manifests, rejected summaries, asset state, and partition locks. WAL mode is enabled by the metadata store.

`data/tmp` is reserved for future build fragments, compaction work, validation, and partition replacement tasks.

## Canonical Records

Canonical Parquet records are row-oriented. They may contain many fields from the source response, plus canonical fields added by normalization.

All non-reference datasets must have:

- `asset_id`: canonical asset identifier.
- `time`: the observation time or the information availability time.

Point-in-time financial datasets also use:

- `period`: the economic or accounting period represented by the record.

Operational fields may include:

- `source`
- `source_dataset`
- `ingested_at`
- `record_hash`

The original source columns are preserved when possible. For example, financial statement records may keep `ann_date`, `f_ann_date`, and `end_date` while also exposing canonical `time` and `period`.

## Public Output Contract

Canonical storage is not the same as public research output.

`lake.query.raw(...)` returns canonical row-oriented records. This is useful for inspection, debugging, and advanced workflows.

`lake.query.field(...)` returns one research field at a time as a long panel:

```text
time | asset_id | requested_value_column
```

The API does not pivot into wide tables. A multi-field request returns a dictionary of independent long panels:

```python
ohlcv = lake.query.fields(
    "daily",
    ["open", "high", "low", "close", "vol"],
    source="tushare",
)
```

Each returned frame has exactly three columns.

## Point-In-Time Semantics

`time` and `period` are intentionally separate.

`time` is when information became available to a researcher.

`period` is the economic or accounting period represented by the record.

For daily market data, `time` is usually the trade date. For financial statements, `time` is usually the announcement or final announcement date, while `period` is the statement end date.

The finance API must never expose a financial record at an observation date earlier than its canonical `time`.

## Partitioning

Partitioning is dataset-driven. Initial built-in strategies are:

- `single_file`: one canonical file for the dataset.
- `year_month`: `year=YYYY/month=MM/data.parquet`.
- `year_bucket`: `year=YYYY/bucket=BB/data.parquet`, where bucket is a stable hash of `asset_id`.

The stable bucket algorithm uses Blake2b rather than Python's built-in `hash()`, because Python hash values are intentionally process-randomized.

## Metadata Manifest

Every canonical partition write updates SQLite manifest rows with:

- source
- dataset
- partition path
- partition values
- row count
- file size
- minimum and maximum `time`
- content hash
- schema hash
- update time

Normal status calls use the manifest and avoid rescanning every Parquet file.

## Atomic Writes

Canonical Parquet replacement follows this pattern:

1. Write a temporary file on the same filesystem.
2. Read it back with Polars.
3. Validate the row count.
4. Atomically replace the destination path.
5. Update the partition manifest.

This prevents incomplete files from becoming visible through the query API.
