# Concepts

## DataSource

`DataSource` isolates provider access behind `read`, `exists`, and `describe`.
Adapters live in `bagelquant_data.datasource` and consume `DataRequest` objects
so callers do not depend on provider-specific client APIs.

## Loader

`Loader` coordinates requests and returns standardized `LoadedDataset` objects.
When configured with a lake, it reads local lake snapshots first and only hits a
provider for bootstrap or explicit refresh.

## Data Lake Manager

`DataLakeManager` owns add, edit, delete, list, and manual provider updates for
the local lake.

Each source's first configured table is its universe-like reference table. For
Tushare, this is `stock_basic`, refreshed from listed, delisted, and paused
stocks to avoid survivorship bias.

## Transform

Transforms are stateless DataFrame operations that can be chained with
`Transform`.

## Metadata

Metadata exists independently of data. Contracts describe dataset identity,
schema, freshness, ownership, version, and lineage.

## Data Lake

Lake storage is separated by data source and table. Tables may also be
partitioned by normalized `time`: daily price tables use year/month/day
partitions, while fundamental tables use the `f_ann_date` year for
point-in-time availability. Writes create immutable snapshots and update latest
pointers at the table and partition level. Panel-like tables expose normalized
`time` and `asset_id` columns. Reference tables that are not panel-like, such as
`stock_basic`, keep table-level snapshots.

Use `LocalDataLake.read` for direct table reads, `read_panel_field` for
long-form `time`, `asset_id`, `value` panels, `fields` for field catalogs, and
`asset_ids` for source asset catalogs.

## Cache

Cache interfaces are optional and should not change dataset identity or
reproducibility guarantees.
