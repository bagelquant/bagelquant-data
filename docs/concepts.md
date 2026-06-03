# Concepts

## DataSource

`DataSource` isolates provider access behind `read`, `exists`, and `describe`.

## Loader

`Loader` coordinates requests and returns standardized `LoadedDataset` objects.
When configured with a lake, it reads local lake snapshots first and only hits a
provider for bootstrap or explicit refresh.

## Data Lake Manager

`DataLakeManager` owns add, edit, delete, list, provider update, and periodic
job orchestration for the local lake.

Provider updates refresh each source's `All` universe. User-defined universes
are validated subsets of `All` and do not limit lake refreshes.

## Transform

Transforms are stateless DataFrame operations that can be chained with
`Transform`.

## Metadata

Metadata exists independently of data. Contracts describe dataset identity,
schema, freshness, ownership, version, and lineage.

## Data Lake

Lake storage is separated by data source, table, year, and month. Writes create
immutable snapshots and update latest pointers at the table and partition level.
Every stored table has a `date` index plus `create_time` and `delete_flag`
columns. The lake also maintains source-local asset and data item id tables.
Reference tables that are not panel-like, such as `stock_basic`, keep their
ordinary row index while still receiving lifecycle columns.

## Cache

Cache interfaces are optional and should not change dataset identity or
reproducibility guarantees.
