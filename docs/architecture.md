# Architecture

```text
Provider APIs and files
    |
    v
DataSource
    |
    v
DataLakeManager.update
    |
    v
LocalDataLake
    |
    +--> LoadedDataset
    |
    +--> RetrievedPanel
             |
             v
       downstream adapter
```

The normal path is provider to local lake to user. `Loader` reads from the local
lake first when configured. Provider access is used for bootstrap and refresh.

The package keeps provider access, metadata, transformation, and storage
interfaces independent. Local V1 storage uses source/table/year/month Parquet
snapshots with JSON metadata, while the interfaces leave room for future
Iceberg, Delta, object storage, or cloud backends.

```text
lake-root/
  tushare/
    daily/
      year=2024/
        month=01/
          snapshots/
```

Reads can project columns and filter dates at the lake boundary. `LocalDataLake`
uses partition metadata to skip snapshots outside the requested range, then
applies exact date filtering after reading. This keeps the API simple while
reducing IO for common panel-field and date-window reads.

The lake maintains source-level system catalogs for asset ids and data item ids.
Table catalog metadata records inferred date, asset, and panel field columns so
`read_panel_field` can load only the requested field plus the minimum date and
asset columns needed to shape a date-by-asset panel.

## Dependency Direction

`bagelquant-data` is below downstream repositories. It must not import
`bagelquant-core`, `bagelquant-bt`, `bagelquant-app`, or documentation sites.

The communication boundary with `bagelquant-core` is plain retrieved data.
`RetrievedPanel` exposes pandas data, universe, and sorted calendar objects,
leaving downstream code responsible for creating `Domain`, `Panel`, and
`CategoryPanel` without coupling the packages.

## Tushare Update Specs

Provider updates are described with `TushareTableUpdateSpec`. A spec keeps a
table's kind, update-universe reference, and trading calendar reference in one object,
which avoids parallel argument maps drifting apart. The manager scans specs into
a dry-run `TushareUpdateReport`, and execution consumes only the confirmed jobs
from that report.
