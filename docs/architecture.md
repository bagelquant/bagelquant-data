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
    +--> PanelInputAgreement
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

## Dependency Direction

`bagelquant-data` is below downstream repositories. It must not import
`bagelquant-core`, `bagelquant-bt`, `bagelquant-app`, or documentation sites.

The communication boundary with `bagelquant-core` is `PanelInputAgreement`.
That agreement exposes pandas data and `DomainSpec` constructor kwargs, leaving
`bagelquant-core` responsible for creating `Domain`, `Panel`, and
`CategoryPanel`.
