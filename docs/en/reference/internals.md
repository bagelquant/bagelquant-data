# Internal Documentation

This page describes implementation boundaries for maintainers and extension
authors.

## Dependency Direction

`bagelquant-data` sits below the rest of the ecosystem. It must not import
`bagelquant-core`, `bagelquant-bt`, `bagelquant-app`, or the website.

The handoff to `bagelquant-core` is plain data: `RetrievedPanel` exposes a
DataFrame, sorted calendar, and universe. Downstream code constructs `Domain`
and `Panel` objects itself.

## Storage Layout

Local V1 storage is partitioned by source, table, year, and month:

```text
lake-root/
  tushare/
    daily/
      year=2024/
        month=01/
          snapshots/
```

JSON catalogs track source-level ids, table metadata, latest snapshot pointers,
and inferred panel fields. Reads use partition metadata to skip snapshots
outside the requested date range, then apply exact filtering after loading.

## Update Flow

Normal provider refresh:

```text
DataRequest -> DataSource.read -> DataLakeManager.update -> LocalDataLake.write
```

Tushare production-style refresh:

```text
reference refresh -> scan_tushare_updates -> review report -> execute jobs
```

The report is the review boundary. Execution should consume the confirmed jobs
from that report instead of rebuilding plans implicitly.

## Module Structure

- `datasource`: provider adapters, request objects, and registry.
- `lake`: snapshot storage, catalogs, direct reads, and update orchestration.
- `loader`: lake-first retrieval and panel-shaped return objects.
- `metadata`: schemas, contracts, identities, and lineage.
- `transform`: stateless DataFrame transformation pipelines.
- `cache`: optional cache policies and implementations.
- `config`: environment and profile settings.

## Failure Handling

Provider and lake errors should include the source, dataset, requested date
range, and operation whenever possible. Missing optional provider dependencies
should fail at adapter construction or first provider use, not during package
import.

