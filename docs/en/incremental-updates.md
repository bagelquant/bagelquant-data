# Incremental Updates

The update API lives under `lake.update`.

```python
lake.update.dataset("daily", source="tushare")
lake.update.datasets(["daily", "daily_basic"], source="tushare")
lake.update.source("tushare")
```

## Update Flow

The V1 update flow is:

```text
request planning
-> fetch
-> staging
-> normalization
-> validation
-> deduplication
-> partition write
-> manifest update
-> staging cleanup
```

Dataset behavior is selected by `DatasetSpec`, not by hardcoded branches in the core pipeline.

## Request Context

The public update API accepts:

- `start`
- `end`
- `assets`
- `workers`
- `batch_size`
- `source_options`
- `progress`
- `max_retries`
- `retry_backoff_seconds`

These values are passed through `RequestContext`. Source adapters may use them to plan requests.
Fetches are parallelized across planned requests/pages, but canonical Parquet commits remain serial per dataset.

Example:

```python
lake.update.dataset(
    "income",
    source="tushare",
    assets=["000001.SZ", "600000.SH"],
    start="2020-01-01",
    end="2026-06-15",
    batch_size=250,
)
```

For offset-paginated APIs, add request options to the dataset spec:

```yaml
request_options:
  pagination: offset
  page_size: 5000
  limit_param: limit
  offset_param: offset
  offset_start: 0
```

Each physical page is retried independently and counted in progress/failure metadata.

## Staging

Fetched source responses are written to `data/staging`. Staging preserves source response boundaries while the pipeline normalizes and commits canonical data.

After a successful commit, staging fragments for the run are removed.

## Canonical Commit

The commit step:

1. Normalizes source rows into canonical fields.
2. Applies partition-derived columns.
3. Runs framework validation.
4. Applies the dataset deduplication strategy.
5. Sorts by the dataset sort order.
6. Writes affected canonical Parquet files.
7. Updates SQLite manifests.

## Current V1 Behavior

The current implementation provides the new API and storage foundation. It supports canonical writes and manifest-driven status. More advanced incremental optimizations are design targets for future iterations:

- asset-level content-hash skipping
- micro-batched partition merging
- partition locks around concurrent writers
- manifest rebuild tooling
- compaction thresholds and repair operations

The API already leaves room for those features without changing user-facing method names.

## Status After Updates

```python
print(lake.status.dataset("daily", source="tushare"))
print(lake.status.runs(limit=20))
```

Status reads SQLite metadata and manifests by default.
