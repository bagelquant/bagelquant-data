# Operations

Inspect a lake through `lake.admin`:

```python
print(lake.admin.summary())
print(lake.admin.status.dataset("daily", source="tushare"))
print(lake.admin.runs())
print(lake.admin.status.pending_update_jobs(source="tushare"))
```

Failed daily and asset requests remain in the metadata database and are retried
before new work in the next update. A persistent failure does not stop other
dates, assets, or datasets from advancing. Inspect `pending_update_jobs` after a
partial run to find the original request parameters, error, and failure count.

Update reports include `elapsed_seconds`, cumulative `fetch_seconds`,
`commit_seconds`, `metadata_seconds`, `commit_count`,
`partitions_rewritten`, and `peak_in_flight`. Use these counters to distinguish
provider latency from local write overhead. `fetch_seconds` is cumulative across
parallel jobs, so it can exceed wall-clock `elapsed_seconds`.

Use `rebuild_manifest` after repairing local Parquet files and
`validate_manifest` to compare metadata with the stored files. Remove a dataset
with `lake.admin.datasets.remove`; pass `delete_data=True, confirm=True` only
when its Parquet data should also be removed.
