# Operations

Inspect a lake through `lake.admin`:

```python
print(lake.admin.summary())
print(lake.admin.status.dataset("daily", source="tushare"))
print(lake.admin.runs())
print(lake.admin.status.update_summary(source="tushare"))
```

The update ledger is commit-backed local state. `provider_scope_checks` is the
separate scheduling watermark, while `api_calls` and ingestion runs are the
attempt history. Failed scopes retain their error and attempt count. Invalid
scopes identify provider responses whose keys, date range, asset identity, or
payload did not satisfy the dataset contract.

Use `reset_update_scopes` to retry selected terminal state deliberately. A
dataset declaration or parameter-variant change automatically invalidates its
old scope identities and creates pending work.

Update reports include elapsed, fetch, commit, and metadata timings, commit and
partition counts, and peak in-flight calls. Fetch time is cumulative across
parallel jobs and may exceed elapsed wall-clock time.

Use `rebuild_manifest` after repairing local Parquet files and
`validate_manifest` to compare metadata with stored files. These integrity
tools do not mutate the update ledger. If external storage changes invalidate
the ledger, reset the affected scopes explicitly before updating.

## Fresh-lake schema contract

Fresh lakes create the interruption-safe ledger schema directly. The metadata
database stores an explicit schema version. Opening an unversioned or older
database fails with a clear incompatibility error; the library never migrates,
repairs, backs up, or rewrites an old lake automatically. Stop all workers and
create a fresh lake root before downloading again.
