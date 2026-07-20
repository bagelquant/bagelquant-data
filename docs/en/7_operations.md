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

Use `reset_dataset_update_coverage` for a full dataset recovery. It refuses to
run while matching writer leases or running scopes exist, preserves canonical
files and audit history, and can clear provider checks so the next explicit
update rebuilds coverage from the requested start.

Update reports include elapsed, fetch, commit, and metadata timings, commit and
partition counts, and peak in-flight calls. Fetch time is cumulative across
parallel jobs and may exceed elapsed wall-clock time.

Use `rebuild_manifest` after repairing local Parquet files and
`validate_manifest` to compare metadata with stored files. These integrity
tools do not mutate the update ledger. If external storage changes invalidate
the ledger, reset the affected scopes explicitly before updating.

## Existing-lake migration

Existing audit-based lakes require a one-time ledger bootstrap. The migration
backs up `metadata/lake.db`, seeds daily success only from physically committed
dates, marks unverifiable dates pending, and conservatively marks every asset
pending for one full re-fetch. Corrupt legacy coverage is never trusted.

The bootstrap is resumable and must complete before normal updates. After it
finishes, the legacy pending-job, coverage, and audit-watermark tables are
removed. Fresh lakes initialize ledger version 1 automatically.
