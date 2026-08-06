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
partition counts, planning time, skipped no-op partitions, and peak in-flight
calls. Fetch time is cumulative across parallel jobs and may exceed elapsed
wall-clock time.

Complete provider request parameters remain available through the admin
facade. Metadata schema v3 stores their JSON as zlib-compressed SQLite blobs
and decodes them transparently when read.

Use `validate_manifest` for a fast metadata/file comparison. For a complete
contract check, call
`lake.admin.validate_dataset("daily", source="tushare", deep=True)`. The deep
validator also reads every canonical Parquet partition and checks its hash,
schema, primary-key columns, null and duplicate keys, and physical partition
ownership. Orphan Parquet files are reported but are never adopted implicitly.

Confirmed corrupt partitions can be moved out of the canonical lake with
`lake.admin.quarantine_partitions(...)`. Pass `confirm=True`, a reason, and an
optional repair ID. The operation uses atomic same-lake moves, updates the
manifest in one transaction, rolls both changes back on failure, and writes a
recovery journal under
`.health-repair-quarantine/<repair-id>/<source>/<dataset>/journal.json`.
Quarantined files are retained until an operator removes them. These integrity
APIs deliberately do not guess which provider scopes to retry; the application
layer must reset the affected scopes before its normal update workflow.

Use `rebuild_manifest` only after an intentional external storage repair. It
adopts the files it finds and therefore is not part of automatic health repair.

## Fresh-lake schema contract

Fresh lakes create metadata schema v3 directly, including canonical dataset
schemas and compressed API audit payloads. Opening an unversioned or older
database fails with a clear incompatibility error; the library never migrates,
repairs, backs up, or rewrites an old lake automatically. Stop all workers,
delete or archive the old lake, and create a fresh lake root before downloading
again.
