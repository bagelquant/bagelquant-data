# Integrity and recovery

```python
facts = lake.integrity.scan("daily", source="tushare", deep=True)
catalog_facts = lake.integrity.scan_many(source="tushare", deep=False)
plan = lake.integrity.repair_plan("daily", source="tushare")
restored = lake.integrity.repair(plan)
```

Scans report manifest/schema/key/hash/partition/recovery facts without mutation.
They do not adopt orphan files or call providers. Shallow scans compare registered
file inventories; deep scans verify content and registered Arrow evidence.
Plans bind lake identity and committed manifest. Execution rejects stale or
foreign plans and holds the dataset's update lease during repair.

`data_meta_path` alone determines committed visibility. Publication prepares
compressed Arrow evidence in that SQLite and immutable Parquet files, then
atomically publishes manifest, schema, commit and coverage. Prepared/unregistered
files are invisible; retained older registered generations are historical evidence.

Local repair reproduces registered original hashes and identities. Verified
recovery batches restore damaged Parquet. Verified committed Parquet can restore
recovery payloads only when every original batch hash/schema is reproduced.
If evidence is insufficient, repair fails explicitly. A completely lost metadata
database requires backup restoration; loose files or current provider bytes
cannot reconstruct authoritative history. No automatic cleanup/quarantine/
manifest adoption or metadata migration exists.

Run history, failures, scopes, coverage and active leases are available through
`lake.integrity`; object summaries through `lake.raw.status/status_many` and
`lake.items.status`. Scope resets and abandoned-owner recovery are explicit
mutations, distinct from passive scans. Tests never use the real workspace data root.
Expired heartbeat is reported without releasing ownership. Only a caller that
has verified owner termination should call `abandon_update_owner`; publishing
also checks the still-owned run id, parent generation and current definition.

`integrity.backup(data_meta_path=..., lake_path=...)` exports a consistent SQLite
snapshot and its committed immutable files into new caller paths; it returns
verified owned file hashes. `integrity.verify_backup()` validates the bundle.
`DataLake.restore(backup_data_meta_path=..., backup_lake_path=...,
data_meta_path=..., lake_path=...)` restores a verified same-schema bundle into
new paths. Backup and restore reject existing destinations. Complete history
and frozen inputs remain in the single metadata file; older physical files are
reconstructed from registered Arrow batches when needed.
