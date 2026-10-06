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
`lake.items.status(name)["committed_definition_current"]` compares the current
physical declaration with the latest committed batch in every current manifest
partition. Registering a new producer revision alone does not make older stored
output current. This check uses committed metadata and does not scan Parquet.
`RawAPI.spec_from_mapping()` is also available from the package root for pure
declaration validation without creating a lake or metadata database.
Expired heartbeat is reported without releasing ownership. Only a caller that
has verified owner termination should call `abandon_update_owner`; publishing
also checks the still-owned run id, parent generation and current definition.

## Storage usage and temporary cleanup

```python
usage = lake.integrity.storage_usage()
plan = lake.integrity.temporary_cleanup_plan()
result = lake.integrity.cleanup_temporary(plan)
```

Usage inventories actual regular files beneath the configured lake and the Data
metadata database plus SQLite sidecars. Physical hard links are counted once.
It separates current generations, retained historical generations, metadata and
recovery, rejected evidence, known atomic temporary files, and unknown files.
`temporary_bytes` includes protected active files; `reclaimable_temporary_bytes`
counts only eligible files. The active source/Raw/DataItem counts are separate;
available counts mean active objects with nonempty committed manifests. These
counts do not establish a global research Available Date.

Cleanup plans contain lake identity, a deterministic plan hash, exact relative
paths, file identities and content hashes. Only Data's recognized atomic-write
temporary names in registered monthly dataset directories can qualify. Active
writers, historical/committed generations, metadata/recovery, rejected evidence,
unknown files, symbolic links and shared hard links are protected. Execution
acquires the selected dataset writer leases and revalidates every candidate
before deleting any file. Changed files fail as a stale plan. Repeating an
already executed plan reports already-missing candidates without broadening its
scope. Browsing usage and planning cleanup never delete or release ownership.

## Portable declaration batches

```python
payload = source.catalog.export_declarations()
plan = destination.catalog.plan_declaration_batch(payload)
if plan["valid"]:
    receipt = destination.catalog.apply_declaration_batch(
        plan, request_id="caller-owned-transfer-id",
        expected_revision=plan["expected_revision"],
    )
    retained = destination.catalog.declaration_batch_receipt(receipt["request_id"])
    verified = destination.catalog.verify_declaration_batch_receipt(receipt)
```

The JSON snapshot schema is `bagelquant.data.declarations.v1`. It contains source
descriptors (`name`, `adapter`, `enabled`, `active`), Raw records (`spec`, `enabled`,
`active`), DataItem records (`spec`, `active`), categories
(`id`, `kind`, `source`, `name`, `parent_id`), assignments
(`kind`, `source`, `object_key`, `category_id`), and a semantic SHA256 `revision`.
Sources exclude all configuration and credentials. Declarations contain no
provider/runtime objects, executable producers, data bytes, commits or results.
Archived declarations are retained. Raw natural identity remains `(source,name)`;
DataItem identity remains `name`. Portable research-object UUIDs and governance
remain caller application metadata.

Planning checks the complete prospective union, including forward DataItem
dependencies, cycles, category parents/namespaces and active assignments. Existing
identical records are skipped; conflicting natural identities are reported and
never overwritten. Raw calendar/fan-out dependencies must exist in the union.
Category identities and their independent provider/item namespaces are preserved.
`valid`, `conflicts`, `issues` and per-section `summary` describe acceptance.

Applying a plan rechecks its bound lake and catalog revision, uses the same
registration helpers as individual APIs, and publishes all declarations and the
immutable request receipt in one Data SQLite transaction. A failure rolls back
the complete batch. A repeated `request_id` and exact plan returns its original
receipt; reusing that ID for another plan fails. Import registers descriptors
only: callers still register provider and producer implementations at runtime.
Receipt verification reports retained evidence `valid` separately from equality
of its requested declarations with today's catalog (`current`). Additional
unrelated declarations do not make its requested declarations stale.

`integrity.backup(data_meta_path=..., lake_path=...)` exports a consistent SQLite
snapshot and its committed immutable files into new caller paths; it returns
verified owned file hashes. `integrity.verify_backup()` validates the bundle.
`DataLake.restore(backup_data_meta_path=..., backup_lake_path=...,
data_meta_path=..., lake_path=...)` restores a verified same-schema bundle into
new paths. Backup and restore reject existing destinations. Complete history
and frozen inputs remain in the single metadata file; older physical files are
reconstructed from registered Arrow batches when needed.

`raw_categories(source).update(id, name=..., parent_id=...)` and the item category API atomically rename/move a folder. Cycles, cross-provider parents and nonempty deletion are rejected. Classification changes do not alter data paths, versions or receipts.
