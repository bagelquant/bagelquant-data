# Integrity and recovery

```python
facts = lake.integrity.scan("daily", source="tushare", deep=True)
catalog_facts = lake.integrity.scan_many(source="tushare", deep=False)
plan = lake.integrity.repair_plan("daily", source="tushare")
restored = lake.integrity.repair(plan)
```

Scans report manifest/schema/key/hash/partition/recovery facts without mutation.
They do not adopt orphan files or call providers. Shallow scans compare registered
file inventories; deep scans verify exact original Parquet bytes and typed evidence.
Plans bind lake identity and committed manifest. Execution rejects stale or
foreign plans and holds the dataset's update lease during repair.

`data_meta_path` determines committed visibility. Publication prepares one
immutable Parquet segment and compact SQLite file/schema/change metadata, then
atomically publishes manifest, schema, commit and coverage. Prepared or
unregistered files are invisible. Every registered segment remains historical
authority; there is no SQLite Arrow body or historical per-row proof index.

Repair audits registered original segments. Missing or damaged original files
require a trusted backup, and repair reports that requirement explicitly.
Current provider responses and loose files cannot reconstruct authoritative
history. No automatic cleanup, adoption or schema migration exists.

Run history, failures, scopes, coverage and active leases are available through
`lake.integrity`; object summaries through `lake.raw.status/status_many` and
`lake.items.status`. Scope resets and abandoned-owner recovery are explicit
mutations, distinct from passive scans. Tests never use the real workspace data root.
`lake.integrity.update_scopes(dataset=..., source=..., status=...,
scope_keys=...)` applies all filters in the native ledger before returning
deterministically ordered rows. Keys may be a finite iterable; omission retains
the full inventory, and an empty iterable returns immediately without a query.
This read-only selection neither resets scopes nor changes currentness evidence.
`lake.items.status(name)["committed_definition_current"]` compares the current
physical declaration with the latest committed batch in every current manifest
partition. Registering a new producer revision alone does not make older stored
output current. This check uses committed metadata and does not scan Parquet.
`RawAPI.spec_from_mapping()` is also available from the package root for pure
declaration validation without creating a lake or metadata database.
Expired heartbeat is reported without releasing ownership. Only a caller that
has verified owner termination should call `abandon_update_owner`; publishing
also checks the still-owned run id, parent generation and current definition.

Use `with lake.integrity.update_owner(workflow_id):` around a workflow's Data
writes. Raw and native Item publishers, including separate lake instances for
the same metadata path, then record that owner in their existing lease rows.
The binding follows the calling execution context; independent caller threads
must enter it or explicitly copy the context. Internal publication workers
retain the enclosing lease. Same-owner nesting is allowed; conflicting owners
fail. After all of that owner's publishers have exited, the coordinator calls
`lake.integrity.abandon_update_owner(workflow_id, reason=...)`. Successful scopes,
flags, immutable originals and frozen receipts remain intact. The context does
not reclaim any lease automatically, and an expired heartbeat never establishes
process exit. An interrupted standalone lease can be recovered by its exact
reported owner only after the coordinator verifies its publisher has exited.

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

## Explicit removal of Item evidence

```python
preview = lake.integrity.item_removal_plan(["obsolete.model.exposures", "obsolete.model.carry"])
# The caller first releases every external reference to the discarded receipts.
plan = lake.integrity.item_removal_plan(
    preview["names"], discard_receipts=preview["required_receipts"],
)
if not plan["blockers"]:
    result = lake.integrity.remove_items(plan)
```

This explicit operation permanently removes only the named Items' declarations,
publications, visibility files, complete flags/builds and explicitly discarded
whole frozen receipts. `items.remove` continues to archive without deleting
history. The plan uses metadata and file attributes; application verifies exact
original checksums. It is bound to the logical/physical lake, schema, scoped
metadata and owned file inventory. Stale or foreign plans, active Data owners
(including expired leases), retained dependencies, creator/build/output receipt
references, parent receipts and unknown/shared/linked files reject removal.

`required_receipts` includes frozen selections of the target Items and their
descendants. Shared upstream Raw/Item sources cannot be discarded. A mixed
receipt must be explicitly discarded whole or retained, which blocks deletion;
retained immutable receipts are never rewritten. External Core/BT/application
references cannot be discovered by Data: their owners must release them first.
The API accepts exact names, not wildcard or whole-root deletion.

Metadata detaches atomically before file cleanup. A bounded operation receipt in
the existing metadata state records the exact old inventory, so repeating the
same plan resumes interrupted cleanup without deleting new generations under
a recreated name. Normal errors release its leases; after a forced process exit,
the caller must verify publisher exit before exact owner abandonment. Unrelated
rows/originals, shared selectors/summaries and global sequence state remain
unchanged. Empty directories remain; unknown files are reported as blockers.

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
new paths. Backup and restore reject existing destinations. Frozen inputs remain in SQLite; every retained value and visibility segment is
backed up as its exact original file.

`integrity.snapshot(data_meta_path=..., lake_path=...)` creates a consistent,
independent copy for isolated work without scanning all historical proof bytes.
Its report explicitly returns `valid=None`, `verification="unverified"` and
`generation_count`; it is not a verified archival backup. Verify the required
original frozen input receipts in the copied lake before computation.
`generation_count` counts manifest generations as before. `immutable_file_count`
counts all independently copied registered files, including committed historical
segments outside the manifest and visibility events; `visibility_count` counts
the visibility subset. Both snapshot and verified backup derive that complete
inventory from copied metadata, reject missing or escaping references, and ignore
unregistered loose files. Verified backup audits all committed originals and
registered visibility events, including those without retained frozen Sources.

Empty scalar Item ranges use the same canonical empty proof in ordinary
planning and full verification. Their metadata summaries retain the original
Source dependencies. Verification still checks every retained original byte and
captured bound, including files outside the empty selected range; a damaged
original or incorrect summary remains an error.

The snapshot checks source main/WAL/journal signatures around copying and
rejects active rollback journals. It includes committed WAL evidence without
opening or checkpointing original metadata; recovery and binding relocation
affect only the destination. macOS uses independent copy-on-write filesystem
clones where supported; other filesystems/platforms fall back to independent
file copies. All committed immutable value and visibility segments are copied; frozen
receipts and compact headers remain in SQLite. No symlinks/hardlinks connect original and copy.
Destinations must be new and must not overlap original metadata/sidecars/lake;
failed-copy cleanup removes only those newly created destinations. `backup` and
`verify_backup` retain full verification of every retained proof.

Direct `inputs.verify(source)` audits share one finite read-only SQLite snapshot
while traversing lazy retained evidence. A later direct call rechecks original
bytes and metadata; successful proofs do not survive the call. Every retained
full-commit seal is checked, with one shared retained-batch membership set per
alias rather than rereading that alias's full history for each seal.
Lazy seal evidence uses the original materialized canonical JSON encoding and
retains its exact saved digest.
Full audits reuse one validated selection-set inventory within the unchanged
read view while checking every member, descriptor and original proof. Public
keyed reads and later audits prepare fresh inventories; no inventory cache
survives an audit.
Empty original audit projections validate key selection and temporal conversion
without hashing unused virtual record IDs for primitive scalar keys when both
selection and normalization retain the original row count and nonempty columns. Complex or
malformed key declarations use the ordinary key derivation path. Captured-bound
auditing alone may also omit unused IDs for its present date axes when all
original columns are primitive or temporal and the same key/normalization/shape
guards pass. Ordinary nonempty projections retain their existing behavior and
signature. Audits still verify every original byte, full typed Arrow
structure, schema, row count, bounds and field-change evidence under the same
cancellation and buffer limits.

Temporary typed-row-v1 hashing reuses lazily encoded String tokens within each
call. Columns share one conservative reserve of at most 4 MiB, further bounded
by the selected frame's estimated bytes; a column may use the remaining shared
reserve. Each cached token debits `512 + 2 * len(encoded_token)` bytes. Exhausted
or oversized tokens use the same uncached encoder without retaining another
entry. There is no unique-value scan, cross-call cache, new resource allowance,
or skipped record-key/content/attestation hash. Token bytes, typed headers,
temporal precision, null/nonfinite handling and all persisted identities remain
unchanged. This reserve estimate is not a hard process RSS guarantee.

`raw_categories(source).update(id, name=..., parent_id=...)` and the item category API atomically rename/move a folder. Cycles, cross-provider parents and nonempty deletion are rejected. Classification changes do not alter data paths, versions or receipts.

## Complete filesystem inventory

`lake.integrity.storage_inventory(within_root=None)` stats the metadata database,
its WAL/SHM/journal sidecars and every entry under the lake, including hidden,
unknown and temporary files. It reports owner paths, per-entry logical bytes,
allocated bytes (`st_blocks * 512`), file kind and device/inode, plus unique-inode
totals. Directory logical bytes are zero; symbolic links count their own object,
not target contents. `within_root` restricts traversal to selected root paths.
Consumers can merge this public inventory with other owners and attribute hard
links once by lexical path. Errors other than disappearing files fail the scan.
It never reads numerical bodies or initializes or repairs storage.

`storage_usage()` still supplies Data's semantic current/history/temporary counts.
The new inventory does not expand `temporary_cleanup_plan/cleanup_temporary`:
unknown files, retained originals and recovery evidence stay protected.
