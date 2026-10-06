# Storage, PIT and recovery

## Implemented authority

- Data 0.7 owns Raw and typed neutral DataItems, independent category trees,
  versions, coverage, frozen input receipts and registered local recovery.
  Public facades: catalog/raw/items/integrity/inputs; pure exploration returns
  statistics and Polars tables. Both data_meta_path and lake_path are explicit.
- Data imports no Core/BT/Workbench. Core numerical and BT result artifacts have
  their own owners; Workbench stores authored closure and backend receipt references.
- Schema 6 rejects old/unversioned databases before writes. No aliases, migrations,
  orphan adoption, automatic cleanup or provider-backed historical recovery.
  Real data/service cutover is a later separately authorized operation.
- `DataLake.inspect` is the public schema/lake-binding readiness query: no original
  files, directories or SQLite sidecars are changed. Include committed live WAL
  using a stable owner-internal temporary metadata snapshot; never infer readiness
  from an immutable main-file read that ignores WAL.
  Refuse a nonzero rollback-journal header without recovery; it may protect
  uncommitted main-file pages. Include journal identity in stability checks.
  An invalidated zero-header PERSIST journal does not block inspection.

## Storage authority

- DataMetaStore is the sole Data SQLite authority for metadata, task state,
  schemas, manifests, versions, coverage, checks, freezes and compressed Arrow
  recovery batches. Do not create per-month SQLite or duplicate state stores.
- Raw and DataItems retain year/month partitioning using immutable Parquet
  generations. Prepare recovery batches/files, then atomically publish manifest,
  schema, commit and coverage in SQLite. Prepared evidence is invisible.
- Normal LazyFrames pin committed generations at creation; do not replace files
  used by admitted readers. Preserve older committed generations and histories.
- Lake file references are relative and verified against their configured roots.
  A read-only open creates no directories/database/tables and performs no recovery.
  Normal read-only queries retain SQLite WAL coordination and may create/update
  WAL/SHM sidecars. Use the inspector for readiness checks requiring zero changes.
- Canonical hashes cover schema, values, nulls and deterministic ordering.
  Preserve scalar and categorical types and committed batch schema.
- Category moves change only catalog organization. Unregistering requires no
  active dependencies, preserves histories/receipts and removes active assignment.
- Declaration batches export non-secret JSON, preserve category identities and
  validate the prospective union. One Data SQLite transaction uses the individual
  registration helpers and publishes an immutable caller request receipt; stale
  plans and natural-key conflicts never overwrite existing declarations. Imported
  descriptors never instantiate providers/producers or acquire data. Application
  research UUIDs and governance remain Workbench responsibilities.

## Point-in-time and input evidence

- Daily history selects each observation's then-visible version. Fixed as_of
  selection is explicit and distinct from observation windows and commit upper
  bounds. General is a complete selected snapshot, not an implicit daily grid.
- Distinguish panel date, observation date, availability date, ingestion/revision
  identity, content commit and availability-check evidence. History initialization
  is an unverified baseline; strict reads exclude it until later verified evidence
  establishes a later availability. Never invent earlier publication timing.
- Identical later acquisition preserves availability evidence without duplicate
  content generations. Freeze both commit and check evidence ceilings atomically.
- Inputs.freeze pins dependencies, definitions/schema, batch hashes, cutoff and
  upper bounds. Inputs.read/verify use exact registered evidence; later updates,
  unregistering and current-file damage never substitute current bytes.
- Nested/captured read boundaries may tighten and reject explicit expansion.
  Queries/verification never invoke providers or submit updates.
- DataItem transformations validate unique (time,asset_id); null is default
  missingness. Ffill requires a finite supplied period and explicit coordinates,
  follows availability and stops at null events. Unknown producer impact rebuilds
  conservatively, retaining producer revision and dependency receipt evidence.

## Integrity and recovery

- Scan reports facts only. Report orphan/unregistered files without adoption;
  retained registered generation histories are not orphans. No scan mutates data.
- Repair plans bind lake identity and current manifest. Repair holds writer
  ownership, reproduces original registered hashes/schema/identity, and rejects
  stale plans or unknown partitions. Cancellation retains already repaired work.
- Compressed batches may restore Parquet; complete verified Parquet may restore
  batch payloads only if every original batch hash is reproduced. Insufficient
  local evidence fails explicitly. Total metadata loss requires backup restoration.
- Never refetch providers to replace historical evidence, fabricate original
  availability, backdate new observations or silently release live ownership.
- Storage usage counts actual regular files with physical hard-link deduplication,
  including metadata/recovery and retained history. Temporary cleanup is explicit:
  freeze exact known atomic temporary files, acquire writer leases and revalidate
  all identities/hashes before deletion. Protect active files, registered history,
  rejected evidence, unknown files and symbolic/shared hard links. Repeated plans
  only report already-missing files; they never broaden the approved candidates.
