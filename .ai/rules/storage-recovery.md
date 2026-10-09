# Storage, PIT and recovery

## Implemented authority

- Publication baseline timing short-circuits after positive evidence and memoizes
  final booleans by exact effective cutoff within the verified owner/receipt
  context only. Negative results require all aliases. Reserve at most one quarter
  of the explicit buffer budget at a conservative 1 KiB per entry, capped at
  1024 entries; evict oldest entries and clear on failure/exit. Never cache
  input frames, assume chronological monotonicity, or skip later byte checks.
  Exact unchanged Item builds pass caller execution limits to byte verification.
  Timing-only reads tighten the cutoff to the latest unresolved date and use
  checksummed captured scoped by-date `min_available` to skip future physical
  batches; attestation copies are bounded to that cutoff. General snapshots,
  unknown/legacy bounds and ordinary value reads retain their original behavior.
  Empty Item recursion uses its own effective cutoff. Every original retained
  batch remains in the receipt and integrity verification, including future ones.
  A timing-only positive proof may avoid frames when every captured scoped batch
  has a uniform baseline flag, all known observation bounds lie entirely inside
  the request, and a nonempty batch proves physical availability by the cutoff.
  Require no eligible nonbaseline witness; validate full seals. Strict/history,
  general, legacy, unknown/mixed or partially overlapping evidence falls back.
  Never infer False from this proof or override a caller-supplied frame.
- Writable initialization creates the derived `version_batches_metadata`
  covering index and analyzes `version_batches` once in the same transaction.
  Recheck existence under the writer lock. Repeat opens do not rebuild statistics.
  Index-free schema-seven read-only opens remain valid and never create the index;
  table layout, payloads, receipts, hashes and PIT evidence remain unchanged.

- `items.publication` preflights related `ItemPublication` outputs and verifies the
  shared input receipt once on context entry, then uses the ordinary historical
  ingestion/range-replacement path. Its private verification authority expires
  on exit or group failure. Require serial groups on the context-owning thread and check cancellation before
  writes and successful exit. Preserve per-table commits on failure/cancellation and do not claim
  cross-table atomicity. Every later context rechecks original proof bytes.
- `inputs.verify` accepts explicit `ExecutionOptions`; one bounded pool verifies
  original IPC checksums and structural/type validity using bounded decompression
  and temporary mapping. Defaults remain serial, no hardware detection, no
  persistent validity cache. Join the pool before serial metadata publication.

- Data 0.7 owns Raw and typed neutral DataItems, independent category trees,
  versions, coverage, frozen input receipts and registered local recovery.
  Public facades: catalog/raw/items/integrity/inputs; pure exploration returns
  statistics and Polars tables. Both data_meta_path and lake_path are explicit.
- Data imports no Core/BT/Workbench. Core numerical and BT result artifacts have
  their own owners; Workbench stores authored closure and backend receipt references.
- Schema 7 rejects old/unversioned databases before writes. No aliases, migrations,
  orphan adoption, automatic cleanup or provider-backed historical recovery.
  Real data/service cutover is a later separately authorized operation.
- `DataLake.inspect` is the public schema/lake-binding readiness query: no original
  files, directories or SQLite sidecars are changed. Include committed live WAL
  using a stable owner-internal temporary metadata snapshot; never infer readiness
  from an immutable main-file read that ignores WAL.
  Refuse a nonzero rollback-journal header without recovery; it may protect
  uncommitted main-file pages. Include journal identity in stability checks.
  An invalidated zero-header PERSIST journal does not block inspection.
- Explicit `DataLake.inspect(runtime=True)` and `DataLake.open(runtime=True)`
  use ordinary read-only WAL coordination for compatibility checks without
  copying metadata. Runtime callers accept SQLite WAL/SHM coordination. Default
  inspections/opens retain strict unchanged-source compatibility preflight.

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

Recovery compression is completed before acquiring SQLite write ownership;
prepared-status/hash checks and evidence insertion stay transactional. Replay
restores the last registered by-date batch schema after concatenation, preserving
entirely null declared scalar types and original partition hashes.

General recovery retains the union of full-snapshot batch schemas, including
columns omitted by later snapshots; its last batch is not a full-partition schema.

Frozen-input currentness avoids reading Item values when the covering successful
build proof is among all captured parents and every captured batch/check/build
parent is recursively current. Any stale parent falls back to ordinary
cutoff/window selection; an unselected historical parent does not invalidate the
result. Missing build proofs cannot establish currentness. Integrity verification
still checks every original retained batch independently on every later call.
Neither optimization changes receipt identity or PIT selection.

`inputs.is_current` also accepts a nonempty finite sequence of original frozen
receipts. Validate each supplied identity/digest before memo reuse; check every
root in one SQLite read view with its own immutable request/cutoff and retained
boundary. Share recursive boolean results only within that invocation, retaining
cycle checks and exact selected-parent fallback for stale historical parents.
Return whether all roots are current without short-circuiting root checks; a
later invocation rereads current evidence. Do not create aggregate receipts,
cache frames, persist validity, or replace independent full byte verification.

Typed-row-v1 payload hashing retains exact persisted bytes and schema identity. Scalar token encoding avoids repeated JSON work; string tokens share a conservative 4 MiB reserve, at most 1 MiB per column and no more than the input-frame byte estimate. Cache fill is lazy from the existing bounded row iterator; no full unique-value list, unbounded string cache, additional worker pool or identity migration. Nested/decimal/binary values retain recursive canonical JSON; temporal native precision, signed zero, NaN/null and infinity remain distinct.

Identical ordered reconciliation fields with identical schema may bypass revision hash joins. Shape, ordering, null/value/availability/baseline differences fall back to ordinary reconciliation. Final withdrawal checks first project time/asset keys; exact keys skip the join and full old payloads are read only for actual withdrawals. Tombstones and frozen historical evidence remain unchanged.

Unchanged-content witnesses stream only record identity, payload hash, original commit and availability through the bounded Polars row iterator into SQLite. Keep every witness and original insertion order in the same header/publication transaction; iteration or constraint failures roll back the whole transaction. Do not materialize full-panel dictionary/parameter lists, split transactions or relax WAL/FULL durability to accelerate attestations.

Record-check readers explicitly filter the small dataset/check headers before indexed witness lookup. The joined header ID defines the same ordering as check_id, avoiding a full-record sort. Unrelated datasets must not add per-record work to frozen capture or ordinary metadata snapshots. Preserve selected rows, cutoffs, proof hashes and historical receipts; do not add a schema migration or drop stored evidence.

Large complete by-date baseline attestations may use an immutable content-addressed full-commit seal. Prove exact unique (record ID, payload hash, original commit) equality against every original registered recovery batch, with one uniform availability date, using bounded external sorting. Counts alone never establish completeness. Retain all original SQL witnesses/headers and batch identities. Freeze embeds the checked seal; read-only operations never persist seals. Validate checksum and exact header/commit/batch binding before cache use. Partial/mixed or general checks retain the ordinary path; oversized unsupported inline evidence fails explicitly before materialization. Legacy receipts recapture their original representation for currentness; changing representation alone does not invalidate them. Replay preserves header eligibility, check/commit ceilings, ingestion cutoff, availability max, attestation ID and parent receipts. Proven per-batch observation bounds prune window reads only; frozen evidence and verification still retain/recheck every original batch on each call. Temporary sort connections/files are closed and removed.

DataItems.update accepts explicit input_windows keyed by declared input alias, mapping to inclusive observation (start,end). Validate aliases, date order and declared start/end plus Raw observation limits before recursion/publication; never infer windows for arbitrary producers. Pin overrides in the actual frozen requests without modifying the DataItem declaration or its hash. Omitted inputs retain their declared scope. General Raw snapshots are not windowed. Exact successful reuse checks the same definition/dependency/range identity before frame construction and still verifies every retained batch byte.

New explicit-window by-date freezes mark scoped_batches and use existing immutable version_batches observation/availability bounds to capture potentially selected batches. Retain unknown bounds, late revisions of in-window observations, related checks, overlapping empty item_range proofs and unknown scopes. Full-commit seals retain every original proof batch reference. Legacy receipts recapture their original broad representation; bounds/markers do not rewrite their bytes or hashes. Reads use matching immutable (commit,partition,hash) bounds to skip impossible batches; general initialization baselines remain complete. Shared-commit checks with unknown record-window membership conservatively invalidate; no unsafe currentness claim. No schema migration, new batch store or implicit rolling/lookback clipping.

Frozen window pruning uses only bounds captured in that checksummed receipt or its exact full-commit seal. Legacy receipts keep their original broad batch read; never consult live batch summaries to alter their replay. Subsequent summary corruption cannot change either legacy or scoped frozen read while retained batch verification still passes. Scoped empty Item currentness ignores unrelated first physical schema publication only when no batch can be selected; retain captured schema bytes, typed declaration and empty dependency proof, and invalidate for relevant content/dependency changes.

`inputs.verify` also accepts a nonempty finite sequence of original frozen
receipts. Each root identity/digest and every dependency edge are checked;
shared parents and original batches are checked once within this invocation.
The multi-root report lists original root IDs/digests and aggregate counts.
There is no new aggregate receipt or cross-call validity cache; a later call
rechecks bytes. Currentness remains a separate check.
