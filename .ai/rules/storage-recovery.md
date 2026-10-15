# Storage, PIT and recovery

## Authority and hard cut

Data owns Raw, neutral typed DataItems, categories, versions, coverage and frozen
inputs. Data imports no other BagelQuant package. Workbench supplies market
semantics, authored definitions and orchestration through public APIs; Core and
BT own their numerical and financial artifacts.

Schema 9 requires a fresh lake. Reject other schemas before writes; do not add
migration readers, aliases, orphan adoption or provider-backed recovery.
`data_meta_path` is the sole Data SQLite metadata authority. `lake_path` contains
the sole original immutable Parquet evidence, partitioned by year/month.

Schema 9 stores Arrow IPC schemas once by digest. Frozen receipts keep small
headers, requests, alias summaries and immutable Source references. Shared
definitions, General headers, original segment/check snapshots and sequence
blocks are content addressed and shared across receipts. Live ledger projections
never replace a frozen original header after indexing or metadata changes. Provider
commit request batches share templates and per-scope deltas. No schema 8 reader
or automatic migration exists.
An identical thin receipt digest reuses its original receipt ID. Different
cutoffs, declarations or retained boundaries remain distinct immutable evidence;
only selected semantic identities govern numerical reuse.

`inputs.get/describe/request_identity` do not expand historical evidence. The
public `FrozenInputReceipt.evidence` is an explicit lazy Mapping: field/sequence
access consumes only the corresponding registered metadata. Ordinary unchanged
Source checks use alias-local publication evidence and, for Items, current
whole-range completion flags and the live declaration dependency closure.
Unknown, partial or changed authority takes the conservative metadata path.

`register_selection_set` validates owner descriptors once and publishes immutable
Source-bound shared references. `describe_selection_set` validates only the set
header and original Source; it does not claim currentness or byte integrity.
`read_selection_set(keys=...)` resolves exact descriptor identities with bounded
membership paths. An all-key read is explicit preparation/audit. `inputs.verify`
audits Source blocks, selection membership/proofs and original bytes. Consumers
retain set references rather than another copy of its descriptor bodies.
`inputs.selection_descriptor_identity` is the sole stable key: it binds all
original proof/scope fields under `selection.handle.v1`, omitting only the
enclosing `source` and `requested_cutoff`. Shared bodies omit those same fields;
the immutable set Source and retained explicit `as_of` reconstruct the exact
requested boundary through the existing request contract. Canonical cutoff,
original `proof_source`, fields, coordinates, strictness and evidence remain
bound. Different explicit `as_of` values retain distinct keys. Member indexes
belong to immutable 64-member chunks, not to Source-bound set IDs. Source removal
retains shared indexes like other shared metadata; it never deletes another
Source's evidence. Earlier set formats are not read or automatically migrated.

Each actual content delta is one immutable Parquet segment. Never rewrite/copy
an entire historical month for an append. SQLite stores file references, exact
file-byte checksums, typed schema, row counts, bounds and compact per-field change
summaries. Initial/all-new coordinate fields share one original-file token and
common bounds; actual revisions retain affected fields and semantic timing.
General field tokens bind unique date/asset coordinates; nonunique unkeyed
matrices retain a conservative whole-file token rather than false marginal
column equality. Explicit audits verify file-bound tokens or admitted typed
General projections against their originals. It stores no data body, per-row integrity/index payload or duplicate
Arrow recovery copy. Typed observation key tokens are temporary read/commit
projections; `_record_id` and `_payload_hash` are not stored in original Parquet.

A first verification of unknown baseline timing may publish an immutable natural
key visibility event (observation keys, original creator commit and first known
availability). Its small SQLite header binds the exact file bytes, schema and
bounds. Already known identical rechecks create no content or visibility file.
An earlier or repeated identical check never backdates that first-known event.
These are causal information events, not another content/integrity index.
An equal DataItem response from a newer producer Source may retain a compact
check scope: original commit, complete natural-key-set identity and row count.
That metadata rebinds the creator only for a fully matching original file scope;
it creates no new value body, row proof or whole-range completion.

Prepare files before publishing metadata. Only committed segments are visible.
Publish schema, manifests, version identity and successful scope transitions
under one definition/parent/lease fence. Cancellation/failure preserves committed
partial work and the previous complete receipt. Every registered original is
immutable and retained for frozen replay; queries never adopt loose files.

## Completion flags and normal planning

`raw.completion_statuses` and `items.completion_statuses` batch-read small node
flags and their dependency metadata in one finite SQLite snapshot.
`items.completion_read_context(names=...)` shares those flags and subsequent
metadata reads in one operation-local read-only snapshot. Nested frozen input
read contexts share it, while competing Data writers may commit. Use this for
readiness planning; a later context observes later publication. It admits no
Data writes and does not fence consumer reference publication.
`items.completion_guard(names=...)` holds the Data publication fence through a
consumer’s finite reference CAS using those batch flags. It reads no input
values, freezes no Source and permits no kernels, providers or Data writes.
This guard requires writable admission; never substitute the read context for
an actual consumer publication fence.
The number of queries is fixed; work is proportional to nodes and declared edges, never data
rows. Each success flag binds definition, covered interval, checked target,
publication revision, input state, semantic version and result reference.
Raw and Item numerical definition hashes exclude only their top-level
`description`. Complete declarations, exports and immutable frozen receipt
digests retain that prose; arbitrary metadata and all other declaration fields
remain authoritative. Description edits preserve flags, dependency/range
identities and completed numerical history.

Advance flags only after successful whole requested range publication. Default
Raw acquisition uses the request ledger for interrupted retry; subset acquisitions
cannot certify the whole node. Beginning an admitted update marks it incomplete;
actual content/visibility publication advances its dataset revision. A retained
success flag cannot hide a pending, failed or newer partial publication. Scope
reset invalidates the complete flag. Do not release writer ownership based solely
on an expired heartbeat; explicit recovery verifies publisher exit first.

Wrap orchestration writes in `integrity.update_owner(owner_id)` so all Raw and
native Item leases share the same externally recoverable workflow identity.
The binding is execution-context local and applies to separate lake instances
with the same resolved metadata path. Same-owner nesting is permitted; replacing
an enclosing owner or supplying a conflicting explicit Raw owner fails before
publication. Independent caller threads must explicitly copy the context or
enter it; internal publication workers retain their enclosing dataset lease.
The context never adopts, clears or expires another publisher's lease. Only
after every publisher for an exact owner has exited may orchestration call
`integrity.abandon_update_owner(owner_id, reason=...)`. That public operation
retains successful scopes, flags, original bodies and frozen evidence, while
releasing only that owner's unfinished runs/scopes/leases. Legacy standalone
leases expose their exact `owner_id` through `active_update_leases`; recovery
requires external owner/process evidence, not date or heartbeat inference.

Covered ranges must contain the requested start/end. Union overlapping or
adjacent successful windows only when the earlier publication revision and
direct dependency state remain current and no unfinished update exists. Changed
authority requires the new completion itself to cover the whole requested span.
Disjoint windows do not certify the hole between them. An unchanged older branch
cannot replace the latest checked target. Empty successful results and
nontrading dates can complete without an invented content version. Keep checked
target separate from last nonempty date and covered observation end. External
`complete_at` or `replace_range(..., available_date=...)` sets the checked target
to that acquisition cutoff while retaining the declared covered interval.
Completion/acquisition dates and internally generated producer timestamps do
not create equal-value content copies. Explicit historical version dates
supplied to `ingest` or a publication frame remain semantic PIT events.

Normal prerequisites/currentness read metadata only: no provider calls, original
Parquet decode/rehash, producer execution or implicit proof preparation. Generic
General inventory completeness uses the success flag, not historical response or
per-row proof scans. Historical General initialization has no verified current
flag; a subsequent real observation creates it. Enabling preservation of earlier
nonempty inventory reopens a latest response that contained empty variants.
Explicit freshness does not certify original-byte integrity.

## Range identities and incremental calculation

Frozen receipts pin declarations, typed schema, original segment/visibility
headers, cutoff, committed/check ceilings and dependency edges. Receipt IDs,
global counters and resource settings do not participate in range content tokens.
Historical replay never substitutes current files or provider bytes.

`inputs.range_identity` and ordinary `selection_identity`/descriptors return
metadata range tokens. Disjoint date ranges, unconsumed field changes and proven
nonintersecting date/asset change bounds preserve identity. Bounds conservatively
invalidate an intersecting change whose exact consumed equality is unavailable.
Changing optional summaries must never change ordinary range identity.

Explicit `prepare_coordinate_selector` stores only a caller-supplied finite
Domain mask, without reading source rows. `prepare_range_selections` stores small
metadata scope summaries during Snapshot preparation. Unchanged readiness reuses
those headers without decoding the selector. Compatible later scopes retain the
original summary Source; original receipts remain immutable.

Explicit `index_selection(s)` may consume/audit bounded original Parquet to create
small exact consumed-value summaries. They are a stronger equality proof, not a
second ordinary identity authority. `selection_equivalent(summaries_only=True)`
compares only registered exact summaries when metadata cannot prove equality.
No per-batch row/scalar index or automatic full-history maintenance exists.

DataItem producer policies declare affected output intervals and read windows.
Append computes new outputs; bounded repairs reuse unaffected history. Newly
visible original segments use registered date bounds and availability delay to
bound impact. First-visibility changes begin at the actual information
availability boundary, not the earlier observation date. Unknown or full-history impact is actionable, never silent replay.
Completed-range authority and pending visibility publish with the success flag;
row-only ingestion cannot create a complete-range certificate.

Same-date initialization uses the existing Item initialization/build ledger with
one pinned original Source, definition and immutable runtime causal policy.
Private unit certificates use `unit_success` and never publish a node flag or
appear as a public whole-range completion. Only the final successful whole
request publishes that authority; cancelled/failed runs retain their committed
units and original Source. Resume subtracts completed observation ranges before
choosing future windows, so different resource/window settings do not rerun
certified history. Reuse checks later same-Item commits, visibility events and
verified empty-range checks against the unit's original output bounds. Unknown
or overlapping output authority requeues the range; disjoint units remain reusable.
Repeat that metadata check at final publication under the writer/transaction
fence. A Source changed during the operation or pause retains its
historical result but cannot publish a current-input flag. No new storage schema
or Workbench lifecycle is introduced.

`items.initialize_many` interleaves independent ready Items by bounded observation
window; `initialize` delegates. Same-date/no-positive-history producers and native
pointwise transforms are eligible. Default units stop at month boundaries;
metadata-admitted tiny whole ranges can coalesce without bypassing numerical
byte checks. Explicit smaller windows stay bounded, and oversized units may split
further under caller admission. Proven baseline publication uses its byte target
without requiring an optional row-count cap; retain individual row timing and
chronological verified rechecks. Unsupported producers retain their whole input
contract and must fit the explicit buffer. Windowing keeps the original
information cutoff, all causal revision boundaries, withdrawals and pending
visibility. Item dependencies must complete before a ready group is admitted.
`items.update_many` uses the same ordinary causal per-Item update path with shared
bounded preparation, never a second incremental planner or completion authority.
One coordinator metadata connection can serve the ready update group without
retaining a transaction or snapshot across Items. Only the outermost `connect`
owns commit/rollback; nested helpers cannot commit an enclosing transaction.
An atomic publication fence may borrow an idle session connection, while active
transaction nesting and read-view upgrades remain rejected. Release every
transaction promptly so later reads observe other writers' committed changes.

Logical Raw field selections remain in each original Source. Original aliases
with identical immutable header, physical request/window and cutoff can share
temporary keys/visibility frames in a disposable byte-bounded preparation context;
no persistent body, row index, proof authority or global cache is created. The
cache is charged inside the caller buffer and expires after its admitted window
(or bounded update operation). Unrelated global counters cannot alter that bound
alias authority. Declared payload projections occur only after complete General
snapshot resolution. Original overcaptured native requests may narrow to their
unchanged declared fields without rewriting receipts or running history; ordinary
timing/baseline/change proofs remain required. Selected-field skip authority
requires registered unchanged value/timing and known nonbaseline evidence.

Read admission uses original footer/schema metadata and a pre-collection row cap
with post-collection byte validation. It must not collect unsupported full history
and only then reject the caller budget. Native admission is an execution-only
caller context-manager callback; external producers default to mixed/exclusive
work. With that callback, mixed revision evaluations remain serial to avoid
cross-thread nested exclusive acquisition. Data never derives hardware policy.

## PIT and typed consumption

Observation date, availability date, ingestion/version identity, content commit
and successful check are separate. Daily history selects each observation's
then-visible version; explicit as_of is a fixed information cutoff. General is a
complete selected event, including a genuinely empty event, not a daily grid.

Bounded observation reads apply `source_time` predicates to original Parquet
scans before computing temporary typed natural-key identities. Retained visibility
events receive the same observation predicates before key encoding and joining.
Validate the whole original's physical schema and row count from its Parquet
footer before bounded consumption; selected row counts are not whole-file counts.
Keep availability lower bounds after latest-version resolution, preserve the
actual first-known date, and leave General event selection unchanged. Ordinary
reads do not audit file bytes; explicit verification still checks the complete
original. Unbounded internal reads retain their full-row contract.

Initialization is an unverified historical baseline. Strict reads exclude it
until a retained natural-key visibility event establishes actual later timing.
Never invent or backdate original publication. Preserve sparse membership, null
and withdrawal events, complete compound keys, schema evolution, categories,
nonfinite/signed floating values and deterministic version precedence.

`input_read_boundary` captures one committed/check ceiling and may tighten only.
`inputs.read_context` shares deeply immutable metadata in a same-thread finite
SQLite view. Caller objects supply only ID/digest. Publication guards hold the
writer fence only for finite metadata/reference CAS, never providers or kernels.
No frame, proof-validity cache or authority survives the operation context.

## Inspection, audits and backups

Read-only opens never initialize or recover. Ordinary WAL coordination may touch
SQLite sidecars. Default `DataLake.inspect` preserves originals/sidecars and
includes committed WAL through a stable private metadata snapshot. Runtime
inspection uses ordinary WAL coordination; do not ignore live WAL via immutable
main-file reads. Active rollback journals are refused without recovery.

`inputs.verify` explicitly rechecks every original file byte and typed structure,
including visibility events and all original dependency edges, with cancellation
and one caller-bounded pool. Multiple roots deduplicate originals only within the
invocation. Small range summaries are audited according to their proof kind;
exact summaries compare consumed values, metadata summaries compare original
registered headers whose files are separately audited.

There is no second data copy for local corruption repair. Missing/damaged original
segments or visibility files require checksum-verified backup restoration. Never
refetch providers as historical evidence. Passive scans report facts, protect
registered history, and never adopt or delete loose files. Explicit temporary
cleanup freezes and revalidates known unpublished atomic temporary files only.

## Explicit Item evidence removal

`integrity.item_removal_plan(names, discard_receipts=...)` previews an exact
caller inventory using metadata and file attributes, without reading bodies.
`integrity.remove_items(plan)` is explicit destructive maintenance, distinct
from `items.remove` archival and temporary cleanup. Require no active Data
writer/run/scope, including expired owners; process exit and external consumer
reference release belong to the caller. Bind logical and physical lake identity,
schema, exact scoped metadata, original file references and attributes. Verify
original checksums before metadata detachment. Reject stale/foreign/forged plans,
symbolic/shared links and unregistered target files rather than adopting them.

Retained declarations, creator/check/build/native completion evidence, frozen
requests and parent edges block removal. Discard only explicitly named whole
receipts in the target Item dependency closure; never delete shared upstream
sources or rewrite mixed immutable receipts. Core/BT/Workbench release their
owned external references before apply; Data cannot discover those stores.

Detach exact metadata atomically, keeping sequence/high-water state and every
unrelated record unchanged. Persist the immutable removal inventory/status under
a namespaced existing `data_meta_state` key, without schema migration. Then
unlink only those exact old files under writer leases. Interrupted cleanup can
repeat that receipt without recapturing a recreated name's generations. A forced
process exit retains its ownership lease; only verified owner recovery releases
it. Leave empty directories and retain shared optional selectors/upstream proofs.
Removal receipts contain file/operation metadata only, never row proof arrays.

Backups copy the metadata snapshot and all retained original content/visibility
files. Total metadata loss requires restoration of that trusted backup. Unverified
snapshot copies remain explicitly unverified; strict backup audits required
original evidence. Preserve historical receipts, lake identity and declarations.
