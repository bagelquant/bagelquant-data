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
Row queries decode immutable request blocks and schemas on their original query
connection, including an enclosing uncommitted writer. Do not reopen a connection
for each lazy descendant or substitute a separate read snapshot during decoding;
retain the original checksum and missing-evidence failures.
An identical thin receipt digest reuses its original receipt ID. Different
cutoffs, declarations or retained boundaries remain distinct immutable evidence;
only selected semantic identities govern numerical reuse.

`inputs.get/describe/request_identity` do not expand historical evidence. The
public `FrozenInputReceipt.evidence` is an explicit lazy Mapping: field/sequence
access consumes only the corresponding registered metadata. Ordinary unchanged
Source checks use alias-local publication evidence and, for Items, current
whole-range completion flags and the live declaration dependency closure.
Unknown, partial or changed authority takes the conservative metadata path.
A sibling requiring conservative checks does not discard another alias's exact
current completion, including a bounded empty range completed in adjacent groups.
Reuse the same checksum, captured state, coverage, dependency and pending-output
fences per alias; load the trusted compact Source header map once per read view.

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

An ordinary selection-set read keeps header admission and complete lazy
descriptor materialization in one finite read view, with bounded Inputs memos.
Validate the registered header before loading its claimed Source. Existing
Inputs readers and active writers retain their original connection and
uncommitted visibility. Registration shares one finite validation context across
its bounded groups, then closes that newly owned context before the original
writer transaction and Source recheck. No context or successful read survives
the operation; cancellation, corruption and buffer limits still fail closed.

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

Native initialization, bounded units and combined incremental ranges check
cancellation inside their completion transaction before and after certificate
publication. Cancellation rolls that transaction back while preserving earlier
committed rows and completed units; it never reports initialization success or
certifies an interrupted enclosing range.

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

`items.resume_update(name, build_id=..., start=..., end=...)` is explicit
original-bound recovery for an interrupted first build of the current numerical
definition. Bind the original partial publication, frozen Source requests and
cutoff, and caller-confirmed original bounds. Refuse any completed current
definition, an unproved success record spanning the whole requested range,
different partial Sources/bounds, stale inputs or initialization authority.
Run the existing producer/publication path without certifying partial rows;
retain all originals. Whole-range completion uses one finite Data writer
transaction with fresh definition/Source/original-build checks, output/check
CAS and cancellation checks. Never hold that fence during numerical work.

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

Record-only scalar snapshot equivalence may also admit equal range tokens for
non-strict Item/by-date Raw selections with fully visible selected values and
unchanged scoped visibility witnesses, including their check timing/ordering.
Use original availability/delay bounds; equal cutoffs alone
cannot admit changed timing evidence. Partial visibility and unsupported witness
forms require existing exact summaries when full header equality fails. A range
mismatch remains unknown in record-only mode. Intersect redundant Raw observation
bounds only in the transient comparison; preserve frozen requests, ordinary
identities and registered descriptors. Charge scoped metadata to the active
buffer, honor cancellation, and do not load optional summary Sources for an
already sufficient range proof.

Scoped equivalence first prunes immutable sequence/branch references using the
receipt's captured observation/availability bounds, then applies the existing
per-batch visibility and exact-summary checks. Missing bounds remain conservative.
Optional Raw summary bounds apply independently to each batch and cannot enable
receipt-level pruning for unbounded historical receipts. This is a scoped proof,
not an audit of disjoint descendants: explicit `inputs.verify()` still traverses
and validates all retained evidence.

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
External `items.publication` uses that same finite writer session for immutable
header traversal across its output dates and groups. It must not retain a
transaction/read snapshot across helpers, bypass any per-group Source/definition
fence, or commit/close an enclosing borrowed session. Cancellation/failure/exit
release owned resources; repeated metadata reads never create another authority.

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
Exclude events with registered disjoint observation bounds before constructing
their scans/joins; missing bounds retain the complete typed consumption path.
Validate the whole original's physical schema and row count from its Parquet
footer before bounded consumption; selected row counts are not whole-file counts.
Keep availability lower bounds after latest-version resolution, preserve the
actual first-known date, and leave General event selection unchanged. Ordinary
reads do not audit file bytes; explicit verification still checks the complete
original. Unbounded internal reads retain their full-row contract.

`inputs.read(date_range=(field,start,end))` is restricted to an immutable extra
Raw primary-key date in effective by-date `versions` view. Preserve its original
typed/text key representation, declaration, frozen Source, cutoff and check
overlays. Reject General/Item inputs, other views, non-key fields and mutable
system/timing fields. Date/Datetime and compact/ISO nullable parsing are row-local
predicates only. Apply the same predicate before visibility-event key encoding;
never substitute a source/announcement-date window for an economic event date.
Within an Original, concatenate exact typed fragments without all-null inference.
For this selector only, reconcile captured Original schemas with compatible
types, retaining typed empty pieces; an all-null selected payload must not erase
its registered type. Never substitute a current/global schema or change ordinary
read normalization while implementing this selection.

Search only that key column in caller-admitted chunks before candidate payload
groups and temporary natural-key construction. Typed temporal statistics can
skip disjoint groups; canonical-looking string extrema do not certify all-row
formatting. Preserve invalid/null/whitespace/non-padded parsing behavior. Progress
must distinguish predicate rows/column bytes from candidate payload group upper
bounds and selected rows. Legacy overlapping layouts may still require the full
date-column lookup on append: bounded retention is not bounded total input work.
No historical row index, alternate authority or implicit audit is permitted.
Preparation cache keys include the exact secondary-date selection and remain
operation-local; original receipts and canonical identities are unchanged.

`integrity.update_scopes(scope_keys=...)` composes the native metadata filter;
omission retains inventory, and an empty iterable performs no ledger query.

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
Authenticated decoded blocks, schemas, current captures and alias fingerprints use one byte-bounded
same-connection reserve, shared by nested contexts at their smallest explicit
limit; omitted nested options inherit the parent. Original digest,
kind and count checks precede insertion; oversize blocks bypass retention.
Any own-connection write disables reuse even after rollback. Owner exit clears
the reserve; explicit audits and closing publication use fresh original checks.
Currentness and selection comparison share current captures in that same shrinking
reserve using full requests, cutoff, read boundary, representation flags and
unchanged write generation. Original Source identities, pending outputs,
creator relationships and completion/range checks remain independently required.
An exact unchanged registered alias state establishes content equality only;
it never admits producer freshness or bypasses pending/creator/completion checks.
`inputs.currentness_identity` streams authoritative mutable metadata and original
completion bindings in one current view. It is an owner-state identity, never
a currentness verdict or audit. A consumer may bind it only in an independently
validated completed publication, after equality from before readiness validation
through the Data writer fence. Fresh fenced equality can reuse that publication;
unknown/changed state needs normal validation. No post-commit qualification,
new Data proof artifact or schema change is involved. Optional selectors and
unrelated frozen Source registrations do not invalidate the identity.

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
An empty original audit projection preserves declared key selection and temporal
normalization, but need not derive discarded virtual record IDs when both key
selection and normalization retain nonempty columns and the original row count,
and normalized keys are only Null, Boolean, String, Binary, integer or floating scalars. Missing
or duplicate key names still fail; malformed selectors and complex key types
retain the ordinary record-key path. The private captured-bound date audit may
apply the same admission only to present `time`/`source_time` axes when the entire
original schema is primitive or temporal. Ordinary nonempty projections retain
their exact key derivation path; the audit-only entry shares one private engine
with that unchanged public signature. Full original
checksums, Arrow validation, schema/count/bounds and field-change audits remain
mandatory, including their cancellation and buffer limits.
A direct audit opens its own finite read-only metadata snapshot. Successful
proofs expire when it exits; a later direct audit rechecks original bytes and
metadata. Each retained full-commit seal is validated separately, while its
alias's immutable retained-batch membership is read once for those seal checks.
Lazy seal mappings/sequences serialize to the original materialized canonical
JSON bytes; auditing never assigns a replacement digest to retained evidence.
Within an unchanged read view, a full audit prepares each registered selection
set's bounded member inventory once and checks every member in finite groups.
Public keyed reads and later audits prepare fresh inventories. A writing shared
view loses reuse admission and revalidates through the ordinary keyed reader.
An unprojected scalar Item scope with no selected original batches retains the
ordinary canonical empty token during metadata-range audit, including causal
baseline evidence and full Source parent provenance. This never skips the
preceding original-byte/bounds audit or accepts a different summary identity.

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
Snapshot and verified export share one inventory from the copied SQLite view:
manifest generations, every committed version segment and all registered
visibility events. Deduplicate confined physical paths; reject malformed paths,
symbolic links and missing originals rather than adopting loose files. Historical
committed originals remain required even when absent from the current manifest.
Verified backup checks committed originals and visibility events even without a
frozen Source. Snapshot performs no historical decode or checksum audit and still
requires explicit verification before computation. Its `generation_count` retains
the manifest count; `immutable_file_count` counts the complete copied inventory
and `visibility_count` counts its visibility files.

`integrity.storage_inventory(within_root=...)` is the public complete stat
inventory for metadata/sidecars and the lake. Include hidden/unknown entries,
logical and allocated sizes, and inode identities for consumer deduplication.
No body reads, initialization or cleanup occur. Data remains independent of Core;
the inventory uses only the standard library. Semantic `storage_usage` and the
existing conservative temporary-cleanup authority remain separate contracts.
