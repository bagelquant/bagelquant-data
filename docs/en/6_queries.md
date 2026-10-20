# PIT reads and frozen inputs

```python
history = lake.raw.read("daily", source="tushare")
snapshot = lake.raw.read("daily", source="tushare", as_of="2020-01-10",
                         observation_start="2020-01-01", observation_end="2020-01-05")
current = lake.raw.read("daily", source="tushare", view="latest")
versions = lake.raw.read_versions("daily", source="tushare")
observations = lake.raw.observations("daily", source="tushare")
item = lake.items.read("close")
fixed_item = lake.items.read("close", view="snapshot", as_of="2020-01-10")
```

Daily Raw and DataItem historical reads select each observation's version visible
on its own date. Fixed `as_of` is an independent information cutoff. Reading a
short observation window does not substitute its endpoint for the cutoff. Raw
`start/end` filter the availability axis; `observation_start/end` filter the
observation axis. `observations()` restores the observation `time` axis after PIT
selection. `general` reads return a complete selected snapshot, with optional
`as_of`; they have no implicit daily coordinate grid.

`strict=True` excludes historical baselines whose original timing is unverified.
Same-value later collection can provide a later verified availability witness;
it cannot invent an earlier publication date. `view="versions"` exposes all
eligible revisions. Ordinary LazyFrames pin committed generations at creation,
so delayed `collect()` does not switch to a newly published generation.

```python
from bagelquant_data import RawInput, ItemInput, input_read_boundary

receipt = lake.inputs.freeze({
    "raw": RawInput("tushare", "daily", view="history"),
    "close": ItemInput("close"),
}, information_cutoff="2020-01-10")
frozen = lake.inputs.read(receipt, "close")
verification = lake.inputs.verify(receipt)
with input_read_boundary(lake.data_meta_path, receipt.max_commit,
                         receipt.information_cutoff, max_check_id=receipt.max_check_id):
    # Readers opened here capture the same boundaries, including across workers.
    reader = DataLake.open(data_meta_path=lake.data_meta_path,
                           lake_path=lake.lake_path, read_only=True)
```

Receipts atomically record dependencies, definitions, schema, compressed batch
identities, information cutoff and commit/check upper bounds. They read verified
local evidence, surviving later revisions, unregistering and current-file damage.
Nested boundaries may tighten but cannot explicitly expand. Reopening receipt
IDs uses `lake.inputs.get(id)`; reading never fetches providers.
An omitted check ceiling is captured from committed runtime metadata without
copying the database or WAL. Later checks do not widen an entered boundary.

For an operation that repeatedly reads the same frozen inputs, enter a finite
read context. It resolves original receipt metadata once and shares one SQLite
read view with matching read-only Data readers opened on the same thread:

```python
with lake.inputs.read_context(receipt, check_canceled=check_canceled,
                              progress=progress) as reader:
    january = reader.read(receipt, "close", start="2020-01-01",
                          end="2020-01-10").collect()
```

Metadata row queries decode shared request blocks and schemas on the same
connection that fetched the rows. This also preserves visibility within an
enclosing write transaction. Immutable-reference checksum and missing-evidence
checks still apply; the connection is released when its owning scope exits.

`inputs.window_read_supported(receipt, alias)` is a metadata-only planning query:
true means every retained by-date batch has captured/sealed bounds or an existing
checked original index header capable of safe pruning. General/missing bounds
return false. It reads no index rows or original bytes and never builds an index
or infers old bounds from live batch metadata. Restored window reads admit header
JSON and selected original rows before decoding, using the public read-context
buffer allocation; cancellation propagates before original reads. Unknown support
must not be treated as a bounded monthly read guarantee.

Observation bounds also reach each selected original Parquet scan and its
first-visibility event scan before temporary natural-key hashes are computed.
The reader checks the whole original's schema and row count from its physical
footer, then consumes only matching observations. The first-known availability
date remains separate from those observation bounds. General snapshots retain
complete event selection; Raw availability lower bounds remain after latest
version resolution. Requested field projection follows PIT selection, and full
byte verification remains an explicit audit.

### Secondary primary-key dates

For a by-date Raw request with effective `view="versions"`, a date stored in an
immutable extra primary-key column can select the consumed events:

```python
with lake.inputs.read_context(receipt, config=execution, progress=progress) as reader:
    events = reader.read(receipt, "events",
        date_range=("event_date", "2024-06-01", "2024-06-30")).collect()
```

The inclusive range parses Date/Datetime values or compact/ISO date text using
nullable parsing. Invalid/null dates do not match. Original text and types are
returned unchanged, including non-padded or whitespace-bearing values accepted
by that parser. The frozen observation window, Source, cutoff, commit/check
ceilings, complete natural keys and first-visibility witnesses remain in force.
This selector neither rewrites the Raw declaration nor creates another receipt
identity. It rejects General, Item, history/snapshot views, non-key fields and
system/timing fields such as `time`, `source_time` and `ingested_at`.

For this selection, fragments of one Original retain its exact registered
schema. Compatible schemas across captured Originals are reconciled without
normalizing selected all-null payloads to `Null`; typed empty pieces retain their
original types too. A current/global dataset schema never replaces those
captured schemas. Ordinary reads retain their existing normalization behavior.
Thus a pruned ordinary read can infer `Null` where this selector retains the
registered `Float64` or `String`; values, natural keys and PIT witnesses agree.

The reader searches only the selected date column in admitted row-group chunks
before opening candidate payload groups and constructing their temporary keys.
Typed temporal statistics can skip disjoint groups; string extrema do not prove
all-row date formatting and never justify that skip. Matching payload groups
remain subject to the ordinary pre-allocation and retained-byte limits. The same
predicate is applied to visibility-event keys before their temporary key encoding.
No persistent row index, implicit audit or historical producer replay is added.

`read_context(progress=...)` emits `phase="date_range_read"` for original and
visibility-key work. It separates decoded `predicate_rows` and compressed
`predicate_column_bytes` from conservative `candidate_payload_rows/bytes`, actual
`selected_rows` and `row_groups_skipped`. Candidate payload work is a row-group
upper bound, since a matching group can contain unrelated rows. A legacy file
whose date column spans the whole history can still require that column's full
lookup on append; this is explicit read work, not metadata-only or a universal
bound on total input I/O. Unchanged Source/currentness checks perform no search.

For authoritative update-scope inspection, `integrity.update_scopes(...,
scope_keys=...)` filters the native ledger before materializing rows. Omission
retains the complete inventory; an empty iterable returns no rows.

## Optional metadata lookup maintenance

`lake.inputs.index_record_lookups(config=ExecutionOptions(...))` explicitly
installs SQLite access indexes for completed builds, dataset version checks and frozen Source check
ceilings. It scans metadata, uses one atomic writer transaction and supports
`check_canceled`. Original receipts, checks, values and canonical identities stay
unchanged. All missing indexes roll back on failure. Admit temporary disk space for
sorting separately from the buffer budget.

New empty tables initialize these cheap indexes; opening populated databases
never builds missing indexes. Ordinary prerequisite queries continue to read
registered metadata without this maintenance. Lookup indexes provide no value
equivalence or currentness authority; selection/check proof maintenance and
explicit original-byte verification remain separate operations.


Ordinary is_current calls hold one finite metadata view and reuse exact recorded
check-to-commit relationships within its budget. Writer generations invalidate
local memos, while outside WAL commits remain outside that fixed view until the
next operation. This reuse never verifies historical bytes or creates proofs.
When one alias needs conservative provenance checks, other aliases retain their
own exact current completion authority. This includes an empty bounded Item
range completed by adjacent publications. Original Source state, complete range,
checked cutoff, dependency definitions and pending-output checks still apply;
unknown or gapped coverage remains incomplete. The compact Source header map is
read once for the operation rather than once for every fallback alias.


inputs.index_selection explicitly builds a canonical scalar summary directly
from bounded original typed evidence, without persisting historical row indexes.
Optional coordinates accept exact Item Date/String keys, including absent values;
shared selectors are stored once. Normal checks only reuse the exact scope and
return unknown when evidence is missing. Full verify checks the saved summary
and selector against original bytes. The operation requires an independent writer,
explicit resource admission and cancellation; it preserves all original receipts.

Finite bulk consumed-projection maintenance uses `InputSelectionScope` and
`inputs.index_selections(receipt, scopes, config=...)`: one alias, 1..16 exact
scopes, one shared original transport per admitted group, and the same canonical
reducer as single-scope/indexed selection. `coordinate_selector_identity` returns
a typed coordinate hash without I/O; first maintenance registers the supplied
actual mask, later explicit groups may name that selector. Ordinary proofs still
bind actual caller coordinates. Groups are atomic, respect aggregate buffer
admission and never create historical row indexes or run upstream producers.

For shared prerequisite inventory, `inputs.selection_descriptors(receipt, scopes,
config=..., check_canceled=...)` accepts the same finite 1..16 same-alias scopes
with actual coordinates. It returns JSON descriptors containing Source/alias,
normalized request, canonical request/evidence keys, fields/coordinate identity,
known/unknown status, complete content token and original proof Source reference.
Unknown scopes are not prepared. `index_selections` remains the explicit
preparation operation; progress counts original batch header rows separately
from owned transport and scalar hash work.

`inputs.resolve_selection_descriptors(receipt, descriptors, ...)` rechecks
registered complete original descriptors against a supplied Source using only
metadata and selector headers. It never reconstructs membership, decodes selector
IPC, or issues a new axis. `as_of=None` follows the target Source's information
cutoff; explicit ISO `as_of` stays fixed. `requested_cutoff` is the actual resolved
date and `canonical_cutoff` the selected stream's canonical date. A new target
without a matching summary is unknown; forged or unregistered original proofs
fail. The consumer must separately prove unchanged Universe/calendar/policy
axes before reusing a stored descriptor. First or changed axes require actual keys.
Registered proof memoization is finite and expires with the read context or writes.

`inputs.read_selection_set` admits the registered header before its Source,
then materializes the selected plain descriptors in one finite metadata snapshot.
Lazy descendants reuse that connection through the end of the call. Existing
readers and writers retain their transaction, including uncommitted writer rows.
`register_selection_set` shares bounded validation memos across its groups and
closes its newly owned reader before the publication transaction. Ordering,
descriptor identities, memory checks and original Source fences are unchanged.
Neither operation reads historical numerical bodies or retains a cross-call cache.

General `coordinate_identity` now inherits exact selected atomic authority across
Sources. Explicit `index_coordinates` retains one original date payload and
records its canonical locator/source in the existing projection index. Ordinary
reads never backfill missing optional records. Selected revisions or different
baseline/cutoff/field/selector/window require their exact authority; unsupported
consumed schema conflicts block before cached proof. `verify` audits the original
Source and sole payload even when a later Source consumes that projection. This
date token does not certify timing or complete row equality.

Before an external producer build, explicit `inputs.prepare_general_projections(
original_receipt, alias, fields, through=end, config=..., check_canceled=...,
progress=...)` prepares its General field prerequisites. Obtain the exact
original input receipt from `items.completed_builds(name)`; the runtime producer
policy still belongs to the caller. The operation shares the planner's original
and every newly visible atomic state, including intermediate commits on one day.
Only missing canonical scopes register captured Source metadata and run the
existing `index_selections` in admitted single-scope groups. Known scopes read
no original rows and retain their proof Sources. Completed groups survive
cancel/budget failure; retries reuse them. No-eligible states are reported as
unsupported without fabricated empty-schema proof. Work reports distinguish
reused/prepared scopes, groups, new Source records, original batch-header rows
and transport. Field preparation cannot override timing/baseline impact or a
real consumed-field change: normal planning still decides producer admission.

Use `fields=("time", "asset_id", "value", "available_date", "observation_date",
"base_available_date")` on `InputSelectionScope`, `index_selection`,
`selection_identity` or `selection_equivalent` when a consumer uses those
columns. Explicit fields bind their actual types and values after the same
PIT/strict/baseline/version selection, including typed empty results. Columns
must exist in the original frozen schema; no dates are invented.
Typed empty by-date publications retain their supplied schema, including extra
fields, in the original empty-check transaction. Freeze after publication to
capture those types; never-materialized declarations still lack undeclared fields.
Fields imply a consumed projection and ordinary checks read only registered summaries. The
default `fields=None` preserves scalar proof identity. `selection_equivalent`
also accepts actual `coordinates`, retaining absent keys and rejecting
keys outside either original window. Item keys use the Item time axis. With
explicit fields, by-date Raw snapshot/history keys use observation `source_time`
(named `time` in the supplied coordinates); physical version `time` remains an
independent consumed field. Every selected row for an extra record key remains
in the projection. Default scalar Raw proofs still reject coordinates.
Explicit fields also support whole General snapshots: use `view="snapshot"`
and omit observation windows, coordinates and causal versions. No daily or asset
grid is inferred; the projection retains every selected row, including duplicate
field tuples from distinct extra keys. Validation and typed empty results use the
selected original atomic snapshot schema, rather than a later dataset schema.
A selected typed empty snapshot is supported. With no eligible atomic event,
field proofs raise `ValueError` before original reads or summary writes; no
current or union schema supplies missing original authority. The existing
General read concatenates retained snapshots before atomic selection, which can
coerce mixed numeric types. Field proofs therefore require identical consumed
field dtypes in every captured atomic schema; conflicts or missing authority
raise `ValueError` before cached proofs, original reads or summary writes.
Unconsumed column schema changes remain irrelevant. The existing
summary store binds its original commit/check/batch authority; field equality
does not prove publication timing or whole Source freshness. Missing proof needs
explicit `index_selection(original, alias, view="snapshot", fields=(...),
as_of=...)` for each original/current state. For same-date intermediate changes,
freeze the named original `max_commit` before indexing; final-state equality
cannot substitute for that intermediate proof. Ordinary lookup never does this
maintenance. Default scalar General projections remain unsupported.
Changes to unconsumed payload fields or
nonmember coordinates do not change this projection; Raw source mode, coverage
and producer freshness require their own original evidence. `inputs.verify`
reconstructs each summary and selector from bounded original evidence.
Independent later dates and ineligible future witnesses reuse already-registered
historical field summaries across newer receipts without original-byte reads.


## Compact storage and readiness

Schema 9 stores every historical value and first-visibility event once in original
immutable Parquet. SQLite holds declarations, compact file/change metadata, node
completion flags and frozen references. `range_identity` and ordinary
`selection_identity` use metadata without original reads or index preparation.
Optional exact `index_selection(s)` and small coordinate axes are explicit;
ordinary identity stays unchanged by preparation. `inputs.verify` audits exact
original Parquet bytes, typed schema, bounds and summaries with bounded readers.
Damaged or missing originals require a trusted backup.

Scoped `selection_equivalent` checks prune immutable metadata branches using
captured observation/availability bounds before loading segment headers. Missing
bounds retain candidates; optional Raw summary bounds apply independently to
each batch. The existing visibility and exact-summary checks still decide
equality. Skipped disjoint descendants are outside this scoped proof; explicit
`inputs.verify` continues to audit every retained descendant.
