# DataItems and external producers

An explicit declaration restore can call `lake.items.register(original_spec,
expected_definition_hash=captured_current_hash)`. Data checks that exact active
hash under the item writer lease and in the same transaction as registration.
A missing or concurrently changed definition is rejected before declaration
changes. This protects one item; several calls are not an atomic group. The
caller must prove the original declaration and retain recovery progress and old
receipts. It does not compute outputs or create completion authority.

A DataItem has unique deterministic `(time, asset_id)` keys and a declared scalar
`value` type: floats, integers, booleans, strings, dates, datetimes or categorical.
Definitions, dependencies and producer revisions persist in the Data SQLite.

`lake.items.completed_builds(name)` reads verified whole-range completion and
pending-visibility authorities. `builds(name)` also includes ordinary successful
row ingests and cannot establish completeness. Missing legacy proofs require
explicit `index_completion` with the original complete producer report.

Native initialization and updates check cancellation before and after saving
each completion certificate in its metadata transaction. A late cancellation
rolls back that certificate and reports `cancelled`; committed output rows and
earlier completed units remain available for retry. Cancellation of the final
combined range does not discard an already completed append unit.

An interrupted first build of a new numerical definition can leave successful
row publications without a completed requested range. Explicitly resume that
attempt with `lake.items.resume_update(name, build_id=original_partial_build_id,
start=original_start, end=original_end)`. The saved build must bind the current
definition and its original frozen Source; `end` must equal that Source's
cutoff. Register the same runtime producer before retrying. Recovery reuses the
existing producer/publication path and preserves every original file. It never
promotes partial rows to completion. Completed current definitions, ambiguous
lost whole-range certificates, stale or different Sources/bounds, and existing
initialization authority are rejected. A dataset lease excludes competing Item
publishers; final completion rechecks Source, definition, output/check counters
and cancellation in one finite writer transaction. Normal update remains the
path for append, repair and no-op work after successful recovery.

`items.recertify(name, start=..., end=..., update_policy=...)` explicitly updates
only a proven unchanged completion's source records. It never initializes,
updates dependencies, reads values or invokes a producer. Recertify dependencies
first. Changed/new/unknown impact is rejected before numerical work. A supplied
policy must agree with any runtime-registered immutable producer contract.
`expected_definition=lake.items.get(name)` binds a consumer's declaration;
the captured definition is checked again on admission and publication. A runtime
producer key/revision's declared causal policy cannot change or be removed.
An initially absent policy may be declared later; active recertification still
checks that policy at admission and publication.
If the original Source is still current but a later dependency publication made
its live completion flag stale, an admitted no-op refreshes only the dependency
state of that exact completed build. Original covered dates, checked target,
semantic identity, Source and result reference stay unchanged, including when
the requested end is earlier. The original output/check ceilings, definition,
current publication and complete flag must still match; wider or missing
authority requires separate evidence. No values, new Source or build are created,
and a current repeat writes no flag. Cancellation rolls back the metadata refresh.
For a consumer publishing saved completion references,
`lake.inputs.publication_guard(receipts)` holds the Data writer fence and one
fixed metadata view through the consumer's own revision CAS. It requires a
writable lake outside an existing read context, performs no byte audit and
writes no Data records. Failure or cancellation releases the fence.

```python
from bagelquant_data import DataItemSpec, RawInput, Select, Cast, ProducerUpdatePolicy

lake.items.register(DataItemSpec(
    "close", (RawInput("tushare", "daily"),), value_dtype="float64",
    time_column="source_time", value_column="close",
    transforms=(Select(("source_time", "asset_id", "close")), Cast({"close": "float64"})),
))
lake.items.initialize("close", end="2020-12-31")
lake.items.update("close", end="2021-01-15")
```

`items.initialize_many(names, start=..., end=..., config=...)` initializes an
independent ready group, interleaving compatible Items over bounded observation
windows. `initialize` uses this same coordinator. `ExecutionOptions` defaults to
31 natural days and stops each fallback unit at a calendar-month boundary.
When original row counts admit a tiny whole range with a broad reserve, this
default coalesces it into one unit; ordinary read/output byte checks still apply.
Smaller `observation_window_days` request explicit windows and can reduce the
working set. Units that exceed their
explicit buffer admission split further, down to one date. All assets for a
date remain together. `observation_window_days=None` requests one admitted range.

Unverified baseline units with independent coordinates are grouped toward a byte
target even when no row-count cap is supplied. An indivisible row may exceed
that target inside the enclosing working buffer. Groups retain each row's original
availability and baseline marker; verified rechecks still publish chronologically.

Windowing applies to a producer declaring `same_date` without positive history,
or to native pointwise `Select`/`Filter`/`MapValues`/`Cast` transformations.
Other producers retain their declared whole-range read requirements and must fit
the caller's buffer budget. A finite-history or as-of producer is never treated
as same-date merely to reduce memory. Parquet reads apply observation predicates
and an admission row limit before collection, then check actual frame bytes;
oversized inputs fail with an actionable `MemoryError` before full-history
key preparation. These are working-buffer limits, not a process-memory hard cap.

Every Item retains its own field declarations and frozen Source. Compatible
physical reads share temporary original keys and visibility preparation in an
operation-local cache, charged at most one quarter of `max_buffer_bytes`. The
cache binds the immutable alias header, physical scope and information cutoff;
it is cleared at the end of each window and never becomes stored history or
proof authority. Producers receive declared payload fields plus causal coordinate
and version channels. A shared read cannot widen an Item's logical dependency.

That same bounded window cache can reuse output natural-key encoding across
Items only when the ordered primitive/temporal key schema and every key value
match exactly. It charges the retained keys and record hashes together and
expires them with the window. Floating/nested keys retain scalar encoding.
Each Item still hashes its own typed payload. Publication reuses its initial
payload hashes only when schema alignment leaves all payload dtypes unchanged
and no semantic-version timing comparison requires a fresh encoding.

The initialization ledger pins its original Source, definition, causal producer
policy and whole requested bounds. Each window uses that original information
cutoff and preserves all causal revisions, removals and pending visibility.
Committed units survive cancellation; resume reuses their exact certificates,
including when resource settings choose different future window sizes. Unit
reuse also checks later output commits and visibility evidence: an overlapping
write at the pinned cutoff requeues the affected range, while disjoint units
remain reusable. Final publication repeats this check under the writer fence.
Unit certificates cannot replace a whole-range completion. Only after every unit
succeeds does one final whole-range record advance completion. A later source
change does not replace the pinned Source during resume; its historical result
remains available and receives no current-input flag until an ordinary update
proves the new input impact. Currentness and original-byte integrity are separate.

`items.update_many(names, end=..., start=..., config=...)` applies the existing
causal append/repair path to independent ready Items with shared bounded input
preparation. Complete declared Item dependencies before admitting either batch
API; an interdependent group is rejected. Completion and failure remain per Item.
The coordinator reuses one metadata connection across updates; every capture,
read snapshot and publication transaction retains its own finite fence. Reuse
holds no operation-long transaction or old metadata snapshot.
Unchanged initialized calls read metadata only and create no workers or input
frames. Earlier native receipts that physically overcaptured fields remain
immutable; an unchanged declaration can narrow that overcapture without numerical
replay. Registered selected-field value, timing and known-baseline evidence must
prove an unrelated-column change; unknown timing still follows ordinary planning.

`ItemBuildReport.work` contains execution diagnostics outside numerical identity.
`windows_built`, `windows_reused`, optional `windows_split`, `build_seconds`,
`input_seconds`, `producer_seconds`, `publication_seconds`, `producer_calls` and
`input_rows` describe one Item. Phase times include permit waits and may overlap;
do not sum them into a critical path. Cache `prepared_hits`, `prepared_misses`,
`source_rows_read`, `key_rows_prepared` and `prepared_peak_bytes` are totals for
the entire batch, repeated in each report: do not sum sibling reports. Key rows
count original by-date rows, excluding separate visibility-event key work.
`output_key_cache_hits` and `output_key_rows_encoded` count the shared
output-key work for the batch, also
repeated rather than additive across sibling reports.

`ExecutionOptions.native_admission` accepts a caller-owned context-manager
factory for `columnar` or `mixed` work. It is execution-only, omitted from repr
and comparison, and never changes definitions or receipts. Data does not choose
machine-wide limits. External producers default to `native_kind="mixed"` in
`register_producer`; explicit `native_kind="columnar"` is a caller promise about
the implementation, separate from its causal policy. Mixed revision evaluations
are serial when admission is supplied, so an exclusive outer numerical permit
cannot deadlock waiting for worker-thread reacquisition.

Built-in `Select`, `Filter`, `MapValues`, `Cast`, explicit-key `Join`, and `Align`
operate on neutral frames. Duplicate output keys fail. Alignment uses caller
coordinates; missing values stay null. Forward filling requires a finite
`forward_fill_sessions` and explicit coordinate dates, respects availability,
and stops at a superseding null event.

Complex computations register an external producer by stable key/revision:

```python
def produce(context):
    return context.read("prices").select("source_time", "asset_id", "close")

lake.items.register_producer("my-prices", "revision-1", produce,
    update_policy=ProducerUpdatePolicy("same_date"))
lake.items.register(DataItemSpec(
    "external", (RawInput("tushare", "daily", alias="prices"),),
    time_column="source_time", value_column="close",
    producer_key="my-prices", producer_revision="revision-1",
))
```

`BuildContext` contains frozen frames and range/cutoff/commit evidence, with no
Data/Core coupling. Producers must be registered again for execution after
reopen; definitions remain readable. Dependency receipts govern invalidation.
Unknown impact returns an actionable blocked plan; it never silently rebuilds completed history. Removed output keys
publish tombstones; historical versions remain. Direct neutral results use
`lake.items.ingest`, and share the same commit/recovery protocol.

Direct output with declared dependencies must pass `input_receipt` from
`lake.inputs.freeze`; Data verifies the input identities and stores that receipt
with its commit. `expected_definition_hash` rejects definitions changed during
external computation. Without explicit availability evidence, direct ingestion
becomes visible at collection time. Historical baseline ingestion remains
explicit and strict reads exclude it. Complete external ranges use
`items.replace_range(name, frame, start=..., end=..., available_date=...,
input_receipt=...)`; missing coordinates receive null events without rewriting
old frozen history.

Related external outputs use `with items.publication(input_receipt=receipt,
config=ExecutionOptions(...), cancelled=callback) as publisher:` and
`publisher.publish(publications)`. The context reads its fixed immutable
input records once on entry and accepts serial groups without accumulating
all output frames. Each `ItemPublication(name, frame,
expected_definition_hash=...)` carries historical versions; supply `start`,
`end` and `complete_at` together to certify a complete range. Every group
preflights its target definitions, dependency identities and frames before
writing and uses the ordinary historical-ingest and complete-range commit path.
Returned reports describe each table's last suboperation, not cumulative rows.
For a derived result with proved discrete repairs, supply the original native
`reuse_build_id` and disjoint `affected_ranges` together with whole coverage.
Data replaces only those ranges, including withdrawals, and certifies reused
history plus complete new dates. Missing native coverage, uncovered new/pending
dates or later output outside those ranges fail explicitly. Untouched versions
and old frozen receipts remain intact; completed partial units can be retried.
Empty affected ranges publish a current-source completion without rewriting
values. Repeating the exact source and original native continuation reuses that
completion only after checking source, output, timing, definition and cancellation
fences. An equal result counter alone does not authorize reuse.
Missing coordinates retain null events. Earlier commits survive later failure
or cancellation; publication is not atomic across output tables. A failed group
invalidates the operation even if caught. Successful context exit checks
cancellation again. Closed operations reject reuse, groups require the owning thread,
and each new context rechecks original registered identities. Ordinary `ingest`
and `replace_range` validate their declared source identities independently;
original-byte verification belongs to explicit `inputs.verify` audits.

The publication operation shares the existing finite metadata writer-session
connection while repeatedly reading immutable input headers. This avoids opening
one SQLite connection per metadata block or output date. It retains no transaction
or read snapshot across helpers/groups: each ordinary publication transaction,
definition/Source check and completion fence remains independent and observes
later committed writers. Nested borrowing does not commit or close an enclosing
session. Cancellation, failure and exit release the operation's resources;
there is no persistent validity cache or new completion authority.

Baseline timing checks all aliases' immutable positive proofs before decoding
optional timing projections. A selected complete General baseline, including
an empty event, is proven from the original snapshot/check headers at the actual
cutoff and commit/check ceilings. One surviving baseline settles the OR for that
date; missing evidence still blocks a negative result. A verified
publication context memoizes the final boolean for each exact effective cutoff;
later attestations can change the result, so dates are never treated as monotone.
Negative results require checking every alias. The memo holds no input frames,
reserves at most a quarter of the supplied buffer budget at 1 KiB per entry,
and is capped at 1024 entries with oldest-entry eviction. Failure and exit clear
it. Exact unchanged builds read registered completion and dependency records without consuming frames. Original-byte verification is explicit; standalone defaults remain serial.

Timing-only reads use the latest unresolved effective cutoff to skip scoped
by-date batches whose checksummed captured minimum physical availability is
later. Attestation copies respect the same cutoff; availability can only move
forward. General snapshots and unknown/legacy bounds keep broad reads. Empty
Item dependencies recurse at their own cutoff. Ordinary value reads, original
receipt identity and complete byte verification of all retained batches,
including future batches, remain unchanged.

A positive-only timing proof can avoid frame reads if every scoped retained batch
has a uniform baseline flag, its known observation bounds are entirely inside
the request, and a nonempty batch proves a physically visible original row.
Eligible nonbaseline witnesses disqualify the candidates they can affect; full
seals are validated. A partial first-visibility event binds both its original
commit set and observation bounds. An untouched partition can still prove a
surviving baseline, even when another partition of the same commit was verified.
Missing or reversed event bounds remain conservative for the referenced commits;
an eligible check without witness metadata leaves the proof unknown.
Strict/history by-date views, legacy/unknown bounds, mixed flags and partial
overlap cannot use this positive shortcut. It never proves a negative result,
overrides a supplied frame, or replaces complete original byte verification.

Read-only Item plans use these metadata proofs across all dependencies at every
new timing boundary. They never decode historical rows to decide whether a
declared neutral General input's first verification preserves an existing
baseline. Unresolved evidence follows the existing selected-event impact or
blocked-plan path. Completed history, producer identity and buffer limits remain
unchanged; admitted updates calculate only new or affected output intervals.

Writable lake initialization creates a derived covering metadata index and
analyzes the batch table once, atomically. This avoids traversing recovery payload
overflow pages just to read metadata. Subsequent opens retain statistics; existing
schema-nine read-only lakes work without this index. Original recovery payloads,
frozen receipts, hashes and historical timing remain unchanged.

A first direct panel publication with unique coordinates and uniform timing
proof uses one atomic commit across its monthly partitions, preserving each
row's availability date. Same-coordinate revisions, existing-content checks and
mixed baseline evidence retain separate chronological publications. Complete
range receipts preserve the declared bounds even when their output is empty.

`items.builds(name)` exposes successful build receipts in publication order,
including definitions, dependency digests, ranges and frozen input references.

`selection_identity(..., values_only=True, causal_versions=True)` is an Item
versions projection for consumers of scalar PIT events. It retains every distinct
availability, baseline flag and typed value after resolving redundant creators
at the same coordinate/availability. It excludes arbitrary auxiliary columns;
it is neither complete Source equivalence nor a producer freshness proof.

`inputs.selection_equivalent(original, current_or_none, alias, ...)` compares the
actual consumed window/view/strictness and information cutoffs. A missing current
receipt captures only current alias metadata inside the read view, without
persisting a freeze. Exact retained header/witness equality can prove reuse;
otherwise optional canonical indexes are needed. `records_only=True` permits
only that header proof; `summaries_only=True` also permits saved canonical
summaries but never decodes per-row indexes. Unknown stays `None`.

Explicit `inputs.index_selection(receipt, alias, start=..., end=..., view=...,
causal_versions=..., config=...)` records a scalar summary from existing value
indexes; it never builds missing batch indexes. Each summary binds selected
original headers, creator ordering, witnesses, unknown empty baselines, the
effective selector/cutoff and its registered original source. Unrelated global
counters/aliases and known empty covers do not invalidate it. `inputs.verify`
audits applicable reused summaries and their original source closure, even when
the caller supplies an equivalent later receipt. Opening a lake and `freeze`
never warm missing historical summaries or sweep whole-history row indexes.

For General snapshot date-axis consumers, `coordinate_equivalent` binds the
actual clip, baseline admission and atomic snapshot. Changed content requires
explicit coordinate indexes; it never infers scalar/timing equivalence from
equal dates. `coordinate_identity` identifies that indexed projection. Both keep
unknown when the required original/current projection or admitted budget is absent.
Finite same-thread read contexts reuse immutable metadata and canonical tokens;
nested caches share only equal buffer budgets, expire on exit, and retain no frames.

`items.index_output_reuse(name, end=..., config=..., check_canceled=..., progress=...)`
explicitly audits later output batches against the original completed result.
It records actual affected dates from numerical, observation, information,
version-availability and baseline channels, excluding creator provenance.
Normal impact plans read these completion/batch/cutoff-bound records without
reading original Parquet values. Input revisions remain independently
planned. Missing/expired records preserve ordinary affected ranges, damaged
records fail, and output timing attestations currently require further explicit
selected-version proof. Maintenance never runs a producer or advances completion;
cancellation retains fully indexed batches and all historical evidence.
A later cutoff keeps a batch's proof when its recorded maximum physical availability
was already covered by maintenance and its own timing-check boundary is unchanged.
Future versions outside that bound remain unknown; affected dates are filtered by
their actual visibility at the requested cutoff.
Explicit maintenance first audits every original batch's date bounds once before
excluding reference candidates. Registered impact records bind that original
metadata corpus; ordinary planning checks its binding without reading original
bytes. Changed original headers fail rather than admitting a stale reference.
The records remain readable after archival. `inputs.is_current(receipt)` checks
the same declared windows and cutoff against current committed evidence without
creating another receipt. It follows the latest retained Item dependency proofs
recursively; a rebuilt result can be current while old historical proofs remain.
Pure verified same-content checks preserve currentness. `inputs.verify` separately
validates retained bytes; freshness does not replace integrity verification.
Fixed-cutoff currentness can compare General atomic event headers or complete
by-date Raw content headers and eligible witnesses without row indexes. Future
known checks are excluded; unknown empty baseline evidence, schema, definitions,
all content batches and visible witnesses remain bound. Changed content still
needs an exact selection proof. `progress=callback` reports these record decisions.
The same resolved record-equivalence path also excludes future physical batches
using captured bounds or existing original-batch bound summaries. Missing bounds
retain whole batches conservatively; neither replay scope nor original receipts
change. Item parent currentness remains a separate recursive requirement.

Empty derived inputs retain their frozen upstream timing proof. A downstream
count or other scalar result remains a historical baseline until its selected
upstream evidence is verified at that cutoff. Later attestations cannot cross
an earlier information date or captured commit/check ceiling. Verified inputs
use immutable timing flags to avoid repeating daily prefix selection.

Initialization uses the caller's bounded worker pool for revision evaluation and
first-publication monthly partition preparation. Writer admission uses a
conservative buffer reserve; later revisions and existing partitions remain
serial. Recovery compression precedes SQLite write-lock acquisition while
checksum verification and publication retain their transactional guarantees.

Resuming an existing internally evaluated unverified baseline groups independent
coordinates into batches limited by row count and a conservative byte reserve.
Every row retains its availability date; verified rechecks retain separate dated
witnesses. This avoids repeatedly rewriting each month for every historical day.

Frozen-input currentness avoids reading Item values when the covering successful
build proof is among all captured parents and every captured batch/check/build
parent is recursively current. Any stale parent falls back to ordinary
cutoff/window selection; an unselected historical parent does not invalidate the
result. Discrete completed ranges also need no whole-range certificate when
all captured creators are current and a complete captured nonempty batch is
inside the observation window and visible at the cutoff (excluding strict
baselines). Counts or overlapping bounds alone do not establish selection.
Empty or unknown selection retains its original completion/index requirements;
no index or byte audit is built implicitly. Integrity verification
still checks every original retained batch independently on every later call.
Neither optimization changes receipt identity or PIT selection.

A verified native monthly completion can also recertify older creator receipts
within that month while the consumer requests the full history. All captured
batches and checks from each recertified creator must fit entirely within the
completion range and precede its output commit and output-check bounds. Its
Source must be current at a cutoff no later than the consumer's. Creators also
used outside that range, unknown bounds, later checks and uncovered empty
completions retain their original requirements; original values and provenance
remain unchanged. This proof reads registered records only.

Typed-row-v1 payload hashing retains exact persisted bytes and schema identity. Scalar token encoding avoids repeated JSON work; string tokens share a conservative 4 MiB reserve, at most 1 MiB per column and no more than the input-frame byte estimate. Cache fill is lazy from the existing bounded row iterator; no full unique-value list, unbounded string cache, additional worker pool or identity migration. Nested/decimal/binary values retain recursive canonical JSON; temporal native precision, signed zero, NaN/null and infinity remain distinct.

Identical ordered reconciliation fields with identical schema may bypass revision hash joins. Shape, ordering, null/value/availability/baseline differences fall back to ordinary reconciliation. Final withdrawal checks first project time/asset keys; exact keys skip the join and full old payloads are read only for actual withdrawals. Tombstones and frozen historical evidence remain unchanged.

Unchanged-content availability witnesses are written with a bounded row iterator, preserving every original record, insertion order and atomic publication. The writer avoids full-panel Python dictionary and SQL parameter lists; a stream or constraint failure rolls back the header and witnesses together. SQLite durability and historical receipts are unchanged.

Evidence queries select the dataset/check headers before looking up their indexed records. Unrelated datasets therefore do not cause full witness scans during reads or input freezing. The selected evidence and its order, cutoff and receipt identity remain unchanged.

Large complete by-date baseline attestations may use an immutable content-addressed full-commit seal. Prove exact unique (record ID, payload hash, original commit) equality against every original registered recovery batch, with one uniform availability date, using bounded external sorting. Counts alone never establish completeness. Retain all original SQL witnesses/headers and batch identities. Freeze embeds the checked seal; read-only operations never persist seals. Validate checksum and exact header/commit/batch binding before cache use. Partial/mixed or general checks retain the ordinary path; oversized unsupported inline evidence fails explicitly before materialization. Legacy receipts recapture their original representation for currentness; changing representation alone does not invalidate them. Replay preserves header eligibility, check/commit ceilings, ingestion cutoff, availability max, attestation ID and parent receipts. Proven per-batch observation bounds prune window reads only; frozen evidence and verification still retain/recheck every original batch on each call. Temporary sort connections/files are closed and removed.

DataItems.update accepts explicit input_windows keyed by declared input alias, mapping to inclusive observation (start,end). Validate aliases, date order and declared start/end plus Raw observation limits before recursion/publication; never infer windows for arbitrary producers. Pin overrides in the actual frozen requests without modifying the DataItem declaration or its hash. Omitted inputs retain their declared scope. General Raw snapshots are not windowed. Exact successful reuse checks registered definition/dependency/range evidence before frame construction. Changed runtime input windows require a selected-input impact proof; they cannot bypass incremental planning. Original-byte verification is explicit.

A `same_date` producer can extend a by-date Raw window's upper bound using metadata alone when its definition, other request fields and lower bound are unchanged, the old upper bound covers the old information cutoff, and complete empty-check evidence agrees through the original check ceiling at the original commit ceiling. Raw availability is never earlier than observation, so the added future observations cannot change old selections, including an old empty selection. The ordinary completed-prefix impact checks still apply. Lower extensions, old windows ending before that cutoff, General snapshots and Item/transitive timing retain their existing proof requirements. Mixed baseline/verified rows do not by themselves block a proven future extension.

Frozen pruning reads checksummed captured bounds only. Legacy receipts retain broad reads; changing live summaries cannot change historical values. Empty typed Item windows retain their captured schema; another month first establishing a physical schema adds no values to that empty window.

`inputs.verify` also accepts a nonempty finite sequence of original frozen
receipts. Each root identity/digest and every dependency edge are checked;
shared parents and original batches are checked once within this invocation.
The multi-root report lists original root IDs/digests and aggregate counts.
There is no new aggregate receipt or cross-call validity cache; a later call
rechecks bytes. Currentness remains a separate check.

## Incremental producer execution

Only original complete-range publication witnesses establish completed coverage;
ordinary `items.ingest` proves its published rows, never absence at other keys.
Whole-range certificates bind original build metadata and an explicit pending
visibility set, including an empty set. Output becoming visible after the saved
cutoff repairs its original observation date; it is not silently lost at append.

Historical completion maintenance is explicit `items.index_completion` with an
`ItemCompletionAttestation`. The caller retains an original whole-range producer
report and its SHA256 reference, complete membership/withdrawal/empty-range
authority, and all pending coordinates at that original cutoff. The attestation
must exactly match the original build, definition, frozen input digest/bounds and
result commit. This never derives completeness from row bounds/counts or an audit.
An indexed complete attestation can precede an empty sparse no-op continuation;
bind `output_receipt_id` and `output_receipt_digest` to the original full-range
output snapshot. Its retained check ceiling excludes every later timing check;
maintenance never absorbs later checks or infers their order from timestamps.
Its original authority and pending coordinates remain immutable. All source,
definition and output/check fences still apply. Changed sparse ranges require a
native predecessor.
It writes immutable proof metadata atomically and retains the old result boundary;
later publications still invalidate currentness. Missing original report or
pending knowledge remains blocked. Ordinary readiness never invokes maintenance.

`items.update` always uses the owner plan, including explicit `start`,
`input_windows` and `force`. Successful range certificates establish completion
even for empty results. Unchanged evidence returns without freezing or invoking
the producer; causal producers append only new dates and repair proven affected
intervals. Missing contracts/proofs, changed dependency definitions, coverage
holes and unproven complete-history impacts return `blocked`. Initialization
and changed numerical producer definitions permit full requested computation.
`force` only recomputes planned units; it does not turn a no-op into cold replay.

`ProducerUpdatePolicy("same_date")` declares pointwise output dependence.
`"forward"` declares a causal suffix. A finite `history_observations` additionally
requires a complete General `calendar_input`, `calendar_field` and optional
`calendar_selector=(field, integer_value)`. Its count follows admitted calendar
coordinates, including dates with missing producer rows. The policy is runtime
evidence, outside the persisted numerical definition. Select/Filter/MapValues/
Cast chains that preserve observation coordinates have a built-in pointwise
contract; joins, alignments and coordinate remaps require additional proof.

Typed consumption records the complete receipt-bound calendar projection.
Historical receipts missing that optional proof require explicit
`inputs.index_coordinates(receipt, alias, field="time", selector=("is_open", 1))`
maintenance for finite historical repair. Pure append does not require the old
projection. Ordinary plans never perform this maintenance or decode original
inputs. `inputs.verify` compares coordinate projections and captured read bounds
with retained original evidence.

Build contexts distinguish the requested output interval, information cutoff
and `completed_through`. Finite producers read the required preceding coordinate
halo; pointwise producers read only output dates. Nonempty native inputs retain
the baseline markers selected at each information cutoff over that consumed
window; timing checks reuse those snapshots without rereading unrelated history.
Empty inputs still follow their original transitive timing proof, including any
parent halo or complete General event. External publications continue to prove
timing over their full declared requests. Strict PIT reads preserve the actual
first-known boundary rather than inheriting unconsumed historical baselines.
The operation pins its spec, callable and runtime policy through planning,
execution and revalidation; cancellation cannot certify unfinished full coverage.

`inputs.freeze(..., retain_batch_bounds=True)` preserves every original batch
and captures physical bounds even for full requests. New item builds use it to
skip impossible original Parquet segments. Legacy receipts retain broad replay, and live
summary changes cannot alter captured reads. These additional physical bounds
do not change full-source request identity or numerical dependency digest.

A `timing_neutral_inputs` declaration permits ignoring repeated same-content
General attestations when the original selected snapshot was already known and
keeps the same sequence and nonbaseline timing at every new boundary. The proof
also covers repeats within the saved cutoff when they preserve the original first
known date; an earlier first-known date remains a historical timing change.
It is local to that alias; other dependency revisions are planned separately, and
unrelated missing indexes do not invalidate the calendar proof. First promotion
normally requires current past input evidence, uniform output baseline commits and
a visible baseline surviving each new timing boundary. When that last baseline
becomes verified, the narrow saved-value path below handles the timing change.
Other unproved timing changes remain blocked. New General contents require
selected-row impact proof when historical output coordinates exist.

For completed native producers whose whole General Raw inputs all declare
timing neutrality, unchanged selected snapshots and schemas can prove the first
aggregate baseline-to-verified boundary. With no pending outputs or numerical
repair, `plan_update` returns an `attest` action with `start`, `end` and
`available_date`, followed by ordinary append actions. Planning reads metadata
only, including checks already captured beyond the old information cutoff.

`items.update` reads saved outputs at their original commit/check boundaries in
monthly units and uses the existing unchanged-content publisher to retain the
values while recording actual later verification. It does not call the producer
for historical dates or rewrite numerical bodies. Earlier snapshots and causal
history retain their original baseline evidence; strict later snapshots can use
the newly verified values. Each timing check and unit certificate commits in one
transaction. Retry skips completed units. The prefix is certified only after all
units succeed; the extended range is certified after its numerical append.
`recertify` rejects this work because it changes timing and consumes saved values.

Bounded reads discard first-visibility events whose registered observation bounds
are disjoint before opening their Parquet or joining the saved baseline. A monthly
timing unit therefore does not reread witnesses for previously completed months.
Unknown bounds retain typed event reads; overlapping events retain their original
check/commit ceilings, availability dates and natural-key matching. Stored date
extents alone do not establish completed verification or current dependencies.

Saved-output admission uses original row/schema/file metadata and Parquet footer
sizes before collection, then checks the collected working set. These are soft
allocation estimates, including variable-width channels, rather than hard RSS
bounds. Oversized units fail explicitly without replaying producers; callers may
retry with a larger admitted buffer. Changed content, undeclared timing effects,
pending outputs, mixed input kinds and damaged proof remain blocked.

A producer that consumes only specified General columns can declare the
immutable runtime policy below, without changing its numerical DataItemSpec:

```python
policy = ProducerUpdatePolicy(
    "same_date",
    timing_neutral_inputs=("inventory",),
    general_fields=(("inventory", ("asset_id", "category")),),
)
```

Each alias must be a declared whole General Raw input. A field proof requires a
selected atomic snapshot, including typed empty; no eligible event cannot borrow
a current or union schema. Captured consumed-field dtypes must also agree;
mixed or unknown schema authority gives an actionable blocked plan whenever an
atomic event is selected, including future-only schema changes before no-op
reuse. An unchanged historical completion without a selected event retains its
existing metadata evidence rather than claiming a field-absence proof. For an already-known
nonbaseline snapshot, identical registered typed projections permit historical
reuse when only unconsumed columns changed. The plan checks each new/captured
future atomic commit, including several commits at one PIT date, and relevant
visibility boundaries. Missing proof gives exact original/current
`inputs.index_selection` maintenance with max_commit/as_of guidance; ordinary
plans never read originals or row indexes. Actual consumed changes or first/
earlier visibility remain blocked for explicit impact resolution, with no silent
full replay. Baseline promotion, output anchors, other inputs and closing fences
retain their independent contracts. `update`/`recertify(config=...)` carry the
caller budget into this metadata planning and no-op binding; read-only
`plan_update` inherits an explicit `inputs.read_context(config=...)` budget.

Same-date producers may declare `output_anchors=("daily", "suspensions")`.
For a per-asset General lookup, they may also declare
`general_asset_local_inputs=("inventory",)`. This promises a lookup affects only
its output row's nonnull asset key, with no global/count/cross-asset effect and
no NULL-key General row influence on nonnull outputs. Consumed `general_fields`
must include `asset_id`. The declaration alone never permits reuse.

Explicit preparation uses the same General API:

```python
lake.inputs.prepare_general_projections(
    original_source, "inventory", ("asset_id", "category"), through=target,
    key_anchors=("daily", "suspensions"),
    completed_start=first_completed_day, completed_end=last_completed_day,
)
```

The compact proof requires all old typed consumed tuples unchanged, including
NULL-key tuples and multiplicity, and only new unique nonnull String keys.
Both selected General states must be known and nonbaseline, with unchanged
definition and consumed schema. Every new atomic commit is checked; a later
commit at the original PIT day is eligible, but backdated/unknown visibility is
not. Each added key must be absent from every completed-prefix anchor at both
original and current frozen ceilings. Exact snapshot coordinate selection
retains extra natural keys and NULL withdrawals; a NULL numerical value is still
a present coordinate. Old field changes, removals, nullable tuple changes,
historical anchor arrivals and unsupported scopes remain blocked. Normal
planning and known repeat preparation read metadata only. Explicit `verify`
follows both original Sources and recomputes this proof from Original bytes.
The existing native update then computes only its planned tail; no alternate
publication path, row index or schema migration is required.
The descriptor key binds every current anchor's selected evidence, request and
cutoff as well as the General state. A full current state and an earlier atomic
state keep separate proofs when sparse visibility witnesses differ. Unrelated
global publications preserve known reuse. Existing General-only descriptors
remain explicitly auditable under their own saved Sources; ordinary planning
never falls back to them. Scoped descriptor body/key bindings must agree.

Every output coordinate must belong to the union of these registered by-date Raw
inputs; invalid declarations fail before upstream work, and producer output is
checked before publication. An interval proven empty in every anchor can reuse
its original completed empty result despite General content or timing changes.
The proof checks original captured headers and all possible current coordinates.
Unknown bounds, late historical coordinates or a nonempty second anchor prevent
reuse. This establishes unchanged output impact, not equivalent input receipts.

Read-only `items.plan_update(..., update_policy=ProducerUpdatePolicy(...))` may
bind an external producer's causal declaration without registering code or
writing the catalog. It must agree with any registered runtime contract; neutral
built-in transformations keep their own authoritative policy. Optional
`inputs.selection_identity(..., values_only=True, coordinates=keys)` accepts
unique typed Date `time` / String `asset_id` keys, proving the consumed scalar
projection including absent keys. It applies visibility/version selection before
restricting keys, binds canonical coordinate identity, and returns unknown when
proof indexes or the admitted memory budget are insufficient.

`inputs.latest(receipt, alias, through=..., config=..., validate_rows=...)`
consumes serial monthly windows and returns one last scalar observation per
asset. The observation boundary keeps the original frozen information cutoff,
including late revisions. Captured physical bounds and finite state admission
are required. A consumer may validate/filter each typed window before reduction;
its numerical admission policy remains with that consumer. Use this read only
for initialization or explicit checkpoint maintenance, never for prerequisites.
Canonical scalar summaries reuse an earlier cutoff only when captured bounds
and all witness dates prove the complete selected event stream already visible;
unknown bounds or empty baseline authority retain the exact cutoff.
New batch indexes also record the maximum availability delay from observation.
With identical original content/witness headers, this optional audited bound can
prove a partial batch fully visible inside a shorter observation window, without
decoding its row index or substituting the observation date for the real cutoff.
Missing bounds stay unknown; complete Source freshness remains a separate proof.

Multiple native completion scopes may certify a visible selection with an empty
head and retained tail. Every captured creator occurrence, including empty
checks, must be wholly covered and respect result commit and output-check bounds.
Each replacement Source remains subject to ordinary currentness. Unknown bounds
or uncovered occurrences remain stale; original receipts stay immutable. The
check prunes unrelated ranges and admits metadata within the caller buffer budget.
This also applies to an unbounded snapshot or versions request that captured a
partial build: that build proves its own range, while other native scopes must
cover every remaining occurrence. It never becomes a whole-range certificate.

A terminal Item with no declared or captured upstream Sources needs no selected
value index to establish upstream freshness when exact metadata equality, no
pending outputs and a complete visible nonempty batch are proved. Empty, unknown,
strict-excluded, future and outside-window selections retain normal requirements.

For an original native completion that predates recorded output-check ceilings,
explicitly call `items.index_completion_output(build_id, output_receipt_id=...,
output_receipt_digest=..., report_id=..., report_digest=...)`. Supply the original
published Frozen output and producer report, never a newly frozen substitute.
An immutable supplementary index binds the original native authority, pending
proof and captured output-check ceiling. Existing native records and numerical
history stay intact; missing/foreign scope or later timing evidence is rejected.

A group of empty sparse continuations with one frozen Source and disjoint outputs shares one fenced metadata currentness check. Each output still validates its original coverage, authority, pending visibility, definition and output/check bounds. These no-op completion records commit atomically; cancellation or failure rolls back the group and releases its leases. No historical values or versions are republished. Groups containing changed ranges or upstream output identities retain ordinary publication semantics.


## Compact storage and readiness

Schema 9 stores every historical value and first-visibility event once in original
immutable Parquet. SQLite holds declarations, compact file/change metadata, node
completion flags and frozen references. `range_identity` and ordinary
`selection_identity` use metadata without original reads or index preparation.
Optional exact `index_selection(s)` and small coordinate axes are explicit;
ordinary identity stays unchanged by preparation. `inputs.verify` audits exact
original Parquet bytes, typed schema, bounds and summaries with bounded readers.
Damaged or missing originals require a trusted backup.

A visibility overlay shares its observation-bounded original natural-key
projection within one collection, so witness branches do not encode original
rows repeatedly. This temporary sharing expires with the collection and does
not change typed keys, visibility dates or frozen evidence.

Raw and Item `description` is retained in complete declarations and exports,
while numerical definition, dependency and range identities exclude it.
Description edits preserve completion flags and reuse existing calculations.
All other declaration fields, including arbitrary Item metadata, remain
authoritative for numerical currentness.

`complete_at` and `replace_range(..., available_date=...)` advance the checked
target to the acquisition cutoff while keeping observation coverage separate.
They retain equal content's original publication dates and bytes. Explicit
historical dates in an ingest/publication frame remain semantic PIT versions.
