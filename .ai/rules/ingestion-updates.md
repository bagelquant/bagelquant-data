# Ingestion, availability and update scopes

Item manifest reads batch committed partition/version-bound metadata in a
constant number of SQL queries, including empty/unmatched partitions. Do not
perform one aggregate query per historical partition inside repeated currentness
checks or inspect payload rows to assemble these recorded bounds.

## Public contract and availability

- The library-only facades are catalog/raw/items/integrity/inputs. Raw supports
  only general/by_date. DataMetaStore stores one explicit data_meta_path SQLite;
  lake_path is also required. No compatibility aliases or default path discovery.
  Do not restore a CLI, `[project.scripts]`, interactive prompts, terminal
  progress or `tqdm`. Callers explicitly select datasets; progress is optional
  callbacks. Queries never launch an update or provider request.
- Mapping is explicit: `by_date` use canonical `(time, asset_id)`
  plus declared extra primary keys. Keep provider-specific adapters at the edge.
- Raw retains provider columns, `source_time` (observation/announcement), `time`
  (version availability), UTC `ingested_at` and committed ingestion identity.
- A negative availability offset requires explicit local `availability_cutoff_time`.
  Receipt at/after this exclusive cutoff rolls the local date forward before
  applying the offset. Do not reinterpret old committed versions when changing
  cutoffs; coordinate an explicit Raw/DataItem/signal/label cutover downstream.
- Historical initialization cannot invent first-publication evidence. A provider
  refetch cannot repair a historical baseline: only checksum-verified ingestion
  evidence can restore old bytes. Missing timing evidence remains unverifiable.

## Coverage, retry and transport

- A scope succeeds only after its canonical Parquet commit succeeds. Provider
  range checks are scheduling information, never proof of local coverage.
- A default General incremental request reuses an already verified complete
  snapshot for the same target and declaration. Prove the full parameter
  inventory from one successful atomic publication/check and its terminal
  scopes before discovery; never infer completeness from existing row counts.
  A new target, explicit refresh, reset/incomplete inventory or definition
  change retrieves the whole inventory. Retry and forward cohorts publish one
  General snapshot together; phase-local snapshots must not replace each other.
  Subset and parameter-dataset requests retain ordinary scheduling. Dynamic
  discovery is pinned by the original completed run within its target date.
  Dynamic reuse also requires the original publication's complete default-
  inventory marker. Older or overridden request records without this authority
  obtain one genuine complete update; never retrofit historical markers.
- Validated empties are durable. Current `by_date` incremental scheduling
  rechecks the latest `recent_recheck_days` natural days (default 3), including
  `success` and `empty`; older daily terminal scopes need explicit refresh/reset
  or definition change.
  Do not restore the obsolete 20-session empty-only retry rule.
- All-null non-key payloads are invalid by default. Sparse-event declarations
  may explicitly set `source_options.allow_all_null_payload=true`, with complete
  valid keys/dates. For `by_date` it automatically retries only `invalid` scopes
  with the exact all-null-payload error, one day at a time; no other invalid
  reason is reopened.
- Keep logical daily coverage separate from provider transport.
  `source_options.daily_range_backfill` compacts only untouched historical
  backlog or interrupted ranges with no result, grouped by variant/adjacent
  dates. Validate/adaptively bisect physical ranges, then map complete results
  to daily success/empty outcomes. Request counts count physical calls;
  outcome/progress counts count logical daily scopes.
- Calendar-month transport uses `daily_range_backfill.window="calendar_month"`
  and complete offset pages. Explicit `raw.refresh` can also group adjacent
  historical daily scopes by month; ordinary incremental rechecks stay daily.
  Refresh preserves retained versions and publishes new checks only after full
  pagination and validation. `initialization_scan` performs a complete paginated
  parameter inventory before filtering to declared source dates and publishing
  daily outcomes. Its optional parameter_dataset is an initialization-only
  planning prerequisite; cohort_size/target_param/separator configure transport.
  Never infer source-date coverage from a different announcement-date window.
  Interrupted inventory scans publish no premature empty/success coverage.
- Validate keys/date/asset identity, truncation and repeated pages before terminal
  outcomes. No-op checks preserve content generations. Cancelled updates preserve
  committed work and unfinished scopes remain resumable.

`integrity.reopen_raw_initialization` may reopen a prematurely completed by_date
Raw only at its original bounds/hash, without any lease (including expired),
unfinished run/scope, prepared version, incremental/refresh commit or verified
incremental check. It returns an audit receipt and resets current daily outcomes
to pending; it retains all committed versions/checks and frozen receipts.
Ordinary `raw.initialize` resumes the same bounds and appends unverified baseline
evidence. This cannot restore original publication timing or backdate incremental
history. Monthly transport also groups historical rechecks during initialization.

## Resource and progress ownership

- Provider fetch/validation and bounded partition preparation share one worker
  pool; no additional writer pool. Central SQLite metadata/coverage publication
  stays serialized. Join admitted writers before rollback so late writes cannot
  undo it. Share one total thread/memory budget with native libraries.
- Workbench owns global scheduler, hardware detection, runtime policy, task
  admission and native thread budgets. Data defaults to one worker and enforces
  caller-supplied worker/in-flight/batch/buffer limits, never probes machine RAM.
  Oversized native initial-range bundles split already-claimed logical scopes
  and retry within the same budget; even single-day leaves retain native range
  truncation/pagination safeguards. Discarded parent fetches count physical calls,
  never completed coverage. Irreducible oversized responses fail explicitly. Resource
  limits do not change content identity; report actual timings/bytes/peaks.
  DataItem revision admission is capped by the number of actual revision
  boundaries, so a single evaluation does not reserve phantom worker buffers.
- `batch_size` counts scopes, `commit_batch_rows` counts rows. Completed Raw
  publication batches also flush at commit_interval_seconds (default 30);
  complete general snapshots and unfinished full inventories remain atomic.
  Schema 8 persists provider/limiter/retry/prepare/claim/commit timings, physical
  attempts, discarded rows and buffer/partition metrics in run/API-call evidence.
- Planning/activity/heartbeat and completed counts are separate; pulses never
  advance completed scope/row counts. Cancellation preserves committed work;
  recovery never releases live ownership or advances watermarks after a failed
  publication. Provider limiter waits check cancellation and preserve independent
  endpoint/committed-scope state.

Required nonempty single-day requests and their first offset page retry transient empty provider responses
within the existing physical-attempt/backoff/cancellation budget. Persistent
empty responses remain invalid without coverage; sparse and pre-start permitted
empties do not retry. Pagination completion empties keep their transport meaning.

For a caller-declared historical General inventory,
`source_options.preserve_nonempty_history=true` requires the first response page
to remain nonempty for a variant with registered prior same-definition nonempty
coverage. Retry within the existing attempt/backoff/cancellation budget;
persistent empties invalidate the whole snapshot without carrying old rows into
a new response. Previously empty/new variants and pagination end pages remain
valid empties. A genuine complete withdrawal requires explicit verification and
an update with this runtime guard disabled; it is not silently accepted as deletion.

## Read-only update plans

Calculation changes must plan from successful completed range certificates and
registered input/version changes. Normal prerequisites use metadata and optional
indexes; original Parquet audits and proof maintenance are explicit operations. With an
unchanged numerical definition, compute only new or affected output coordinates;
read the producer's necessary lookback/state separately from output ranges.
New cutoffs, enclosing receipts and global commit/check counters alone never
justify full-history computation. Full computation is initialization or a
relevant numerical definition change. Unknown impact/proof is an actionable
planning gap, not permission for implicit replay or invented reuse.

Producer/transform impact declarations must preserve PIT revisions, deletions,
availability and empties. Retry/cancellation retains completed units without
certifying an unfinished range. Test append/local repair against a full reference
and instrument dates/calls to prove unchanged history is not recomputed.
Record remaining implementation gaps separately from these requirements.

Implemented Item updates use one plan for every public update entry, pin the
numerical spec/callable/runtime policy, and certify full completion only after
every planned unit succeeds without cancellation. Runtime ProducerUpdatePolicy
declares pointwise/causal finite-calendar impact outside numerical identity.
Finite repair uses complete receipt-bound coordinate indexes, including empty
dates; missing old proof requires explicit inputs.index_coordinates maintenance.
Native nonempty inputs derive timing from their already selected consumed
snapshots at each information boundary, including the declared finite halo.
Do not reread a full receipt or propagate unconsumed historical baseline flags
into a bounded verified tail. Empty inputs retain their original transitive
timing proof; their parent requests may require a halo or complete General event.
External Item publications retain whole-request timing admission. Neutral
General timing checks require a registered numerical/trace-neutral contract.
A pointwise native input window may extend only its upper observation bound
without a uniform-baseline proof when both frozen and current declarations are
the same `by_date` Raw, every non-window request field and the lower bound remain
unchanged, and the old upper bound covers the original information cutoff.
Canonical Raw versions and first-known overlays are never available before
their observation, so added observations cannot change earlier selections.
At the original commit ceiling, retain exact complete empty-check authority
through the original check ceiling; missing/different evidence keeps the timing
guard. This proves only the scope extension: ordinary completed-range impact
checks still reject changed historical values/timing. Lower extensions, earlier
old upper bounds, General and Item/transitive inputs retain their existing
positive timing requirements; empty row counts alone prove nothing.
An already-known selected snapshot uses alias-local sequence/timing evidence;
same-cutoff repeats preserve the original first-known date. Backdated first
visibility remains a historical timing change requiring explicit impact proof.
other dependency changes remain independently planned. First baseline promotion
requires current past inputs and uniform output baseline surviving every new boundary. New
General content and unproved timing changes require explicit selected-row impact
proof when completed coordinates exist. A same-date producer may declare
`output_anchors`: its output coordinates must belong to the union of these
registered by-date Raw inputs. Validate this contract before upstream computation
and before publication. Only proven absence of coordinates in every anchor
permits reuse of a closed empty historical interval across General changes.
Bind original captured batch headers; unknown bounds or late historical anchor
coordinates prevent this shortcut. Empty output row counts do not prove absence.
This is output-impact authority, never source receipt equivalence.
Runtime `ProducerUpdatePolicy.general_fields` may declare immutable
`((alias, (field, ...)), ...)` whole-General consumption outside numerical
identity. The producer must consume precisely those columns and independently
declare timing neutrality. For an already-known nonbaseline snapshot, registered
typed summaries may prove that new content changes only unconsumed columns.
Field proofs require a selected atomic snapshot (typed empty is supported),
never a current/union schema for no eligible event.
Every captured atomic schema must agree on consumed-field dtypes; conflicting
or unknown authority yields an actionable schema block before numerical work.
Compare every new/captured-future snapshot at its original commit ceiling,
including several snapshots at the same PIT date, and every relevant visibility
boundary. Bind the selected original schema, full row multiplicity and complete
request. Missing summaries require explicit original/current index_selection
maintenance at the named max_commit/as_of; ordinary planning never decodes
originals or row indexes. Actual consumed changes, first/earlier visibility,
baseline promotion or unsupported scopes remain actionable blocked plans, not
full-history replay. Output anchors and other dependency impacts remain separate.
Direct update/recertify config and an explicit plan read_context both bound
General metadata/summary admission; resource settings never change proof identity.
Runtime `general_asset_local_inputs=(alias, ...)` is a behavioral contract only
for same-date producers with consumed `asset_id` and complete `output_anchors`.
Each General lookup affects only its output's nonnull asset key; NULL-key rows
must not affect nonnull outputs, and global/count/cross-asset effects are excluded.
Explicit `inputs.prepare_general_projections(..., key_anchors=..., completed_start=...,
completed_end=...)` may register a compact positive descriptor in the existing
summary store. Require known nonbaseline original/current events, unchanged
definition/consumed schema, exact old typed consumed multiset (including every
NULL tuple and multiplicity), and only unique new nonnull String keys. A later
commit at the original PIT day is eligible; an earlier/unknown date is not.
Check every intermediate atomic state, including A->B->A. Prove each new key
absent from every original and current completed-prefix by-date Raw anchor via
complete exact snapshot coordinate summaries: extra natural keys and NULL
withdrawal payloads still count as present. Bind both Sources, all anchor
definitions/schemas/request ceilings and the complete bounded date-key selector.
Planner admission reads only bound descriptors/summary headers; missing, stale,
corrupt or unsupported evidence blocks without old numerical replay. Unchanged
prepared states and repeat preparation do not read Original rows. Explicit
verification follows both Sources and recomputes typed additions and every
anchor absence; this new descriptor kind is never treated as a scalar hash.
No new table, row index, receipt format, numerical edition or alternate native
execution path is introduced. Other dependency/timing/publication fences remain.
The positive descriptor locator also binds each current anchor's exact selected
evidence, snapshot request and cutoff. Identical General content under a full
current ceiling and an earlier atomic ceiling cannot overwrite different sparse
anchor witnesses. Use neither whole-Source UUIDs nor global commit/check counters
for this namespace; unrelated publication preserves known metadata reuse.
The descriptor body repeats that scoped namespace, and ordinary preparation and
admission exclusively use scoped locators. Existing General-only descriptors can
be explicitly audited as historical evidence at their two saved Sources: exact
old locator/body, typed changes, selector and every child binding are recomputed.
They never admit a current plan. Removing/rebinding the namespace on a scoped
body is damaged evidence, not a historical-audit fallback.
Force does not replay no-op history. Initialization/definition changes are full
computation; unchanged producers execute only planned new/affected units.

A proved unchanged update whose original whole Source is stale may publish a
new immutable native completion against current frozen inputs. Repeat the full
causal-impact plan under the Data writer fence, preserve pending coordinates and
all old builds/creator receipts, and bind original/current sources, predecessor
completion and producer policy. Fence definitions and output/check boundaries;
cancel after metadata preparation rolls back completion. Already-current no-ops
do not freeze or write new completion. This does not weaken Raw currentness.
Explicit `items.recertify` uses this same no-op path with numerical work
forbidden: reject any new/changed/unknown plan before input value reads or
producer invocation, and never recursively update dependencies. Callers
recertify dependencies first and supply their actual immutable causal policies;
conflicting registered policies are rejected. Bind the admitted definition to
the caller's expected declaration and internal spec capture, and repeat policy/
definition fences before publication. A runtime producer key/revision's causal
policy is immutable across registration. Closing fences still apply.
First verification of unknown by-date baseline timing uses retained natural-key
visibility events. Known repeated or earlier checks do not backdate first
publication or create duplicate events. Timing-neutral producers may ignore
already-known checks; actual new visibility impact begins at its availability
boundary. General first visibility requires its atomic timing contract. Compact
check-scope authorities can rebind equal Item outputs to a new producer Source
only for complete matching original natural-key sets, without value copies or
whole-range completion claims.

Impact queries select compact segment/change metadata explicitly and never read original Parquet values.
Explicit `items.index_output_reuse` audits later output versions against original
completed channels and records batch-bound affected dates. Creator-only duplicate
publications need no producer replay when this proof is available; independent
input changes still follow ordinary impact planning. Preserve physical availability,
same-availability revision precedence, baseline and full scalar/trace channels.
Explicit maintenance audits every original batch's typed date bounds before pruning
reference candidates, binds the complete original batch metadata set, and fences
that binding at publication and subsequent record-only plans. Never trust an
unaudited original header to exclude a superseding value from the reference.
Missing/expired records remain conservative, corrupted bindings fail.
An increased cutoff preserves a batch's proof when its registered maximum physical
availability was already covered by maintenance and its own check boundary is unchanged;
otherwise newly visible versions require further proof. Actual affected coordinates
remain filtered by the requested visibility cutoff.
Output timing attestations require further selected-version proof. No ordinary path invokes this
maintenance or infers Source equivalence from output equality.

Raw plan_updates and Item plan_update use durable initialization and committed evidence,
never row counts. Resume original unfinished bounds/definition before incremental extension.
Raw plans order calendar/parameter prerequisites. No provider calls or metadata writes occur.

DataItem revision evaluation and initial monthly partition preparation share one
caller-bounded pool. First publication admits writers within the unused
previous-revision buffer reserve using conservative per-partition copy estimates;
existing partitions and later revision publications remain serial. No second
writer pool or hardware-based allocation is created.

Existing internally proven unverified Item baselines may publish independent
coordinates in caller row/byte bounded batches, retaining each row's availability
date. First publication remains atomic. Verified witnesses, repeated coordinates
and mixed timing proofs retain chronological date checks: combining a verified
check at the batch maximum would delay frozen cutoff visibility.

Item publication checks cancellation between bounded commits, reports retained
row counts after each commit and drains the shared pool before returning. A
canceled partial publication writes no completed Item-build certificate.

Pure empty sparse groups with one Source, exact original coverage and no affected ranges may share one record-only Source check under a single writer transaction. Data atomically acquires all output leases and retains each original definition, completion, pending, output/check and cancellation fence. Exclude every captured/resolved or registered transitive upstream output from this optimization. Failure rolls back all new group metadata; numerical publications retain their ordinary per-item retry semantics. The private transaction capability expires at exit, and metadata admission uses the caller buffer budget.
