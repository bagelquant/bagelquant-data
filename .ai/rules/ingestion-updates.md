# Ingestion, availability and update scopes

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
  Schema 7 persists provider/limiter/retry/prepare/claim/commit timings, physical
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

## Read-only update plans

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
