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
- Validate keys/date/asset identity, truncation and repeated pages before terminal
  outcomes. No-op checks preserve content generations. Cancelled updates preserve
  committed work and unfinished scopes remain resumable.

## Resource and progress ownership

- Provider fetch/validation and bounded partition preparation share one worker
  pool; no additional writer pool. Central SQLite metadata/coverage publication
  stays serialized. Join admitted writers before rollback so late writes cannot
  undo it. Share one total thread/memory budget with native libraries.
- Workbench owns global scheduler, hardware detection, runtime policy, task
  admission and native thread budgets. Data defaults to one worker and enforces
  caller-supplied worker/in-flight/batch/buffer limits, never probes machine RAM.
  Oversized responses must stream within limits or fail explicitly. Resource
  limits do not change content identity; report actual timings/bytes/peaks.
- Planning/activity/heartbeat and completed counts are separate; pulses never
  advance completed scope/row counts. Cancellation preserves committed work;
  recovery never releases live ownership or advances watermarks after a failed
  publication. Provider limiter waits check cancellation and preserve independent
  endpoint/committed-scope state.
