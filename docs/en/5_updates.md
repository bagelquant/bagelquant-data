# Updates

Run one dataset, a list of datasets, or every enabled dataset for a source.

```python
lake.update.dataset("daily", source="tushare", end="2026-07-10")
lake.update.datasets(
    ["daily", "income"], source="tushare",
    start="1999-12-31", end="2026-07-10", workers=4,
)
lake.update.source("tushare", end="2026-07-10", confirm=False)
```

## Authoritative update scopes

The metadata database owns a shared `update_scopes` ledger. An update first
synchronizes the selected dataset's expected scopes, then claims and executes
eligible rows. It never infers completeness from the maximum date in Parquet.

- `by_daily` creates one scope per open calendar date and parameter variant.
- `by_asset` creates one scope per asset and parameter variant.
- `data_max_time` and scope success are local, commit-backed facts.
- `provider_scope_checks.checked_through` records the end of a validated
  provider check independently from the latest returned observation.
- `general` datasets remain explicit replacement refreshes and do not use
  incremental scopes.

For a dataset with `request_discovery`, discovery runs once per explicit update
before scopes are synchronized. Its normalized values participate in the same
variant identity as static parameters, so daily and asset ledgers retain
independent recoverable scopes for every discovered value. The discovery call
is recorded in the target dataset's API audit with `request_kind = 'discovery'`.
If discovery fails, produces no values, or a general fan-out request fails, the
existing general dataset is not replaced.

Scope statuses are `pending`, `running`, `success`, `empty`, `failed`, and
`invalid`. Every validated empty response finishes the scope as `empty`, even
when no local rows exist. The separate provider check controls when that scope
is eligible again, while `data_max_time`, `last_success_at`, `row_count`, and
`commit_run_id` continue to describe only committed local data. Successful
current-day daily scopes are checked once more after the date becomes
historical. Invalid responses require an explicit operator reset.

For `by_asset`, normal work starts after the provider-check watermark. Once every
`revision_refresh_days`, the request also includes the preceding
`revision_lookback_days`, allowing later provider revisions to upsert canonical
records without repeatedly downloading the full history.

## Commit and failure semantics

Selected datasets run sequentially. Each dataset owns one bounded provider
thread pool, so `workers` controls concurrency inside that dataset and never
multiplies across datasets. Results are committed on the scheduler thread. A
single SQLite writer connection is reused on that thread, while every durable
outcome retains its own transaction boundary. A nonempty scope becomes successful only after the
corresponding Parquet batch commits. If the write fails, the scope becomes
failed and its local watermark does not advance. A validated empty response
writes the API audit (`result_kind = 'empty'`), provider check, `empty` scope
transition, and durable run `empty_count` in one SQLite transaction. It does not
increment `success_count`, update `last_success_at`, or move `data_max_time`.
An all-empty valid run has status `no_data` and no local data change.

Before publishing a partition, the lake hashes its sorted, rechunked Arrow IPC
logical content. If the existing manifest has the same hash, the Parquet file
and manifest row are left untouched while the scope, provider check, and run
still complete normally. `partitions_rewritten` therefore counts physical
writes, while `partitions_skipped` counts no-op partitions.

For every `by_daily` dataset, an update first rechecks existing `empty` scopes
that fall within the latest 20 requested trading sessions. These calls are
audited as `empty_recheck`. A repeated empty stays `empty`; a nonempty response
commits canonically and changes the scope to `success`. Failed scopes and these
recent empty scopes form a repair phase that finishes and commits before any
new forward work starts. Older daily empties and empty `by_asset` scopes remain
terminal until reset or a definition change. Empties first observed during a
run are considered for repair on the next run, not twice in the same run.

Each selected dataset has a writer lease tied to a workflow owner. A second
process cannot update that dataset until the first process finishes or its
lease expires. Scopes are claimed only when entering the bounded in-flight
queue. Cooperative cancellation stops new claims, settles completed provider
calls, commits completed nonempty buffers, preserves completed empties, and
releases leases. Owner cleanup after forced termination changes only genuinely
unfinished `running` scopes to retryable `failed`; committed `success` and
durable `empty` scopes remain unchanged.

Failed physical calls are attempted three times within one invocation with a
fixed, cancellable 60-second wait. A persistent failure is stored in the scope
ledger and retried before forward or revision work in a later update; it does
not block unrelated scopes.

Omit `batch_size` to commit when the dataset completes or its buffer reaches
`max_buffer_mb` (512 MiB by default). Setting `batch_size` explicitly keeps a
request-count commit boundary. Within retry and incremental work, requests are
ordered by physical partition affinity: daily scopes by month and asset scopes
by stable asset bucket. When a `by_asset` dataset has an empty manifest, the
initial build also commits at retry/forward and bucket boundaries. A bucket
smaller than the configured buffer is therefore written once; explicit
`batch_size` and `max_buffer_mb` limits can still split an oversized bucket.
Up to four internal workers hash, write, validate, and publish independent
Parquet partitions in parallel. One writer pool is reused across every commit
and sequential dataset in the update invocation. Incoming rows are
deduplicated once per commit, new partitions avoid unnecessary reads, and
coverage is aggregated once from the touched canonical partitions. Provider
workers also combine and validate each request before returning it to the
scheduler. Schema reconciliation, manifest publication, API audit writes, and
scope transitions remain serialized, and any writer failure settles the
remaining in-flight work before the whole batch is rolled back.
`max_in_flight` bounds queued calls:

```python
report = lake.update.source(
    "tushare",
    workers=4,
    max_in_flight=8,
    max_buffer_mb=512,
    confirm=False,
)
```

Applications can observe `sync`, `claim`, `fetch`, `commit`, and `complete`
progress through `progress_callback`. Reports include changed partition hashes
for downstream invalidation, `planning_seconds`, and separate rewritten and
skipped partition counts. `bytes_written` is the cumulative size of physically
rewritten Parquet files and is zero for a fully no-op update.

Pass provider-specific values with `params`. The normalized parameter variant
is part of the scope identity; ledger-owned date, asset, and range values still
take precedence.

Inspect and reset state through the status facade:

```python
lake.admin.status.update_summary(source="tushare")
lake.admin.status.provider_scope_checks(source="tushare", dataset="income")
lake.admin.status.update_scopes(
    source="tushare", dataset="income", status="failed"
)
lake.admin.status.reset_update_scopes([123, 124])
```
