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

Scope statuses remain compatible with `pending`, `running`, `success`, `empty`,
`failed`, and `invalid`, but new empty responses do not create `empty` local
coverage. A scope with no committed data returns to `pending`; its separate
provider check controls when it is eligible again. Successful current-day daily
scopes are checked once more after the date becomes historical. Invalid
responses require an explicit operator reset.

For `by_asset`, normal work starts after the provider-check watermark. Once every
`revision_refresh_days`, the request also includes the preceding
`revision_lookback_days`, allowing later provider revisions to upsert canonical
records without repeatedly downloading the full history.

## Commit and failure semantics

Provider calls share one bounded thread pool. Results are committed per dataset
on the scheduler thread. A nonempty scope becomes successful only after the
corresponding Parquet batch commits. If the write fails, the scope becomes
failed and its local watermark does not advance. Validated empty responses
update only the provider-check record and API-call audit. They do not increment
`success_count`, update `last_success_at`, or move `data_max_time`. Reports expose
them as `empty_count`; an all-empty valid run completes with no local data change.

Set `historical_empty_is_error = true` for a dense `by_daily` dataset whose
historical open-date request must contain rows. An unexpected historical empty
then follows the configured retry policy and aborts the batch as a retryable
failure after retries are exhausted. Current-day empties remain provisional.

Each selected dataset has a writer lease. A second process cannot update that
dataset until the first process finishes or its lease expires. Stale running
scopes are recovered on the next invocation.

Failed physical calls are retried three times within one invocation. A
persistent failure is stored in the scope ledger and retried by a later update;
it does not block unrelated scopes.

Use `batch_size` to control successful requests per commit, `max_in_flight` to
bound queued calls, and `max_buffer_mb` to limit buffered frames:

```python
report = lake.update.source(
    "tushare",
    workers=4,
    batch_size=100,
    max_in_flight=8,
    max_buffer_mb=256,
    confirm=False,
)
```

Applications can observe `sync`, `claim`, `fetch`, `commit`, and `complete`
progress through `progress_callback`. Reports include changed partition hashes
for downstream invalidation.

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
lake.admin.status.reset_dataset_update_coverage(
    ["daily", "income"], source="tushare", clear_provider_checks=True
)
```
