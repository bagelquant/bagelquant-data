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
- `by_asset` creates one scope per asset and parameter variant. Its
  `checked_through` watermark records the end of the last successful provider
  check; `data_max_time` records the latest returned observation.
- `general` datasets remain explicit replacement refreshes and do not use
  incremental scopes.

Scope statuses are `pending`, `running`, `success`, `empty`, `failed`, and
`invalid`. Updates select pending and failed scopes. Successful current-day
daily scopes are checked once more after the date becomes historical. Invalid
responses require an explicit operator reset.

For `by_asset`, normal work starts after `checked_through`. Once every
`revision_refresh_days`, the request also includes the preceding
`revision_lookback_days`, allowing later provider revisions to upsert canonical
records without repeatedly downloading the full history.

## Commit and failure semantics

Provider calls share one bounded thread pool. Results are committed per dataset
on the scheduler thread. A nonempty scope becomes successful only after the
corresponding Parquet batch commits. If the write fails, the scope becomes
failed and its watermark does not advance. Empty successful responses need no
Parquet write and can be recorded immediately after response validation.

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
lake.admin.status.update_scopes(
    source="tushare", dataset="income", status="failed"
)
lake.admin.status.reset_update_scopes(
    source="tushare", dataset="income", statuses=("failed", "invalid")
)
```
