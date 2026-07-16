# Updates

Run one dataset, a list of datasets, or every enabled dataset for a source.

```python
lake.update.dataset("daily", source="tushare", today="2026-07-10")
lake.update.datasets(["trade_cal", "daily"], source="tushare", start="19991231")
lake.update.source("tushare", end="2026-07-10")
```

`by_daily` uses its `calendar`; `by_asset` uses its `asset_list`. The lake
deduplicates incremental records by their derived primary key before writing
their partition files.

Incremental updates default to a fallback start of `1999-12-31` and an end of
today. Existing daily datasets and assets resume on the day after their latest
local date; the supplied `start` is used only when no local data exists. Compact
`YYYYMMDD` dates are accepted.

`datasets()` and `source()` plan and display their jobs before asking whether to
run all incremental jobs, daily jobs, asset jobs, a general refresh, or quit.
Pass `confirm=False` to run all daily and asset jobs without prompting. General
datasets are refreshed only through the explicit general choice or a direct
`dataset()` call.

`workers` is a global source-call limit for one update invocation. Requests for
different dates, assets, and datasets share the same thread pool, while commits
remain coordinated per dataset. Failed logical requests are attempted three
times, recorded as pending, and retried before newly planned work on the next
update. A persistent failure does not block later dates or assets:

```python
lake.update.source("tushare", workers=8)
lake.admin.status.pending_update_jobs(source="tushare")
```

Use `batch_size` to control how many successful logical requests are combined
per incremental commit. The default is 100.

The scheduler keeps a bounded number of calls queued. By default this is twice
`workers`; use `max_in_flight` to override it. `max_buffer_mb` (default 256)
also flushes buffered results before `batch_size`, limiting memory use:

```python
report = lake.update.source(
    "tushare",
    workers=8,
    batch_size=100,
    max_in_flight=16,
    max_buffer_mb=256,
)
print(report.elapsed_seconds, report.commit_count, report.partitions_rewritten)
```

For Tushare, start with 4 workers and increase to 8 only when the account's
rate limits allow it. Larger batches reduce partition rewrites but retain more
downloaded data in memory. Update reports expose fetch, commit, and metadata
timings plus the peak queued call count for tuning.

Applications can observe the scheduler through `progress_callback` without
parsing terminal output. The callback receives an immutable `UpdateProgress`
snapshot when work is planned, when a retry or new-request phase advances, and
when a dataset finishes. Snapshots include the dataset, phase, completed and
total provider calls, success and failure counts, downloaded rows, and final
status. Pagination can increase the total while an update is running. Callback
delivery is synchronous on the scheduler thread. The existing `progress=True`
option continues to control tqdm terminal output independently.

## Completeness audits and confirmed execution

Use `lake.update.plan(...)` before a provider-backed update when the caller
must inspect the work before it starts. Plans are immutable snapshots of the
selected datasets, lake manifests, coverage records, and logical provider
requests. `lake.update.execute(plan, ...)` rechecks the plan fingerprint and
refuses stale plans rather than silently running a different download.

```python
plan = lake.update.plan(
    ["daily", "income"], source="tushare",
    start="2020-01-01", end="2026-07-10", audit="fast",
)
for summary in plan.summaries:
    print(summary.dataset, summary.missing, summary.estimated_calls)
report = lake.update.execute(plan, workers=4)
```

`fast` is the default audit used by normal updates. It plans forward work,
pending retries, provisional empty scopes, and gaps already known to the
coverage index. `full` compares the complete requested history: every open
calendar date for `by_daily`, and every listing-active asset-year for
`by_asset`. Full audits are intended for the initial coverage backfill and
periodic operator checks, not every routine update.

A successful provider response establishes coverage even when it contains no
rows. Historical empty scopes are retained so sparse datasets are not
downloaded forever. Today's empty daily response and the current asset-year
remain provisional and are reconsidered by later fast audits. Failed requests
never establish coverage.

Execution reports include changed partition identities and their before/after
content hashes. Applications should use those changes to invalidate downstream
calculations instead of treating every successful update as a full rebuild.

Pass provider-specific values for one run with `params`, for example
`lake.update.dataset("stock_basic", source="tushare", params={"exchange": "SSE"})`.
Per-run `params` override a dataset's configured `source_api_params` and
`source_api_param_sets`; planner-owned daily and asset keys continue to take
precedence. General datasets merge every expanded parameter-set response and
replace stored data only when all calls succeed.
