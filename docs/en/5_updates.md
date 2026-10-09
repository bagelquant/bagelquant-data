# Updates and execution

```python
from bagelquant_data import ExecutionOptions

limits = ExecutionOptions(workers=2, max_in_flight=2,
                           batch_size=16, max_buffer_bytes=64 * 1024 * 1024)
lake.raw.initialize("daily", source="tushare", end="2020-12-31", config=limits)
lake.raw.update("daily", source="tushare", end="2021-01-15", config=limits)
lake.raw.refresh("daily", source="tushare", start="2020-12-01",
                  end="2020-12-31", config=limits)
```

Initialization defaults to `2000-01-01`; its end must be supplied. Persisted
range and definition identity allow an interrupted initial run to resume.
Changing either fails. Historical provider bytes cannot establish overwritten
original revisions; initial data is marked a historical baseline.

Read-only `raw.plan_updates(datasets, source=..., start=..., end=...)` returns ordered
`source`, `dataset`, `mode`, `start`, `end` actions. Calendar/parameter prerequisites
precede their consumers. `items.plan_update(name, start=..., end=...)` returns equivalent
item actions. New objects initialize; running initialization resumes its frozen bounds
before a later incremental action; completed or previously committed objects update.
Validated empty initialization counts as completed. Plans never fetch or publish, and
changed unfinished definitions/start or an earlier end are rejected.

Daily incremental updates recheck the latest three natural days by default.
Older terminal scopes need an explicit refresh/reset or definition change.
Declared date/parameter scopes determine coverage; row density does not.
Validated empty results are durable. All-null non-key responses are invalid
unless sparse-event declarations set `allow_all_null_payload=True` explicitly.

Raw keeps observation date `source_time`, availability `time`, UTC `ingested_at`,
content revision and commit identity separately. The exclusive local
`availability_cutoff_time` advances receipts at/after the boundary by one day
before `availability_day_offset`; negative offsets require an explicit cutoff.
Later identical content records availability attestations without another
content generation. A strict read can use only attestations visible by its cutoff.

Fetch/validation and partition preparation share one configured worker pool.
SQLite publication stays serial. Defaults: one worker, one in-flight request,
64 MiB buffer. Data never probes hardware or chooses global admission. Buffers
reserve space for response queues and publication batches. Oversized native
initial-range bundles discard their buffered bytes and bisect the already-claimed
daily scopes within the same budget. Single-day leaves retain range truncation
checks and pagination; discarded parent calls never advance coverage. A single
day or complete general snapshot that still exceeds the budget fails explicitly.
Provider/native-library internal allocations require the caller's separate budget.

Use `progress_callback` and `cancel_check` for orchestration. Cancellation retains
committed work and leaves unfinished scopes resumable. Reports expose actual
counts, timings, bytes and in-flight peaks; those operational counters do not
change content identity. Workbench supplies local ceilings and native thread policy.

`batch_size` counts logical scopes, while `commit_batch_rows` counts rows.
Completed publication buffers also flush every `commit_interval_seconds`
(default 30); an incomplete general snapshot never flushes early. Schema 7
stores run/API-call metrics, separating provider calls, limiter/backoff waits,
preparation, scope claims, commits and discarded parent rows.

Transport does not change logical daily coverage. The optional
`daily_range_backfill.window="calendar_month"` groups dates within each month
and retrieves complete offset pages. Explicit `raw.refresh` can use this same
monthly transport to recheck history, retaining old committed versions; ordinary
incremental rechecks stay daily. Use a page size no larger than the endpoint's
actual response cap. `initialization_scan` retrieves complete
history using offset pagination, optionally across parameter cohorts, then
filters by the declared source date and splits into daily outcomes. A caller
can supply `parameter_values`, or a `parameter_dataset`/`parameter_field` whose
complete local inventory is already available. `target_param`, `cohort_size`
and `separator` describe the provider transport. No daily completion is inferred
until the entire inventory has been retrieved and validated. Memory overflow,
repeated pages and wrong cohort assets fail explicitly without false coverage.

If an initial response was silently truncated, `integrity.reopen_raw_initialization`
can reopen the original completed bounds/hash with an explicit reason. It refuses
incremental/refresh commits, verified checks, leases (including expired leases),
unfinished runs/scopes and prepared versions. It returns an audit receipt and
preserves committed versions and frozen inputs. Resume with `raw.initialize`;
newly retrieved history remains an unverified baseline, never verified original
publication evidence. Monthly transport also supports these initialization rechecks.

`DataLake.inspect(runtime=True)` and `DataLake.open(runtime=True)` check committed
schema through ordinary read-only WAL coordination without copying the database.
Their default strict preflight preserves original database/sidecar bytes.

Required nonempty single-day requests and their first offset page retry transient empty responses within the existing physical-attempt budget. Persistent empties remain invalid; permitted sparse empties and pagination completion pages are unchanged.
