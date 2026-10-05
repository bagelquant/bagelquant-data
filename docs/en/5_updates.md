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
reserve space for response queues and publication batches; oversized responses
fail explicitly. A complete general snapshot must fit the supplied buffer.
Provider/native-library internal allocations require the caller's separate budget.

Use `progress_callback` and `cancel_check` for orchestration. Cancellation retains
committed work and leaves unfinished scopes resumable. Reports expose actual
counts, timings, bytes and in-flight peaks; those operational counters do not
change content identity. Workbench supplies local ceilings and native thread policy.
