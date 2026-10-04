# Updates, versions, and recovery

Use `lake.update.dataset()` or `lake.update.datasets()` with an explicit source.
The modes are `initialize`, `incremental` (default), and `refresh`.
Initialization requires a frozen start/end and a new dataset, or the same unfinished
initialization range and definition. A completed initialization cannot be repeated.
Refresh rechecks the explicit range and appends new versions when content changes.

A successful initialization uses source dates for the historical baseline. Later
versions use `max(source_time, local ingestion date + availability_day_offset + rollover)`.
With an explicit local `availability_cutoff_time` (`HH:MM:SS`), rollover is one
at/after the cutoff and zero before it. Negative offsets require a cutoff.
Workbench uses the next-open convention: offset −1 and an Asia/Shanghai 09:30
cutoff prevent revisions received at or after the open from entering the
previous signal date. Calendar dates
are not exchange sessions; consumers align them forward to open sessions.
`ingested_at` is always UTC.
Same-day versions are ordered by ingestion timestamp and commit sequence. Empty
responses do not delete records. Identical responses record a check referencing the
visible commit; they do not change manifests or downstream content generations.

Coverage is one date × parameter variant. Natural dates include weekends; trading
sources use the declared calendar. Every update fills unfinished scopes and rechecks
the latest three natural days through its target. All pages must pass key/date/asset,
truncation, and repeated-page checks before a scope becomes `success` or `empty`.
Cancellation preserves completed scopes and leaves unfinished scopes resumable.
Four workers are the default. Provider fetch/validation and partition writes share
one executor bounded by `workers`; there is no additional writer thread pool.
Changed independent monthly partitions are prepared and written in batches of
at most four tasks. Only the scheduler publishes `lake.db` metadata and coverage.
All admitted writers finish before any failed batch is rolled back, so a late
writer cannot undo rollback. Reports expose `bytes_read` for old canonical
partitions, `bytes_written`, and `peak_partition_in_flight` (queued plus running
partition tasks) alongside commit time. These are operational counters, not
content identity. Admission, retry, and pagination share the
provider limiter; Tushare's global ceiling is 500 requests/minute with lower endpoint
limits applied as well. Applications never launch an upstream update from a query.

Use `uv run python scripts/benchmark_updates.py --workers 1` and repeat with
`--workers 2`, `4`, and `8` for a synthetic throughput comparison. The benchmark
uses temporary lakes and fake providers; compare elapsed/commit time, partition
task counts and bytes, keeping all dataset sizes and native thread settings fixed.

Each availability month contains `data.parquet` and `recovery.sqlite`. The journal
holds immutable compressed Arrow batches with schema, hashes, and commit identity.
The publication order is journal, Parquet, then the `lake.db` commit and coverage.
Unregistered prepared batches are never visible. Neither journal nor Parquet history
is automatically pruned. General partitions use snapshot month and a unique snapshot
ID, independently of business dates inside the snapshot.

```python
lake.admin.recovery_status("income", source="tushare", deep=True)
lake.admin.repair_partitions("income", source="tushare", partitions=[
    "year=2026/month=09/data.parquet",
])
```

Local repair replays only registered batches and preserves PIT dates, ingestion
identities, content hashes, and coverage. A broken journal can be rebuilt only when
the verified complete Parquet reproduces every original batch hash and schema.
Insufficient evidence on both sides blocks recovery. A provider's newest data is
never substituted for a lost historical version. Missing date scopes may be fetched
by an explicit normal update; later historical refreshes are new PIT observations.
There is no provider-backed `baseline_repair` option. Missing coverage cannot
authorize backdating a newly fetched value or replacing an existing baseline.

Old metadata schemas are rejected before database writes. Back up and explicitly
rebuild an incompatible lake; there are no automatic migrations or compatibility readers.
