# Storage, PIT queries and recovery

## Storage authority

- Use temporary output, validation and atomic publication. Keep canonical
  Parquet and SQLite metadata consistent under failures/retries. `lake.db`
  alone establishes committed visibility and coverage.
- Availability-month `data.parquet` and `recovery.sqlite` retain immutable
  Arrow batches, schemas, hashes and commit identity. Publish journal, Parquet,
  then central metadata/coverage; prepared unregistered batches are invisible.
  Never automatically prune recovery/history.
- Preserve stable year/asset-bucket partitions for `by_asset`; changing bucket
  count after canonical data exists requires explicit rebuild.
- Schema v4 is a fresh-lake hard cut. Reject old/unversioned databases before
  writes; never silently migrate, repair, delete or adopt orphan files.
- Canonical columnar hashes cover schema, values and null positions. Preserve
  canonical row order, including normalized trailing validity bits, rather
  than using physical scan order as evidence.

## Point-in-time queries

- Availability `start`/`end` and observation `observation_start`/`observation_end`
  are separate. Select versions at `as_of_date` before projecting the observation
  axis; an observation-window endpoint never replaces the information cutoff.
- `lake.query.frozen()` / `frozen_raw_reads(root, max_commit)` enforce an explicit
  committed-read boundary, including captured readers passed to workers.
  Passive reads never use current bytes in place of frozen inputs or call providers.
- Current freshness and historical integrity are separate facts. An unchanged
  check does not create a content version; empty replies never delete records.
  Same-day ordering uses ingestion timestamp and commit sequence.

## Integrity and recovery

- Deep validation/quarantine expose integrity facts and retained recovery
  journals. They do not guess provider scopes to reset; application orchestration
  decides resets. Orphan files are reported, never automatically adopted.
- Quarantine requires explicit intent, atomic same-lake moves, a metadata
  transaction, rollback and retained journal. Unknown/temporary/pending files,
  recovery evidence and Git metadata are protected from incidental cleanup.
- Local repair replays registered batches with exact PIT dates, ingestion IDs,
  hashes and coverage. Rebuild a journal only from complete verified Parquet
  reproducing every original hash/schema. Insufficient evidence blocks recovery.
- Never substitute current provider bytes, fabricate first-visible timing or
  backdate newly fetched values. Explicit later refreshes are new PIT observations;
  no provider-backed `baseline_repair` or automatic compatibility recovery.
- `rebuild_manifest` is an explicit external repair, not an automatic health
  operation. Retain backups/recovery journals during any authorized maintenance.
