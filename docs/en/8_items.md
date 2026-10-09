# DataItems and external producers

A DataItem has unique deterministic `(time, asset_id)` keys and a declared scalar
`value` type: floats, integers, booleans, strings, dates, datetimes or categorical.
Definitions, dependencies and producer revisions persist in the Data SQLite.

```python
from bagelquant_data import DataItemSpec, RawInput, Select, Cast

lake.items.register(DataItemSpec(
    "close", (RawInput("tushare", "daily"),), value_dtype="float64",
    time_column="source_time", value_column="close",
    transforms=(Select(("source_time", "asset_id", "close")), Cast({"close": "float64"})),
))
lake.items.initialize("close", end="2020-12-31")
lake.items.update("close", end="2021-01-15")
```

Built-in `Select`, `Filter`, `MapValues`, `Cast`, explicit-key `Join`, and `Align`
operate on neutral frames. Duplicate output keys fail. Alignment uses caller
coordinates; missing values stay null. Forward filling requires a finite
`forward_fill_sessions` and explicit coordinate dates, respects availability,
and stops at a superseding null event.

Complex computations register an external producer by stable key/revision:

```python
def produce(context):
    return context.read("prices").select("source_time", "asset_id", "close")

lake.items.register_producer("my-prices", "revision-1", produce)
lake.items.register(DataItemSpec(
    "external", (RawInput("tushare", "daily", alias="prices"),),
    time_column="source_time", value_column="close",
    producer_key="my-prices", producer_revision="revision-1",
))
```

`BuildContext` contains frozen frames and range/cutoff/commit evidence, with no
Data/Core coupling. Producers must be registered again for execution after
reopen; definitions remain readable. Dependency receipts govern invalidation.
Unknown impact conservatively rebuilds the declared range. Removed output keys
publish tombstones; historical versions remain. Direct neutral results use
`lake.items.ingest`, and share the same commit/recovery protocol.

Direct output with declared dependencies must pass `input_receipt` from
`lake.inputs.freeze`; Data verifies the input identities and stores that receipt
with its commit. `expected_definition_hash` rejects definitions changed during
external computation. Without explicit availability evidence, direct ingestion
becomes visible at collection time. Historical baseline ingestion remains
explicit and strict reads exclude it. Complete external ranges use
`items.replace_range(name, frame, start=..., end=..., available_date=...,
input_receipt=...)`; missing coordinates receive null events without rewriting
old frozen history.

Related external outputs use `with items.publication(input_receipt=receipt,
config=ExecutionOptions(...), cancelled=callback) as publisher:` and
`publisher.publish(publications)`. The context verifies its fixed immutable
input proof once on entry and accepts serial groups without accumulating
all output frames. Each `ItemPublication(name, frame,
expected_definition_hash=...)` carries historical versions; supply `start`,
`end` and `complete_at` together to certify a complete range. Every group
preflights its target definitions, dependency identities and frames before
writing and uses the ordinary historical-ingest and complete-range commit path.
Returned reports describe each table's last suboperation, not cumulative rows.
Missing coordinates retain null events. Earlier commits survive later failure
or cancellation; publication is not atomic across output tables. A failed group
invalidates the operation even if caught. Successful context exit checks
cancellation again. Closed operations reject reuse, groups require the owning thread,
and each new context rechecks original bytes. Ordinary `ingest` and
`replace_range` retain independent verification.

Baseline timing stops reading aliases once positive evidence is found. A verified
publication context memoizes the final boolean for each exact effective cutoff;
later attestations can change the result, so dates are never treated as monotone.
Negative results require checking every alias. The memo holds no input frames,
reserves at most a quarter of the supplied buffer budget at 1 KiB per entry,
and is capped at 1024 entries with oldest-entry eviction. Failure and exit clear
it. Exact unchanged builds also pass the caller's execution limits into fresh
byte verification; standalone defaults remain serial.

Timing-only reads use the latest unresolved effective cutoff to skip scoped
by-date batches whose checksummed captured minimum physical availability is
later. Attestation copies respect the same cutoff; availability can only move
forward. General snapshots and unknown/legacy bounds keep broad reads. Empty
Item dependencies recurse at their own cutoff. Ordinary value reads, original
receipt identity and complete byte verification of all retained batches,
including future batches, remain unchanged.

A positive-only timing proof can avoid frame reads if every scoped retained batch
has a uniform baseline flag, its known observation bounds are entirely inside
the request, and a nonempty batch proves a physically visible original row.
Eligible nonbaseline witnesses prevent this shortcut; full seals are validated.
Strict/history views, general snapshots, legacy/unknown bounds, mixed flags and
partial overlap use ordinary selection. This never proves a negative result,
overrides a supplied frame, or replaces complete original byte verification.

Writable lake initialization creates a derived covering metadata index and
analyzes the batch table once, atomically. This avoids traversing recovery payload
overflow pages just to read metadata. Subsequent opens retain statistics; existing
schema-seven read-only lakes work without this index. Original recovery payloads,
frozen receipts, hashes and historical timing remain unchanged.

A first direct panel publication with unique coordinates and uniform timing
proof uses one atomic commit across its monthly partitions, preserving each
row's availability date. Same-coordinate revisions, existing-content checks and
mixed baseline evidence retain separate chronological publications. Complete
range receipts preserve the declared bounds even when their output is empty.

`items.builds(name)` exposes successful build receipts in publication order,
including definitions, dependency digests, ranges and frozen input references.
The records remain readable after archival. `inputs.is_current(receipt)` checks
the same declared windows and cutoff against current committed evidence without
creating another receipt. It follows the latest retained Item dependency proofs
recursively; a rebuilt result can be current while old historical proofs remain.
Pure verified same-content checks preserve currentness. `inputs.verify` separately
validates retained bytes; freshness does not replace integrity verification.
`inputs.is_current([receipt_a, receipt_b])` checks every original root in one
read view and returns whether all are current. Each supplied identity/digest is
validated, including duplicate IDs, before recursive results are shared within
this call. Original windows/cutoffs and exact stale-parent selection remain;
the next call checks fresh evidence. Empty sequences fail. No aggregate receipt,
frame cache or cross-call validity cache is created.
`inputs.verify(receipt, config=ExecutionOptions(...))` accepts explicit local
worker, in-flight and buffer limits. It checks every unique retained batch's
original uncompressed IPC checksum and structural/type validity, including
ancestor evidence, with bounded decompression and temporary-file mapping.
One bounded pool finishes before publication; the default is serial. Every
invocation rechecks bytes without a persistent validity cache or new identity.

Empty derived inputs retain their frozen upstream timing proof. A downstream
count or other scalar result remains a historical baseline until its selected
upstream evidence is verified at that cutoff. Later attestations cannot cross
an earlier information date or captured commit/check ceiling. Verified inputs
use immutable timing flags to avoid repeating daily prefix selection.

Initialization uses the caller's bounded worker pool for revision evaluation and
first-publication monthly partition preparation. Writer admission uses a
conservative buffer reserve; later revisions and existing partitions remain
serial. Recovery compression precedes SQLite write-lock acquisition while
checksum verification and publication retain their transactional guarantees.

Resuming an existing internally evaluated unverified baseline groups independent
coordinates into batches limited by row count and a conservative byte reserve.
Every row retains its availability date; verified rechecks retain separate dated
witnesses. This avoids repeatedly rewriting each month for every historical day.

Frozen-input currentness avoids reading Item values when the covering successful
build proof is among all captured parents and every captured batch/check/build
parent is recursively current. Any stale parent falls back to ordinary
cutoff/window selection; an unselected historical parent does not invalidate the
result. Missing build proofs cannot establish currentness. Integrity verification
still checks every original retained batch independently on every later call.
Neither optimization changes receipt identity or PIT selection.

Typed-row-v1 payload hashing retains exact persisted bytes and schema identity. Scalar token encoding avoids repeated JSON work; string tokens share a conservative 4 MiB reserve, at most 1 MiB per column and no more than the input-frame byte estimate. Cache fill is lazy from the existing bounded row iterator; no full unique-value list, unbounded string cache, additional worker pool or identity migration. Nested/decimal/binary values retain recursive canonical JSON; temporal native precision, signed zero, NaN/null and infinity remain distinct.

Identical ordered reconciliation fields with identical schema may bypass revision hash joins. Shape, ordering, null/value/availability/baseline differences fall back to ordinary reconciliation. Final withdrawal checks first project time/asset keys; exact keys skip the join and full old payloads are read only for actual withdrawals. Tombstones and frozen historical evidence remain unchanged.

Unchanged-content availability witnesses are written with a bounded row iterator, preserving every original record, insertion order and atomic publication. The writer avoids full-panel Python dictionary and SQL parameter lists; a stream or constraint failure rolls back the header and witnesses together. SQLite durability and historical receipts are unchanged.

Evidence queries select the dataset/check headers before looking up their indexed records. Unrelated datasets therefore do not cause full witness scans during reads or input freezing. The selected evidence and its order, cutoff and receipt identity remain unchanged.

Large complete by-date baseline attestations may use an immutable content-addressed full-commit seal. Prove exact unique (record ID, payload hash, original commit) equality against every original registered recovery batch, with one uniform availability date, using bounded external sorting. Counts alone never establish completeness. Retain all original SQL witnesses/headers and batch identities. Freeze embeds the checked seal; read-only operations never persist seals. Validate checksum and exact header/commit/batch binding before cache use. Partial/mixed or general checks retain the ordinary path; oversized unsupported inline evidence fails explicitly before materialization. Legacy receipts recapture their original representation for currentness; changing representation alone does not invalidate them. Replay preserves header eligibility, check/commit ceilings, ingestion cutoff, availability max, attestation ID and parent receipts. Proven per-batch observation bounds prune window reads only; frozen evidence and verification still retain/recheck every original batch on each call. Temporary sort connections/files are closed and removed.

DataItems.update accepts explicit input_windows keyed by declared input alias, mapping to inclusive observation (start,end). Validate aliases, date order and declared start/end plus Raw observation limits before recursion/publication; never infer windows for arbitrary producers. Pin overrides in the actual frozen requests without modifying the DataItem declaration or its hash. Omitted inputs retain their declared scope. General Raw snapshots are not windowed. Exact successful reuse checks the same definition/dependency/range identity before frame construction and still verifies every retained batch byte.

New explicit-window by-date freezes mark scoped_batches and use existing immutable version_batches observation/availability bounds to capture potentially selected batches. Retain unknown bounds, late revisions of in-window observations, related checks, overlapping empty item_range proofs and unknown scopes. Full-commit seals retain every original proof batch reference. Legacy receipts recapture their original broad representation; bounds/markers do not rewrite their bytes or hashes. Reads use matching immutable (commit,partition,hash) bounds to skip impossible batches; general initialization baselines remain complete. Shared-commit checks with unknown record-window membership conservatively invalidate; no unsafe currentness claim. No schema migration, new batch store or implicit rolling/lookback clipping.

Frozen pruning reads checksummed captured bounds only. Legacy receipts retain broad reads; changing live summaries cannot change historical values. Empty typed Item windows retain their captured schema; another month first establishing a physical schema adds no values to that empty window.

`inputs.verify` also accepts a nonempty finite sequence of original frozen
receipts. Each root identity/digest and every dependency edge are checked;
shared parents and original batches are checked once within this invocation.
The multi-root report lists original root IDs/digests and aggregate counts.
There is no new aggregate receipt or cross-call validity cache; a later call
rechecks bytes. Currentness remains a separate check.
