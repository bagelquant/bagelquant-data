# PIT reads and frozen inputs

```python
history = lake.raw.read("daily", source="tushare")
snapshot = lake.raw.read("daily", source="tushare", as_of="2020-01-10",
                         observation_start="2020-01-01", observation_end="2020-01-05")
current = lake.raw.read("daily", source="tushare", view="latest")
versions = lake.raw.read_versions("daily", source="tushare")
observations = lake.raw.observations("daily", source="tushare")
item = lake.items.read("close")
fixed_item = lake.items.read("close", view="snapshot", as_of="2020-01-10")
```

Daily Raw and DataItem historical reads select each observation's version visible
on its own date. Fixed `as_of` is an independent information cutoff. Reading a
short observation window does not substitute its endpoint for the cutoff. Raw
`start/end` filter the availability axis; `observation_start/end` filter the
observation axis. `observations()` restores the observation `time` axis after PIT
selection. `general` reads return a complete selected snapshot, with optional
`as_of`; they have no implicit daily coordinate grid.

`strict=True` excludes historical baselines whose original timing is unverified.
Same-value later collection can provide a later verified availability witness;
it cannot invent an earlier publication date. `view="versions"` exposes all
eligible revisions. Ordinary LazyFrames pin committed generations at creation,
so delayed `collect()` does not switch to a newly published generation.

```python
from bagelquant_data import RawInput, ItemInput, input_read_boundary

receipt = lake.inputs.freeze({
    "raw": RawInput("tushare", "daily", view="history"),
    "close": ItemInput("close"),
}, information_cutoff="2020-01-10")
frozen = lake.inputs.read(receipt, "close")
verification = lake.inputs.verify(receipt)
with input_read_boundary(lake.data_meta_path, receipt.max_commit,
                         receipt.information_cutoff, max_check_id=receipt.max_check_id):
    # Readers opened here capture the same boundaries, including across workers.
    reader = DataLake.open(data_meta_path=lake.data_meta_path,
                           lake_path=lake.lake_path, read_only=True)
```

Receipts atomically record dependencies, definitions, schema, compressed batch
identities, information cutoff and commit/check upper bounds. They read verified
local evidence, surviving later revisions, unregistering and current-file damage.
Nested boundaries may tighten but cannot explicitly expand. Reopening receipt
IDs uses `lake.inputs.get(id)`; reading never fetches providers.

For an operation that repeatedly reads the same frozen inputs, enter a finite
read context. It resolves original receipt metadata once and shares one SQLite
read view with matching read-only Data readers opened on the same thread:

```python
with lake.inputs.read_context(receipt, check_canceled=check_canceled,
                              progress=progress) as reader:
    january = reader.read(receipt, "close", start="2020-01-01",
                          end="2020-01-10").collect()
```

Context entry verifies every retained original batch by default. `verify=False`
defers byte verification to an explicit `reader.verify(receipt, ...)`; it does
not assert validity. A nonempty finite receipt sequence shares metadata and
deduplicates verification across its original roots and dependency batches.
Metadata is deeply immutable and external receipt objects supply only ID/digest.
Later `verify` calls in the same entered context reuse successfully verified
original batch keys and receipt graph summaries, checking every supplied root
digest. These operation-local proofs and metadata expire on exit; no frame or
validity cache survives. Writes are excluded from the read
context. A later operation reloads metadata and checks original bytes again.
The context belongs to its entering thread; the verifier alone may use its
explicit bounded worker pool.
Ordinary Raw/DataItem reads also reuse that transaction; dataset snapshots do
not begin, commit or roll back a transaction owned by the enclosing context.
`inputs.is_current` reuses completed True/False results within this fixed view,
keyed by original receipt ID/digest and the complete reader boundary (path,
commit/check ceilings and as_of). Every supplied root digest is still checked.
The context keeps at most 4096 booleans and caches no failed/partial call or frame;
exit clears them. Outside-context and new-context calls recapture current evidence.
The first Item stale-parent check also avoids numerical value allocation: a
transient Parquet spool retains only timing/identity/lineage columns, sharded by
record identity so revisions across months remain together. Each shard uses the
ordinary attestation and snapshot selection. A bounded staging buffer combines
small fragments into row groups, preserving per-record order and independently
copying projected buffers before the original mapping closes.
`is_current(..., config=options)`
overrides the enclosing context budget; absent both, default ExecutionOptions
apply. Oversized projected shards or witnessed expansion fail with MemoryError.
Cancellation/failure removes the spool. Original SHA, IPC structure/type checks
and independent verification remain unchanged.

`inputs.read(..., start=..., end=...)` narrows the inclusive observation window;
expanding captured request or Raw observation bounds raises `ValueError`. The
original information cutoff, commit/check ceilings and digest remain unchanged.
Only checksummed captured batch bounds prune reads; legacy/general or unknown
bounds retain conservative reads. Verification always includes all retained
batches, including those skipped for the narrower observation window.

`inputs.window_read_supported(receipt, alias)` is a metadata-only planning query:
true means every retained by-date batch has captured observation bounds capable
of safe window pruning. General/legacy/unknown bounds return false. It reads no
live batch summaries, does not verify bytes and does not promise a particular
window skips batches. Consumers may choose one admitted full-domain read for a
legacy input instead of repeatedly loading all history for shorter windows.

`inputs.verify(..., config=ExecutionOptions(...), check_canceled=...,
progress=...)` checks cancellation during receipt traversal, decompression and
IPC validation. The no-argument cancellation callback raises the caller's
exception. Progress receives `{stage: "verify_inputs", completed: int, total:
int}` for unique completed batches. Temporary files/maps and worker pools close
on failure or cancellation; completion is reported only after successful work.
Small original IPC stays in a bounded operation-local memory buffer, avoiding
temporary file/mmap traffic; payloads exceeding min(64 MiB, one quarter of the
per-worker buffer allocation) spill to the temporary mapping path. Decode chunks
reserve another quarter and leave the remainder for Arrow/type validation. Both
paths verify the exact full checksum, IPC structure and supported types; neither
retains input frames or verification transport after return.
