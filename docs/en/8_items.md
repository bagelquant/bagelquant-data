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

Empty derived inputs retain their frozen upstream timing proof. A downstream
count or other scalar result remains a historical baseline until its selected
upstream evidence is verified at that cutoff. Later attestations cannot cross
an earlier information date or captured commit/check ceiling. Verified inputs
use immutable timing flags to avoid repeating daily prefix selection.
