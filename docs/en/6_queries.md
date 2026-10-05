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
