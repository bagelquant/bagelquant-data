# Quickstart

```python
from pathlib import Path
from datetime import date
import polars as pl
from bagelquant_data import DataLake, DatasetSpec, RawInput

lake = DataLake.open(data_meta_path=Path("example/data_meta.sqlite"),
                     lake_path=Path("example/lake"))
spec = DatasetSpec("prices", "by_date", date_kind="calendar",
                   field_mappings={"time": "time", "asset_id": "asset_id"})
lake.raw.ingest(spec, pl.DataFrame({
    "time": [date(2020, 1, 2)], "asset_id": ["A"], "close": [11.25],
}), mode="initialize")
print(lake.raw.observations("prices", source="custom").collect())
receipt = lake.inputs.freeze({"prices": RawInput("custom", "prices")},
                             information_cutoff="2020-01-02")
print(lake.inputs.read(receipt, "prices").collect())
print(lake.inputs.verify(receipt))
```

This example uses local supplied frames, without provider calls. Initialization
records a historical baseline; strict reads can exclude unverified timing.
To open existing data pass the same explicit paths and `read_only=True`. Mutating
APIs then raise `PermissionError`, and missing files/databases fail without creation.
