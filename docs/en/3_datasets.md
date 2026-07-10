# Datasets

Register a dataset with the name, update type, and only the options its update
planner needs.

```python
from bagelquant_data import DatasetSpec

lake.admin.datasets.register(DatasetSpec("trade_cal", "general"))
lake.admin.datasets.register(DatasetSpec("daily", "by_daily", calendar="trade_cal"))
lake.admin.datasets.register(DatasetSpec("balancesheet", "by_asset", asset_list="stock_basic", primary_key_extra=("period",)))
```

The pipeline derives the incremental key from `time`, `asset_id`, and optional
`primary_key_extra` fields. General datasets do not require a canonical key.

Store the same compact mapping in TOML and register it with `register_toml`.
