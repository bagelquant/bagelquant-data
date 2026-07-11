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

Use the optional `source_api_params` table for provider parameters that should
be sent unchanged on every update of a dataset. List values in this table are
passed through to the provider as one parameter value.

```toml
name = "stock_basic"
update_type = "general"
source = "tushare"

[source_api_params]
exchange = "SSE"
list_status = "L"
```

Use `source_api_param_sets` when one dataset refresh needs several provider
calls. Each table expands list values into independent calls; list values in
the same table form a Cartesian product.

```toml
[source_api_params]
exchange = "SSE"

[[source_api_param_sets]]
list_status = ["L", "D", "P"]
```
