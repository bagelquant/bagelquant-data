# Datasets

Register a dataset with the name, update type, and only the options its update
planner needs.

```python
from bagelquant_data import DatasetSpec

lake.admin.datasets.register(DatasetSpec("trade_cal", "general"))
lake.admin.datasets.register(DatasetSpec("daily", "by_daily", calendar="trade_cal", field_mappings={"trade_date": "time", "ts_code": "asset_id"}))
lake.admin.datasets.register(DatasetSpec("st", "by_daily", calendar="trade_cal", date_param="pub_date", field_mappings={"trade_date": "time", "ts_code": "asset_id"}))
lake.admin.datasets.register(DatasetSpec("balancesheet", "by_asset", asset_list="stock_basic", primary_key_extra=("period",), field_mappings={"ann_date": "time", "ts_code": "asset_id"}))
```

The pipeline derives the incremental key from `time`, `asset_id`, and optional
`primary_key_extra` fields. Each incremental dataset must explicitly declare
the provider-to-canonical field mapping; general datasets do not require one.

`by_daily` datasets send each missing calendar day under `date` by default. Set
`date_param` when a provider API uses a different date parameter, such as
`date_param = "pub_date"` for Tushare's `st` API. The generated date always
overrides a conflicting value in `source_api_params` or runtime `params`.

Store the same compact mapping in TOML and register it with `register_toml`.

```toml
name = "daily"
update_type = "by_daily"
calendar = "trade_cal"

[[field_mappings]]
trade_date = "time"
ts_code = "asset_id"
```

Mappings are true renames, so provider columns named `trade_date` and
`ts_code` are stored as `time` and `asset_id`. A mapping may rename other
columns as well, but incremental datasets must map both canonical key fields.

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
