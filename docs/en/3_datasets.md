# Datasets

Register a dataset with the name, update type, and only the options its update
scope synchronizer needs.

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

[field_mappings]
trade_date = "time"
ts_code = "asset_id"
```

`by_asset` declarations may configure later-revision refreshes:

```toml
update_type = "by_asset"
asset_list = "stock_basic"
asset_bucket_count = 32
revision_lookback_days = 730
revision_refresh_days = 30
```

`asset_bucket_count` controls the stable hash buckets inside each year. A new
asset rewrites only its year/bucket partition rather than one file for the
entire year. The default is 32. Once canonical data exists, changing the count
is rejected because it changes the physical layout; clear the dataset and
rebuild it before registering a different count.

The lake stores provider checks separately from commit-backed `data_max_time`.
This prevents sparse event data from being downloaded repeatedly while the
revision window still captures later restatements.
For every `by_daily` dataset, validated empty scopes in the latest 20
requested calendar sessions are rechecked on the next update. Older empty
daily scopes and empty `by_asset` scopes remain terminal until reset or a
definition change.

Mappings are true renames, so provider columns named `trade_date` and
`ts_code` are stored as `time` and `asset_id`. A mapping may rename other
columns as well, but incremental datasets must map both canonical key fields.

The lake also stores a canonical dataset schema. All-null input columns remain
untyped until an actual value establishes their type; compatible integer and
floating inputs are promoted deterministically. New columns are added to the
canonical schema, and strict numeric parsing rejects incompatible string data.

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

## Provider API and request discovery

By default, the declared dataset name is also the provider API name. Set
`source_api` when a stable local dataset name must call a differently named
provider API. The local name remains the raw-data identity, while `source_api`
controls only the request sent to the configured adapter.

`request_discovery` performs one provider request at planning time and turns a
non-empty result column into target request variants. Its values are deduplicated
and sorted, then form a Cartesian product with `source_api_params` and
`source_api_param_sets`. The discovery target parameter must not also appear in
either static parameter declaration.

```toml
name = "sw_l1_industry_membership"
source = "tushare"
source_api = "index_member_all"
update_type = "general"

[source_api_params]
is_new = "N"

[request_discovery]
api = "index_classify"
params = { level = "L1" }
result_field = "index_code"
target_param = "l1_code"
```

Discovery is declarative: adapters receive only an API name and request
parameters. A missing result field, an empty result, or a discovery error fails
the update before any target request is made.
