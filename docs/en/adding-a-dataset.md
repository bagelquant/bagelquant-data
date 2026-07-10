# Adding A Dataset

Datasets are registered with a compact lifecycle declaration:

```python
lake.admin.datasets.add(name, update_type, reference=None, **kwargs)
```

Only `name` and `update_type` are required. `reference` is optional, and unknown keyword arguments are saved as source API static parameters.

## Minimal YAML

```yaml
name: daily
update_type: by_daily
reference: trade_cal
request_date_param: date
```

Register it:

```python
lake.admin.datasets.add_from_yaml("datasets/examples/custom_daily.yaml")
```

Or register directly:

```python
lake.admin.datasets.add("daily", "by_daily", reference="trade_cal", request_date_param="date")
```

## Update Types

- `general`: fetch the whole dataset and replace the canonical dataset with one `data.parquet` file. Pass `reference=True` for reference data.
- `by_daily`: use a calendar reference dataset, fetch missing open dates through `today`, and store `year=YYYY/month=MM/data.parquet`.
- `by_id`: use an ID-list reference dataset, fetch each ID from its latest stored date through `today`, and store `year=YYYY/batch=NN/data.parquet`.

`by_daily` defaults to `reference="trade_cal"` when omitted.

`by_id` defaults to `reference="asset_list"` when omitted. `batch_count` defaults to `32` stable hash buckets.

## Source API Kwargs

Unknown kwargs become static source API parameters:

```python
lake.admin.datasets.add(
    "stock_basic",
    "general",
    reference=True,
    exchange="SSE",
    list_status="L",
)
```

The updater sends those values with every request for that dataset.

Known framework kwargs, such as `source_dataset`, `id_column`, `request_id_param`, `start_date`, `calendar_date_column`, `calendar_open_column`, and `batch_count`, configure lake behavior instead.

## Canonical Columns

The standard normalizer infers canonical `asset_id` and `time` from common source columns:

- IDs: `asset_id`, `ts_code`, `symbol`, `code`, `ticker`
- Dates: `time`, `date`, `trade_date`, `ann_date`, `cal_date`

Advanced specs may still pass explicit normalizer fields such as `field_mapping`, `asset_column`, `time_column`, `period_column`, `primary_key`, and `deduplication`.

## Examples

Reference dataset:

```python
lake.admin.datasets.add("stock_basic", "general", reference=True)
```

Daily market dataset:

```python
lake.admin.datasets.add("daily", "by_daily", reference="trade_cal", request_date_param="date")
```

Asset-oriented dataset:

```python
lake.admin.datasets.add(
    "income",
    "by_id",
    reference="stock_basic",
    id_column="ts_code",
    request_id_param="id",
    start_date="2010-01-01",
)
```
