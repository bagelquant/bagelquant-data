# Adding A Dataset

Datasets are defined by `DatasetSpec` or YAML. A normal dataset should not require changes to the core framework.

## Minimal YAML Shape

```yaml
name: daily
source: tushare
source_dataset: daily
category: market
field_mapping:
  ts_code: ts_code
  trade_date: trade_date
required_columns: [asset_id, time]
primary_key: [asset_id, time]
asset_column: ts_code
time_column: trade_date
request_planner: snapshot
normalizer: standard
deduplication: primary_key_last
partition_strategy: year_month
update_mode: upsert
sort_columns: [time, asset_id]
reference: false
```

Register it:

```python
lake.datasets.add_from_yaml("path/to/dataset.yaml")
```

## Required Fields

Every spec needs:

- `name`: canonical dataset name.
- `source`: registered source name.
- `source_dataset`: provider endpoint or table name.
- `category`: descriptive group such as `market`, `financial_statement`, `financial_event`, or `reference`.
- `field_mapping`: source-to-canonical rename map used by the normalizer.
- `required_columns`: columns that must exist after normalization.

## Canonical Time Mapping

For non-reference datasets, set:

- `asset_column`
- `time_column`

For point-in-time financial datasets, also set:

- `period_column`
- `point_in_time: true`

Example:

```yaml
asset_column: ts_code
time_column: f_ann_date
period_column: end_date
point_in_time: true
```

## Keys

Use `primary_key` when records should be unique by a specific set of columns.

Use `business_key` when records may have revisions or multiple valid versions, but still share a logical business identity.

Do not assume `asset_id + time` is always unique.

## Request Planner

Initial planners:

- `snapshot`: one request for the dataset or date range.
- `by_asset`: one request per asset when assets are supplied.

Future planners may include `by_trade_date`, `by_period`, `by_date_range`, `paged`, or custom registered planners.

## Normalizer

`standard` maps configured fields and derives canonical columns:

- `asset_id`
- `time`
- `period` when configured
- `source`
- `source_dataset`

Use a custom normalizer only when a source response needs specialized parsing.

## Deduplication

Initial strategies:

- `none`
- `exact_record_hash`
- `primary_key_last`

Choose `primary_key_last` for daily market data where the latest record should replace the old row for the same key.

Choose `exact_record_hash` for financial records where multiple versions can be valid and only exact duplicates should be dropped.

## Partition Strategy

Initial strategies:

- `single_file`: reference or small datasets.
- `year_month`: dense market time series.
- `year_bucket`: financial statements and sparse event records.

For `year_bucket`, configure `bucket_count`:

```yaml
partition_strategy: year_bucket
partition_options:
  bucket_count: 32
```

## Reference Datasets

Reference datasets set:

```yaml
reference: true
partition_strategy: single_file
```

They are read with `lake.query.reference(...)` and are exempt from the single-value panel contract.

## Validation Checklist

Before adding a dataset, confirm:

- canonical `asset_id` and `time` can be derived for non-reference data
- `period` is present for point-in-time financial data
- required source columns are preserved
- the partition strategy matches access patterns
- deduplication does not discard meaningful revisions
- sort order supports common scans
