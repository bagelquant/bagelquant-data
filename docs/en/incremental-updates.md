# Updates

Dataset updates run through `lake.update`.

```python
lake.update.dataset("daily", source="tushare", today="2026-07-10")
lake.update.datasets(["daily", "daily_basic"], source="tushare")
lake.update.source("tushare")
```

## Update Flow

```text
lake-owned request planning
-> fetch
-> staging
-> normalization
-> validation
-> deduplication
-> update_type storage write
-> manifest update
-> staging cleanup
-> run metadata
```

## Update Types

`DatasetSpec.update_type` controls planning, merge behavior, and storage layout:

- `general`: fetch one whole-dataset response, replace existing canonical data, and write `data.parquet`.
- `by_daily`: read a calendar reference dataset, fetch missing open dates through `today`, merge affected partitions, and write `year=YYYY/month=MM/data.parquet`.
- `by_id`: read an ID reference dataset, fetch each ID from its latest stored date through `today`, merge affected partitions, and write `year=YYYY/batch=NN/data.parquet`.

`by_id` batches use deterministic stable hash buckets. The default `batch_count` is `32`.

## Request Options

`lake.update.dataset(...)` accepts:

- `start`
- `end`
- `today`
- `ids`
- `workers`
- `batch_size`
- `source_options`
- `progress`
- `max_retries`
- `retry_backoff_seconds`

Example:

```python
lake.update.dataset(
    "income",
    source="tushare",
    ids=["000001.SZ", "600000.SH"],
    today="2026-07-10",
    batch_size=250,
    max_retries=3,
)
```

## Pagination

Offset-paginated APIs are configured on the dataset spec:

```yaml
request_options:
  pagination: offset
  page_size: 5000
  limit_param: limit
  offset_param: offset
  offset_start: 0
```

Each physical page is retried independently and recorded in API-call metadata.

## Observability

Updates persist:

- ingestion runs
- API call attempts
- request parameters
- retry counts
- success/failure counts
- rows downloaded
- rows committed
- error messages
- rejected-row summaries

Inspect update state through `lake.admin`:

```python
print(lake.admin.runs(limit=20))
print(lake.admin.failures(dataset="daily", source="tushare"))
print(lake.admin.status.dataset("daily", source="tushare"))
```

## Staging And Rejected Rows

Fetched source responses are written to `staging/` before normalization. Staging for a run is cleaned after the commit attempt.

Rows rejected during normalization are written to `rejected/` and summarized in SQLite:

```python
rejected = lake.admin.rejected("income", source="tushare")
```
