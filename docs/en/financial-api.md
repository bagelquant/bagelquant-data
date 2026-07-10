# Financial And PIT Helpers

Financial records are queried through `lake.query.fundamental(...)`. Generic financial transforms are available under `lake.query.fundamentals`.

## Event-Level Fundamentals

```python
earnings_ytd = lake.query.fundamental(
    "income",
    "n_income_attr_p",
    source="tushare",
    value_name="earnings_ytd",
)
```

The event-level output is:

```text
asset_id | time | period | earnings_ytd
```

`time` is the information availability date. `period` is the accounting or economic period.

## Latest Available Values

```python
observations = lake.query.observations(
    start="2025-01-01",
    end="2025-12-31",
    frequency="month_end",
    assets=["000001.SZ"],
)

latest = lake.query.fundamental(
    "income",
    "n_income_attr_p",
    source="tushare",
    observations=observations,
    value_name="earnings_ytd",
    collect=True,
)
```

Latest-value alignment guarantees:

```text
event.time <= observation.time
```

## YTD To Period Flow

```python
earnings_quarter = lake.query.fundamentals.ytd_to_period(
    earnings_ytd,
    value_column="earnings_ytd",
    output_name="earnings_quarter",
)
```

For quarterly data:

```text
Q1 = YTD_Q1
Q2 = YTD_H1 - YTD_Q1
Q3 = YTD_Q3 - YTD_H1
Q4 = YTD_FY - YTD_Q3
```

## Trailing Aggregation

```python
earnings_ttm = lake.query.fundamentals.trailing(
    earnings_quarter,
    value_column="earnings_quarter",
    periods=4,
    operation="sum",
    output_name="earnings_ttm",
)
```

Supported operations:

- `sum`
- `mean`
- `min`
- `max`
- `first`
- `last`

## Average Stock Variables

```python
total_assets = lake.query.fundamental(
    "balancesheet",
    "total_assets",
    source="tushare",
)

avg_assets = lake.query.fundamentals.average_stock(
    total_assets,
    value_column="value",
    periods=4,
    method="endpoint",
    output_name="avg_assets",
)
```

Supported methods:

- `endpoint`
- `period_mean`

## Weighted Average

```python
weighted = lake.query.fundamentals.weighted_average(
    share_events,
    value_column="shares",
    effective_time_column="effective_time",
    period_start_column="period_start",
    period_end_column="period_end",
    output_name="weighted_average_shares",
)
```

## Generic Ratio

```python
ratio = lake.query.fundamentals.ratio(
    numerator=earnings_ttm,
    denominator=weighted,
    numerator_column="earnings_ttm",
    denominator_column="weighted_average_shares",
    output_name="value",
)
```

Zero denominator policies:

- `null`
- `nan`
- `raise`
