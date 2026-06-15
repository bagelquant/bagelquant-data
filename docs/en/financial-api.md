# Financial API

The financial API lives under `lake.finance`. It provides generic operations that users compose into indicators. It intentionally avoids hardcoded metrics such as `eps_ttm()` or `roe_ttm()`.

## Event-Level Field Extraction

Financial source records are event-level data. The canonical event output is:

```text
asset_id | time | period | value
```

Example:

```python
events = lake.finance.field(
    "income",
    "n_income_attr_p",
    source="tushare",
)
```

You can rename the value column:

```python
earnings = lake.finance.field(
    "income",
    "n_income_attr_p",
    source="tushare",
    value_name="earnings_ytd",
)
```

## Point-In-Time Alignment

`asof` aligns event records to an observation grid. It guarantees:

```text
event.time <= observation.time
```

Example:

```python
observations = lake.query.observations(
    start="2025-01-01",
    end="2025-12-31",
    frequency="month_end",
    assets=["000001.SZ"],
)

aligned = lake.finance.asof(
    earnings,
    observations,
    value_column="earnings_ytd",
    output_name="earnings_ytd",
    collect=True,
)
```

The final aligned output is:

```text
time | asset_id | earnings_ytd
```

## Latest Available Field

`latest` combines field extraction and point-in-time alignment.

```python
total_assets = lake.finance.latest(
    "balancesheet",
    "total_assets",
    source="tushare",
    observations=observations,
    value_name="total_assets",
    collect=True,
)
```

## YTD To Period Flow

Many statement flow fields are cumulative year-to-date values. Use `ytd_to_period` to convert them into period values.

```python
earnings_ytd = lake.finance.field(
    "income",
    "n_income_attr_p",
    source="tushare",
)

earnings_quarter = lake.finance.ytd_to_period(
    earnings_ytd,
    value_column="value",
    output_name="earnings_quarter",
)
```

For quarterly data, the conceptual conversion is:

```text
Q1 = YTD_Q1
Q2 = YTD_H1 - YTD_Q1
Q3 = YTD_Q3 - YTD_H1
Q4 = YTD_FY - YTD_Q3
```

## Trailing Aggregation

Use `trailing` for rolling operations over event periods.

```python
earnings_ttm = lake.finance.trailing(
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

Use `average_stock` for balance-sheet style stock variables:

```python
avg_assets = lake.finance.average_stock(
    total_assets_events,
    value_column="value",
    periods=4,
    method="endpoint",
    output_name="avg_assets",
)
```

Initial methods:

- `endpoint`
- `period_mean`

## Weighted Average

`weighted_average` is generic. Weighted-average shares are one application, but the primitive is not tied to shares.

```python
weighted = lake.finance.weighted_average(
    share_events,
    value_column="shares",
    effective_time_column="effective_time",
    period_start_column="period_start",
    period_end_column="period_end",
    output_name="weighted_average_shares",
)
```

## Generic Ratio

`ratio` joins numerator and denominator frames and computes a generic ratio.

```python
eps_like = lake.finance.ratio(
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

The framework provides primitives; users name and validate business-specific indicators in their own research layer.
