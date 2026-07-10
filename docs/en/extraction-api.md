# Query API

The query API lives under `lake.query`.

```python
from bagelquant_data import DataLake

lake = DataLake.open("data")
```

## Raw Records

`raw` returns canonical row-oriented records as a Polars `LazyFrame`.

```python
records = lake.query.raw(
    "income",
    source="tushare",
    start="2020-01-01",
    end="2026-06-15",
    assets=["000001.SZ"],
    columns=["asset_id", "time", "period", "report_type", "n_income_attr_p"],
)
```

Raw queries preserve repeated records, revisions, and PIT versions.

## Panels And Prices

`field`, `panel`, and `price` return one value field as a long panel:

```text
time | asset_id | value_column
```

```python
close = lake.query.price(
    "daily",
    "close",
    source="tushare",
    start="2025-01-01",
    end="2025-12-31",
    collect=True,
)

value_panel = lake.query.panel(
    "daily",
    "close",
    source="tushare",
    value_name="value",
    collect=True,
)
```

`fields` returns a dictionary of independent long panels:

```python
ohlcv = lake.query.fields(
    "daily",
    ["open", "high", "low", "close", "vol"],
    source="tushare",
    collect=True,
)
```

## Duplicate Resolution

If a dataset has multiple records for the same `(time, asset_id)`, `field` raises by default. Supported resolution rules are:

- `error_on_multiple`
- `latest_period`
- `latest_revision`
- `first`
- `last`

```python
latest = lake.query.field(
    "income",
    "n_income_attr_p",
    source="tushare",
    resolve="latest_period",
)
```

## PIT Fundamentals

Without observations, `fundamental` returns event-level PIT records:

```python
earnings = lake.query.fundamental(
    "income",
    "n_income_attr_p",
    source="tushare",
    value_name="earnings_ytd",
)
```

With an observation grid, it returns the latest available value at each observation:

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

PIT alignment uses `event.time <= observation.time`.

## Events

Append-only event datasets are queried with `events`:

```python
earnings_events = lake.query.events(
    "events",
    source="custom",
    event_type="earnings",
    start="2025-01-01",
    collect=True,
)
```

The event query preserves canonical records and can filter by time range, assets, and `event_type`.

## Reference Data

Reference datasets are row-oriented:

```python
stock_basic = lake.query.reference(
    "stock_basic",
    source="tushare",
    collect=True,
)
```

## Record Inspection

```python
preview = lake.query.records(
    "daily",
    source="tushare",
    limit=100,
)
```

## Observation Grids

Observation grids are `(time, asset_id)` frames used for PIT alignment.

```python
observations = lake.query.observations(
    start="2025-01-01",
    end="2025-12-31",
    frequency="month_end",
    assets=["000001.SZ", "600000.SH"],
)
```

Supported frequencies:

- `daily`
- `week_end`
- `month_end`
- `quarter_end`
- Polars date interval strings
