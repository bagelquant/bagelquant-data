# Extraction API

The extraction API lives under `lake.query`.

```python
from bagelquant_data import DataLake

lake = DataLake.open("data")
```

## Raw Canonical Records

`raw` returns canonical row-oriented records as a Polars `LazyFrame`.

```python
records = lake.query.raw(
    "income",
    source="tushare",
    start="2020-01-01",
    end="2026-06-15",
    assets=["000001.SZ"],
    columns=[
        "asset_id",
        "time",
        "period",
        "report_type",
        "n_income_attr_p",
    ],
)
```

Raw records preserve multiple point-in-time versions. The raw API must not silently collapse financial statement revisions or repeated records.

## Single-Field Long Panels

`field` is the main research extraction method for non-reference data.

```python
close = lake.query.field(
    "daily",
    "close",
    source="tushare",
    start="2025-01-01",
    end="2025-12-31",
    collect=True,
)
```

The output has exactly three columns:

```text
time | asset_id | close
```

The result is sorted by:

```text
time, asset_id
```

By default the value column keeps the requested field name. You can rename it:

```python
panel = lake.query.field(
    "daily",
    "close",
    source="tushare",
    value_name="value",
    collect=True,
)
```

## Multiple Fields

`fields` returns a dictionary of separate long panels.

```python
ohlcv = lake.query.fields(
    "daily",
    ["open", "high", "low", "close", "vol"],
    source="tushare",
    start="2025-01-01",
    end="2025-12-31",
    collect=True,
)

close = ohlcv["close"]
volume = ohlcv["vol"]
```

Each value in the dictionary is an independent frame with exactly three columns.

## Duplicate Resolution

Some datasets are not unique by `(time, asset_id)`. Financial statements may contain multiple records for the same availability date and asset because different periods, report types, or revisions are economically meaningful.

The default rule is `error_on_multiple`. If duplicates exist, `field` raises instead of silently choosing one row.

Supported resolution rules:

- `error_on_multiple`
- `latest_period`
- `latest_revision`
- `first`
- `last`

Example:

```python
latest = lake.query.field(
    "income",
    "n_income_attr_p",
    source="tushare",
    resolve="latest_period",
)
```

For financial statements, prefer `lake.finance`, because it is explicit about event-time data and point-in-time alignment.

## Reference Data

Reference datasets are exempt from the three-column panel contract.

```python
stock_basic = lake.query.reference(
    "stock_basic",
    source="tushare",
    collect=True,
)
```

Reference data remains row-oriented.

## Record Inspection

```python
preview = lake.query.records(
    "daily",
    source="tushare",
    limit=100,
)
```

Use this for debugging and quick inspection.

## Observation Grids

Observation grids are generic `(time, asset_id)` frames used for point-in-time alignment.

```python
observations = lake.query.observations(
    start="2025-01-01",
    end="2025-12-31",
    frequency="month_end",
    assets=["000001.SZ", "600000.SH"],
)
```

Supported initial frequencies include:

- `daily`
- `week_end`
- `month_end`
- `quarter_end`
- custom Polars date interval strings

The output has two columns:

```text
time | asset_id
```
