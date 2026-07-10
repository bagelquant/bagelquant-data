# Tushare Source

Tushare is the bundled source adapter under:

```text
bagelquant_data.sources.tushare
```

The adapter implements source registration, token configuration, provider parameter mapping, API fetching, and pandas-to-Polars conversion.

## Installation

```bash
uv sync --extra tushare
```

## Credentials

Use an environment variable:

```bash
export TUSHARE_TOKEN="..."
```

Or configure the source in the lake:

```python
from bagelquant_data import DataLake
from bagelquant_data.sources.tushare import TushareSource

lake = DataLake.open("data")
lake.admin.sources.add(TushareSource())
lake.admin.sources.edit("tushare", token="...")
```

Saved source options live in the lake metadata DB and are redacted from source listings.

## Register Datasets

Tushare datasets use the same compact registration path as any other source. The source is selected when updating or querying.

```python
lake.admin.datasets.add("trade_cal", "general", reference=True)
lake.admin.datasets.add("daily", "by_daily", reference="trade_cal", request_date_param="date")
lake.admin.datasets.add("income", "by_id", reference="stock_basic", id_column="ts_code")
```

## Parameter Mapping

The lake plans requests from each dataset's `update_type`. The Tushare adapter maps generic request keys to Tushare parameters:

- `start` -> `start_date`
- `end` -> `end_date`
- `date` -> `trade_date`
- `id` -> `ts_code`

Date values are formatted as Tushare `YYYYMMDD` strings.

## Updating

```python
lake.update.dataset("daily", source="tushare")

lake.update.dataset(
    "income",
    source="tushare",
    ids=["000001.SZ", "600000.SH"],
    today="2026-06-15",
)
```

For `by_id` datasets, pass `ids` explicitly or register/update the configured ID reference dataset first.

## Querying

```python
close = lake.query.price(
    "daily",
    "close",
    source="tushare",
    collect=True,
)

earnings = lake.query.fundamental(
    "income",
    "n_income_attr_p",
    source="tushare",
)
```
