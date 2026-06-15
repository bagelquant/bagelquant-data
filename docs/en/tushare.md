# Tushare Source

Tushare is the first bundled source adapter. It lives under:

```text
bagelquant_data.sources.tushare
```

The core framework does not import Tushare-specific code.

## Installation

Install optional dependencies:

```bash
uv sync --extra tushare
```

## Credentials

Use an environment variable:

```bash
export TUSHARE_TOKEN="..."
```

Or configure at runtime:

```python
from bagelquant_data import DataLake
from bagelquant_data.sources.tushare import TushareSource

lake = DataLake.open("data")
lake.sources.register(TushareSource())
lake.sources.configure_tushare(token="...")
```

`configure_tushare` persists the token in the local lake metadata DB for future runs. Tokens are not included in `repr` output or source listings and should not be committed.

## Register Dataset Specs

Bundled specs live in:

```text
src/bagelquant_data/sources/tushare/datasets/
```

Register examples:

```python
lake.datasets.add_from_yaml(
    "src/bagelquant_data/sources/tushare/datasets/daily.yaml"
)
lake.datasets.add_from_yaml(
    "src/bagelquant_data/sources/tushare/datasets/income.yaml"
)
```

## Initial Dataset Set

Reference:

- `stock_basic`
- `trade_cal`

Market:

- `daily`
- `daily_basic`
- `adj_factor`

Financial statements:

- `income`
- `balancesheet`
- `cashflow`

Financial events:

- `forecast`
- `express`

## Canonical Time Mapping

Market datasets:

```text
daily:       asset_id = ts_code, time = trade_date
daily_basic: asset_id = ts_code, time = trade_date
adj_factor:  asset_id = ts_code, time = trade_date
```

Financial statement datasets:

```text
income:       asset_id = ts_code, time = f_ann_date, period = end_date
balancesheet: asset_id = ts_code, time = f_ann_date, period = end_date
cashflow:     asset_id = ts_code, time = f_ann_date, period = end_date
```

Financial event datasets:

```text
forecast: asset_id = ts_code, time = ann_date, period = end_date
express:  asset_id = ts_code, time = ann_date, period = end_date
```

Original source columns are preserved where possible.

## Updating Tushare Data

```python
lake.update.dataset("daily", source="tushare")

lake.update.datasets(
    ["daily", "daily_basic", "adj_factor"],
    source="tushare",
)

lake.update.dataset(
    "income",
    source="tushare",
    assets=["000001.SZ", "600000.SH"],
    start="2020-01-01",
    end="2026-06-15",
)
```

Financial statement/event datasets call Tushare once per asset because the API requires `ts_code`. If `assets` is omitted, the updater derives the universe from `stock_basic`, so update/register `stock_basic` first.

## Querying Tushare Data

```python
close = lake.query.field(
    "daily",
    "close",
    source="tushare",
    collect=True,
)

income = lake.query.raw(
    "income",
    source="tushare",
    columns=["asset_id", "time", "period", "n_income_attr_p"],
)
```

Use `lake.finance` for point-in-time financial transformations.
