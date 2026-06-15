# Tushare 数据源

Tushare 是第一个内置数据源适配器，位于：

```text
bagelquant_data.sources.tushare
```

核心框架不导入 Tushare 专用代码。

## 安装

安装可选依赖：

```bash
uv sync --extra tushare
```

## 凭证

使用环境变量：

```bash
export TUSHARE_TOKEN="..."
```

或运行时配置：

```python
from bagelquant_data import DataLake
from bagelquant_data.sources.tushare import TushareSource

lake = DataLake.open("data")
lake.sources.register(TushareSource())
lake.sources.configure_tushare(token="...")
```

`configure_tushare` 会把 token 保存到当前数据湖的本地元数据 DB，供后续运行使用。Token 不会出现在 `repr` 输出或 source 列表中，也不应提交到仓库。

## 注册数据集规格

内置规格位于：

```text
src/bagelquant_data/sources/tushare/datasets/
```

注册示例：

```python
lake.datasets.add_from_yaml(
    "src/bagelquant_data/sources/tushare/datasets/daily.yaml"
)
lake.datasets.add_from_yaml(
    "src/bagelquant_data/sources/tushare/datasets/income.yaml"
)
```

## 初始数据集

引用：

- `stock_basic`
- `trade_cal`

市场：

- `daily`
- `daily_basic`
- `adj_factor`

财务报表：

- `income`
- `balancesheet`
- `cashflow`

财务事件：

- `forecast`
- `express`

## 标准时间映射

市场数据集：

```text
daily:       asset_id = ts_code, time = trade_date
daily_basic: asset_id = ts_code, time = trade_date
adj_factor:  asset_id = ts_code, time = trade_date
```

财务报表数据集：

```text
income:       asset_id = ts_code, time = f_ann_date, period = end_date
balancesheet: asset_id = ts_code, time = f_ann_date, period = end_date
cashflow:     asset_id = ts_code, time = f_ann_date, period = end_date
```

财务事件数据集：

```text
forecast: asset_id = ts_code, time = ann_date, period = end_date
express:  asset_id = ts_code, time = ann_date, period = end_date
```

原始源字段应尽量保留。

## 更新 Tushare 数据

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

财务报表和财务事件数据集会按资产逐个调用 Tushare，因为 API 需要 `ts_code`。如果省略 `assets`，更新器会从 `stock_basic` 推导股票池，因此请先更新或注册 `stock_basic`。

## 查询 Tushare 数据

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

Point-in-time 财务变换请使用 `lake.finance`。
