# 提取 API

提取 API 位于 `lake.query`。

```python
from bagelquant_data import DataLake

lake = DataLake.open("data")
```

## 标准原始记录

`raw` 返回行式标准记录，类型是 Polars `LazyFrame`。

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

原始记录会保留多个 point-in-time 版本。raw API 绝不能静默合并财务报表修订或重复记录。

## 单字段长面板

`field` 是非引用研究数据的主要提取方法。

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

输出只有三列：

```text
time | asset_id | close
```

结果按以下列排序：

```text
time, asset_id
```

默认值列名保留请求字段名。也可以重命名：

```python
panel = lake.query.field(
    "daily",
    "close",
    source="tushare",
    value_name="value",
    collect=True,
)
```

## 多字段

`fields` 返回多个独立长面板组成的字典。

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

字典中的每个值都是只有三列的独立 frame。

## 重复记录处理

有些数据集并不按 `(time, asset_id)` 唯一。财务报表可能因为不同期间、报告类型或修订版本而在同一可获得日和资产下有多条记录。

默认规则是 `error_on_multiple`。如果存在重复，`field` 会抛错，而不是静默选择一行。

支持的规则：

- `error_on_multiple`
- `latest_period`
- `latest_revision`
- `first`
- `last`

示例：

```python
latest = lake.query.field(
    "income",
    "n_income_attr_p",
    source="tushare",
    resolve="latest_period",
)
```

对财务报表，优先使用 `lake.finance`，因为它对事件时间数据和 point-in-time 对齐更明确。

## 引用数据

引用数据集不受三列面板契约限制。

```python
stock_basic = lake.query.reference(
    "stock_basic",
    source="tushare",
    collect=True,
)
```

引用数据保持行式结构。

## 记录检查

```python
preview = lake.query.records(
    "daily",
    source="tushare",
    limit=100,
)
```

用于调试和快速检查。

## 观察网格

观察网格是通用 `(time, asset_id)` frame，用于 point-in-time 对齐。

```python
observations = lake.query.observations(
    start="2025-01-01",
    end="2025-12-31",
    frequency="month_end",
    assets=["000001.SZ", "600000.SH"],
)
```

初始支持频率：

- `daily`
- `week_end`
- `month_end`
- `quarter_end`
- 自定义 Polars 日期间隔字符串

输出两列：

```text
time | asset_id
```
