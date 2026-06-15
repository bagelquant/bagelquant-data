# 财务 API

财务 API 位于 `lake.finance`。它提供用户可组合的通用操作，不提供 `eps_ttm()` 或 `roe_ttm()` 这类硬编码指标。

## 事件级字段提取

财务源记录是事件级数据。标准事件输出是：

```text
asset_id | time | period | value
```

示例：

```python
events = lake.finance.field(
    "income",
    "n_income_attr_p",
    source="tushare",
)
```

可以重命名值列：

```python
earnings = lake.finance.field(
    "income",
    "n_income_attr_p",
    source="tushare",
    value_name="earnings_ytd",
)
```

## Point-In-Time 对齐

`asof` 将事件记录对齐到观察网格，并保证：

```text
event.time <= observation.time
```

示例：

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

最终对齐输出：

```text
time | asset_id | earnings_ytd
```

## 最新可用字段

`latest` 组合了字段提取和 point-in-time 对齐。

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

## YTD 转单期流量

许多利润表或现金流字段是年初至今累计值。使用 `ytd_to_period` 可转换为单期值。

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

季度概念转换：

```text
Q1 = YTD_Q1
Q2 = YTD_H1 - YTD_Q1
Q3 = YTD_Q3 - YTD_H1
Q4 = YTD_FY - YTD_Q3
```

## 滚动聚合

使用 `trailing` 对事件期间做滚动操作。

```python
earnings_ttm = lake.finance.trailing(
    earnings_quarter,
    value_column="earnings_quarter",
    periods=4,
    operation="sum",
    output_name="earnings_ttm",
)
```

支持操作：

- `sum`
- `mean`
- `min`
- `max`
- `first`
- `last`

## 存量变量平均

`average_stock` 用于资产、权益、库存、股本等资产负债表存量变量：

```python
avg_assets = lake.finance.average_stock(
    total_assets_events,
    value_column="value",
    periods=4,
    method="endpoint",
    output_name="avg_assets",
)
```

初始方法：

- `endpoint`
- `period_mean`

## 加权平均

`weighted_average` 是通用操作。加权平均股本只是其中一个应用，原语本身不绑定到股本。

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

## 通用比率

`ratio` 连接 numerator 和 denominator frame，并计算通用比率。

```python
eps_like = lake.finance.ratio(
    numerator=earnings_ttm,
    denominator=weighted,
    numerator_column="earnings_ttm",
    denominator_column="weighted_average_shares",
    output_name="value",
)
```

零分母策略：

- `null`
- `nan`
- `raise`

框架提供原语；业务指标的命名和校验由用户自己的研究层完成。
