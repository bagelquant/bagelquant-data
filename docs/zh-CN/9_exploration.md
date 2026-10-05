# 探索

```python
from bagelquant_data import exploration

frame = lake.items.read("close").collect()
summary = exploration.overview(frame, coordinates=coordinates)
daily = exploration.coverage(frame, coordinates, by="time")
distribution = exploration.distribution(frame)
quantiles = exploration.quantiles(frame)
outliers = exploration.outliers(frame)
series = exploration.time_series(frame, "A")
section = exploration.cross_section(frame, "2020-01-02")
```

概况分别计数 null、NaN、infinity。覆盖分母必须提供明确、唯一、非空的 time×asset_id
坐标，可表达稀疏成员关系。数值分布和分位数只统计有限值，分类值提供频率。
`profile_item` 返回概况及 Polars 结果表。Data 不生成图表、报告、Web 服务或应用元数据，
Workbench 根据领域坐标将结果呈现在 GUI。
