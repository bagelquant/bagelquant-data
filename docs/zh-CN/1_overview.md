# 概览

BagelQuant Data 是本地研究数据湖，使用 canonical Parquet 保存数据、SQLite 保存权威元数据和恢复批次。公共入口固定为 `lake.admin`、`lake.update` 和 `lake.query`。

数据集只有两种更新类型：`general` 在显式更新内容发生变化时生成完整快照，无变化时只记录检查；`by_daily` 按交易日或自然日以及声明的参数 variant 规划请求。两类数据都按 PIT 可用月份分区，并保留不可变版本。

`source_time` 表示源观测或公告日期，`time` 表示版本可用日期，`ingested_at` 表示实际 UTC 入库时间。数值计算在选择 PIT 版本后恢复普通观测日期轴。
