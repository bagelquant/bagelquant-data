# 概念

## DataSource

`DataSource` 把 provider 访问封装在 `read`、`exists` 和 `describe` 后面。适配器位于 `bagelquant_data.datasource`，并消费 `DataRequest`，让调用方不依赖 provider 原生客户端 API。

## Loader

`Loader` 负责编排请求并返回标准化的 `LoadedDataset`。如果配置了数据湖，它会优先读取本地快照，只在首次引导或显式刷新时访问 provider。

## DataLakeManager

`DataLakeManager` 负责本地数据湖的新增、编辑、删除、列表查询，以及手动 provider 更新。

每个 source 的第一个配置表是类 universe 的参考表。对 Tushare 来说是 `stock_basic`，会同时刷新上市、退市和暂停上市股票，避免幸存者偏差。

## Transform

Transform 是无状态 DataFrame 操作，可以通过 `Transform` 进行链式组合。

## Metadata

元数据独立于数据本身。契约用于描述数据集身份、schema、新鲜度、负责人、版本和血缘。

## Data Lake

数据湖按 source 和 table 分层存储，表也可以按标准化后的 `time` 分区。日频价格表使用 year/month/day 分区；基本面表使用 `f_ann_date` 对应的年份分区，以支持 point-in-time 可用性。写入会创建不可变快照，并更新 table 和 partition 层面的 latest 指针。

Panel 形状的表会暴露标准化后的 `time` 和 `asset_id` 列。非 panel 形状的参考表，例如 `stock_basic`，保留 table 级快照。

## Cache

缓存接口是可选能力，不应改变数据集身份或可复现性保证。
