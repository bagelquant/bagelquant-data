# 新增数据集

数据集由 `DatasetSpec` 或 YAML 定义。普通数据集不应要求修改核心框架。

## 最小 YAML 结构

```yaml
name: daily
source: tushare
source_dataset: daily
category: market
field_mapping:
  ts_code: ts_code
  trade_date: trade_date
required_columns: [asset_id, time]
primary_key: [asset_id, time]
asset_column: ts_code
time_column: trade_date
request_planner: snapshot
normalizer: standard
deduplication: primary_key_last
partition_strategy: year_month
update_mode: upsert
sort_columns: [time, asset_id]
reference: false
```

注册：

```python
lake.datasets.add_from_yaml("path/to/dataset.yaml")
```

## 必填字段

每个 spec 需要：

- `name`：标准数据集名。
- `source`：已注册数据源名。
- `source_dataset`：提供商 endpoint 或表名。
- `category`：描述性分组，例如 `market`、`financial_statement`、`financial_event`、`reference`。
- `field_mapping`：normalizer 使用的源字段到标准字段映射。
- `required_columns`：标准化后必须存在的列。

## 标准时间映射

非引用数据集设置：

- `asset_column`
- `time_column`

Point-in-time 财务数据还要设置：

- `period_column`
- `point_in_time: true`

示例：

```yaml
asset_column: ts_code
time_column: f_ann_date
period_column: end_date
point_in_time: true
```

## 键

当记录应按某组列唯一时，使用 `primary_key`。

当记录可能有修订或多个有效版本，但共享逻辑业务身份时，使用 `business_key`。

不要假设 `asset_id + time` 总是唯一。

## 请求规划器

初始规划器：

- `snapshot`：对数据集或日期范围发一个请求。
- `by_asset`：提供 assets 时，每个资产发一个请求。

未来可加入 `by_trade_date`、`by_period`、`by_date_range`、`paged` 或自定义注册规划器。

## Normalizer

`standard` 根据配置字段派生标准列：

- `asset_id`
- `time`
- 配置时的 `period`
- `source`
- `source_dataset`

只有当源响应需要特殊解析时才使用自定义 normalizer。

## 去重

初始策略：

- `none`
- `exact_record_hash`
- `primary_key_last`

日频市场数据通常用 `primary_key_last`，因为相同 key 的最新记录应替换旧记录。

财务记录通常用 `exact_record_hash`，因为多个版本可能都有效，只应删除完全重复记录。

## 分区策略

初始策略：

- `single_file`：引用或小型数据集。
- `year_month`：密集市场时间序列。
- `year_bucket`：财务报表和稀疏事件记录。

对 `year_bucket`，配置 `bucket_count`：

```yaml
partition_strategy: year_bucket
partition_options:
  bucket_count: 32
```

## 引用数据集

引用数据集设置：

```yaml
reference: true
partition_strategy: single_file
```

它们通过 `lake.query.reference(...)` 读取，不受单值面板契约限制。

## 校验清单

新增数据集前确认：

- 非引用数据可派生标准 `asset_id` 和 `time`
- point-in-time 财务数据存在 `period`
- 必要源字段被保留
- 分区策略匹配访问模式
- 去重不会丢弃有意义修订
- 排序顺序支持常见扫描
