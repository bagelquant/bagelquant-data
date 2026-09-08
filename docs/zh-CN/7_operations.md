# 运维

通过 `lake.admin` 查看数据集、manifest、scope 和 ingestion run。显式浅扫描检查元数据及文件；深扫描读取 canonical Parquet 并验证内容哈希、schema、主键和分区归属。扫描不会收养孤立文件，也不会调用 Provider。隔离保留原文件及恢复日志，应用层显式重置受影响 scope 后重试正常更新流程。

## 快照修复与活动

General 快照只有在 canonical manifest 和文件存在、当前所有参数组合都有终态 Provider scope 时才是最新。快照缺失或参数组合尚未完成时，显式更新会重取全部组合，再替换完整快照。仅有 manifest 的旧 scope 不会被收养为 Provider 覆盖记录。

`arrow-ipc-v1` 逻辑哈希在序列化前规范化空值位图尾部未使用位，让不同 worker 数量下相同数据的校验和一致；实际值、schema 和空值位置仍参与校验。

`UpdateProgress` 在 scope 数量尚未知时也报告 planning、discovery。请求活动包含 `current_scope`、`in_flight`、`request_count`、`wait_reason` 和 `wait_seconds`。数据源可通过 `request_status` 报告配额等待且不调用 Provider。完成量仍代表逻辑 scope，不由心跳推进。

按日期更新 General 时，完成统计只针对本次快照。旧 checkpoint 的未完成记录保留为历史，不会把后来成功的完整刷新误判为 partial。

资产级空响应保留修订复查时间。已有检查未设复查日期时，状态汇总使用与请求计划相同的 UTC 复查周期；已完成的空响应不会立即被标为到期。
