# 计算记录

普通读取消费原始 Parquet 的 typed 数值。冻结、更新计划、完成检查和范围身份只读
SQLite 小型元数据，不解码历史。`raw.completion_statuses` 和
`items.completion_statuses` 在一个固定元数据视图批量读取节点 flag；定义、发布修订、
连续覆盖、未完成更新和直接依赖状态共同决定是否完成。

Schema 9 将 Arrow schema 按 digest 只保存一次。冻结输入保存薄 header 和 alias 到
不可变 Source 的引用；共享块在收据之间共用原始 segment/check header 快照，当前可变
索引投影不能替代它。
`inputs.get` 返回显式 lazy `evidence` Mapping，访问字段才解引用；`describe` 返回每个
alias 的定义/schema 身份、范围、行数和 Source 引用。普通未变化检查使用登记的本地
发布修订和完整生产者完成证明；未知、未完成或变化证据仍走保守元数据比较。

`register_selection_set(receipt, descriptors)` 验证完整 Data 描述符后返回
`{id, digest, source, count}`。`describe_selection_set` 只验证薄 set header 和原始
Source；`read_selection_set(reference, keys=[...])` 只读取指定描述符，省略 keys 明确
表示准备或审计中的全量读取。key 为 UTF-8 JSON 的 SHA-256：排序字段、紧凑分隔符、
`ensure_ascii=False`、`default=str`。`inputs.verify` 检查所有共享后代、selection set
完整成员和 proof，以及原始不可变文件字节。

`inputs.describe`、`request_identity`、`range_identity`、`selection_identity`
和 `selection_descriptors` 均只读。支持的范围总能取得保守 token，显式维护不会
改变普通 token。范围外追加及未消费字段变化不影响身份；稀疏坐标的矩形边界只提供
保守判断，需要更精确相等证明时显式准备小型 selection 摘要。

`prepare_coordinate_selector` 仅保存调用方 Domain 坐标；`prepare_range_selections`
登记小型元数据摘要并保留原 Source 引用。`index_selection(s)` 和
`prepare_general_projections` 显式比较预算内的原始 typed 数据。坐标身份需要显式
准备小型日期轴，缺失时返回 `None`。这些操作均不创建历史逐行索引。

`inputs.verify` 核验凭证绑定、数值和首次可见事件文件的完整原始字节、类型、边界及
已有可选摘要。取消会关闭读取器并丢弃未完成的操作缓存。资源预算不影响数值身份；
预算不足报 `MemoryError`。原始文件缺失或损坏需要可信备份。

`items.completion_guard(names=...)` 在有限 Data 发布锁内返回同一批完成 flag。
消费方可在固定视图内执行自己的引用 CAS；计算、Provider 调用及 Data 写入必须
放在 guard 外。
