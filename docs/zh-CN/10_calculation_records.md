# 计算记录

普通读取消费原始 Parquet 的 typed 数值。冻结、更新计划、完成检查和范围身份只读
SQLite 小型元数据，不解码历史。`raw.completion_statuses` 和
`items.completion_statuses` 在一个固定元数据视图批量读取节点 flag；定义、发布修订、
连续覆盖、未完成更新和直接依赖状态共同决定是否完成。

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
