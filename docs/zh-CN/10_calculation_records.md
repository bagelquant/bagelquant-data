# 计算记录

普通读取信任已登记不可变元数据，并保留数值类型检查，不重新计算字节或内容哈希。显式审计检查原始完整性，并将当前版本派生索引与原始证据对照。新输出仍保留规范内容哈希。

Owner 公共 API：`inputs.describe / request_identity / selection_identity / index_plan / build_index / verify`。索引维护由冻结计划显式执行，支持取消；普通打开不回填历史，也不改写旧 receipt 或 manifest。缺失、部分或版本不匹配的派生索引不提供 selection/interval 证明；可用的权威元数据精确命中和覆盖父记录快捷证明仍有效。资源预算不改变数值身份。

完整 `verify` 使用显式资源配置，未指定时继承当前读取上下文。选取摘要审计超出允许的缓冲预算时抛出 `MemoryError`，不会返回完整审计通过或自动扩大预算。原始字节传输可以落盘，但选取计算本身仍须满足工作集预算。

`inputs.request_identity(receipt, alias)` 对原始已登记 alias 请求、cutoff 和完整证据元数据计算身份，排除外层 receipt ID/digest、全局计数和可选索引。索引维护和无关 root alias 不改变该身份；alias 内检查与父引用保守保留。完整请求身份与精确选中窗口证明分别使用。
