# PIT 读取和冻结输入

```python
history = lake.raw.read("daily", source="tushare")
fixed = lake.raw.read("daily", source="tushare", as_of="2020-01-10",
                      observation_start="2020-01-01", observation_end="2020-01-05")
current = lake.raw.read("daily", source="tushare", view="latest")
versions = lake.raw.read_versions("daily", source="tushare")
item = lake.items.read("close")
snapshot = lake.items.read("close", view="snapshot", as_of="2020-01-10")
```

按日期 Raw 与 DataItem 默认逐观测日选择当时可见版本。固定 as_of 是独立信息截止日；
读取窗口终点不能替代它。Raw start/end 过滤可用轴，observation_start/end 过滤观测轴。
`raw.observations` 在 PIT 选择后恢复观测 time 轴。General 读取完整快照，可给 as_of，
没有隐含的逐日坐标网格。`strict=True` 排除不可验证的历史基线；后续相同值只能证明
较晚可用性。普通 LazyFrame 在创建时固定已提交 generation，延迟 collect 不换到新版本。

```python
from bagelquant_data import RawInput, ItemInput, input_read_boundary

receipt = lake.inputs.freeze({"raw": RawInput("tushare", "daily"),
                             "close": ItemInput("close")},
                             information_cutoff="2020-01-10")
print(lake.inputs.read(receipt, "close").collect())
print(lake.inputs.verify(receipt))
with input_read_boundary(lake.data_meta_path, receipt.max_commit,
                         receipt.information_cutoff, max_check_id=receipt.max_check_id):
    reader = DataLake.open(data_meta_path=lake.data_meta_path,
                           lake_path=lake.lake_path, read_only=True)
```

冻结原子记录依赖、定义、schema、恢复批次、信息截止日、commit 与 check 上界。
读冻结输入校验本地不可变证据，后续修订、注销或当前文件损坏不改变重现。
receipt ID 用 `inputs.get` 重开；嵌套边界可收窄，明确扩大时拒绝。读取不触发 Provider。

重复读取同一冻结输入时可进入有限生命周期的只读上下文：

```python
with lake.inputs.read_context(receipt, check_canceled=check_canceled,
                              progress=progress) as reader:
    january = reader.read(receipt, "close", start="2020-01-01",
                          end="2020-01-10").collect()
```

同线程、同 metadata 路径的只读 Data reader 共享 SQLite 读视图与不可修改的原始凭证
元数据。外部凭证对象只提供 ID/digest，其证据不成为校验依据。默认入口检查所有
保留批次；`verify=False` 只延迟到显式 `reader.verify`，不代表有效性证明。支持非空
有限凭证序列，去重共享父凭证和批次。同一入口上下文内，后续 `verify` 复用已成功
检查的原始批次键和凭证依赖摘要，仍检查每个提供的根 digest。退出后元数据和
操作级证明失效，无 frame 或有效性缓存；后续上下文
重新读取元数据、校验原始字节。上下文不允许写入；只有显式预算的 verifier 可使用
有界工作池，元数据上下文属于入口线程。
普通 Raw/DataItem 读取也复用外层读事务，dataset snapshot 不在已有事务内重复
BEGIN，也不提交或回滚外层事务。
同一固定读视图内，`inputs.is_current` 按原始凭证 ID/digest 和完整 reader 边界
（metadata 路径、commit/check 上界、as_of）复用已完成的 True/False，仍检查每个
提供的根 digest。最多保留 4096 个布尔值，不缓存失败/部分调用、异常或 frame，
退出后清除。上下文外和新上下文重新捕获当前证据。首次 Item 过时父依赖检查
也只投影时点、记录身份和 lineage 列，写入临时 Parquet，按记录身份哈希分片；
跨月份修订保留在同一分片，仍使用原有 attestation/snapshot 选择器。
暂存缓冲区限制在预算的四分之一，独立复制投影 Arrow 数据，按分片稳定顺序
合并小片段后写入 row group，避免大量微小 footer，并在原始 mmap 关闭前解除引用。
`is_current(..., config=options)` 可覆盖外层上下文的资源预算，未提供时使用
默认 ExecutionOptions；投影分片或 witness 扩展超过预算时以 MemoryError
关闭操作。取消/失败会清理临时文件。原始 SHA、IPC 结构/类型及独立字节验证不变。

`inputs.read(..., start=..., end=...)` 收窄包含端点的观测窗口，禁止扩大凭证请求或
Raw observation 上界。信息截止日、commit/check 上界和 digest 保持原值。仅使用
凭证内有校验和的批次边界剪枝；旧凭证、General 或未知边界保守读取。验证仍检查
全部保留批次，包括窄窗口未读取的批次。

`inputs.window_read_supported(receipt, alias)` 只检查原始凭证元数据：所有按日期
批次都有已捕获的观测边界时返回 true，General、旧凭证或未知边界返回 false。
不读取实时批次摘要、不校验字节，也不保证某个具体窗口一定减少批次。调用方可以
为不支持窗口剪枝的输入在预算内读取一次完整所需范围，避免按小窗口反复加载历史。

`inputs.verify` 接受 `config`、`check_canceled` 和 `progress`。无参数取消回调在
依赖遍历、解压和 IPC 校验期间调用，通过抛出调用方异常取消。进度字典提供
`stage="verify_inputs"`、`completed`、`total`，计数为去重后的批次。失败或取消关闭
临时文件、映射和线程池；仅成功批次增加完成计数。
小型原始 IPC 使用操作内有界内存缓冲，避免临时文件和 mmap；超过
`min(64 MiB, 每个 worker 缓冲预算的四分之一)` 时转入临时文件映射。解压块另预留
四分之一预算，其余用于 Arrow/type 校验。两条路径均检查完整原始 SHA、IPC
结构及支持的类型，返回后不保留输入 frame 或验证缓冲。
