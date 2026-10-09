# DataItem 与外部 producer

仅在所有 scoped 原批次均有统一基线标记、已知观测范围完全位于请求内、且非空批次证明有截止日前的物理可见行时，可通过正向时间证明避免读取 frame。任何已可见的非基线核验都禁止此捷径；完整提交 seal 仍验证。strict/history、general、旧或未知范围、混合标记、部分重叠均回到实际选择；不据此证明否定结果、不覆盖调用方已提供的 frame，不替代全部原始字节校验。

时间判定读取按尚未判定日期的最大有效截止日，仅通过已冻结且校验的 scoped 日频批次 `min_available` 跳过不可能可见的未来批次；核验副本也受相同截止日限制，可用时间只会后移。general 快照、未知或旧范围仍宽读取，空 Item 依赖按自身截止日递归。普通值读取和收据身份不变，完整字节校验仍包含全部原始批次，包括跳过读取的未来批次。

基线时间判定发现肯定证据后停止读取后续输入。已核验的发布上下文仅按精确有效截止日期缓存最终布尔值；后续核验可能改变判定，不能假设日期单调。否定判定必须检查全部输入。缓存不保留 frame，按每项 1 KiB 保守预留至多四分之一显式 buffer，最多 1024 项，满时淘汰最早项，失败或退出时清空。已完成构建的复用校验也传入调用方资源限制；单独使用仍默认串行。

可写打开湖时，原有初始化路径原子创建派生元数据覆盖索引并进行一次统计分析；持有 writer 锁后再次确认索引不存在，后续打开不重建统计。没有此索引的 schema-seven 湖仍可只读打开。恢复 payload、冻结收据、哈希、历史时点和原始证据不变。

DataItem 的 `(time, asset_id)` 唯一且排序确定，value 声明 float/integer/boolean/string/
date/datetime/categorical 标量类型。定义、依赖和 producer revision 保存在 Data SQLite。

```python
from bagelquant_data import DataItemSpec, RawInput, Select, Cast

lake.items.register(DataItemSpec(
    "close", (RawInput("tushare", "daily"),), value_dtype="float64",
    time_column="source_time", value_column="close",
    transforms=(Select(("source_time", "asset_id", "close")), Cast({"close": "float64"})),
))
lake.items.initialize("close", end="2020-12-31")
lake.items.update("close", end="2021-01-15")
```

内置 Select/Filter/MapValues/Cast/显式键 Join/Align。重复输出键报错。坐标由调用者提供，
默认缺失 null；向前填充必须声明有限 `forward_fill_sessions`，遵守可用日和显式坐标日期，
后来的 null 事件终止之前的值。Data 不推断中国日历或全体股票分母。

复杂计算通过 `items.register_producer(key, revision, callable)` 注册，DataItemSpec
同时声明 producer_key/revision 和依赖。BuildContext 提供冻结 frames、起止日、信息截止日
及 commit；producer 返回中性 Polars 结果，Data 不导入 Core。重开后定义保留，执行时重新
注册 callable。依赖 receipt 控制失效，未知影响范围保守重建；移除坐标发布 tombstone，
保留历史。外部直接结果用 `items.ingest`，共用同一个提交和恢复协议。

声明依赖的直接结果必须传 `inputs.freeze` 生成的 `input_receipt`；Data 校验依赖身份并将
receipt 写入提交证据。`expected_definition_hash` 可拒绝计算期间更改的定义。未明确声明
可用日期的外部结果在采集时才可见；历史基线必须显式标记，严格读取会排除。
完整范围用 `items.replace_range(name, frame, start=..., end=..., available_date=...,
input_receipt=...)` 发布；缺少的坐标新增 null 事件，已冻结历史保持可重现。

首次直接发布的 Panel 如果坐标唯一、时点证明一致，会跨月分区一次原子提交，并保留
每行可用日期。同坐标修订、已有内容复查和混合基线证明仍按可用日期分别发布。
完整范围的 receipt 在输出为空时也保留调用者声明的起止日。

`items.builds(name)` 按发布顺序提供成功构建的定义、依赖摘要、范围和冻结输入引用，
注销后仍可读取。`inputs.is_current(receipt)` 在原有窗口及信息截止日下检查当前已提交
证据，不创建新 receipt；Data 递归检查最新选定的 Item 上游证明，保留旧证明不会导致
已重建结果永远失效。已验证内容的同值复查保持 currentness。`inputs.verify` 单独校验
保留的数据字节；新鲜度检查不替代完整性验证。
`inputs.is_current([receipt_a, receipt_b])` 在同一读取视图内检查全部原始根收据，
返回是否全部 current。先逐根校验身份/digest（包括重复 ID），只在本次调用内共享
递归布尔结果；各自窗口/截止日、陈旧父收据的精确选择保持，下一次重新检查。
空列表报错；不创建合成收据、Frame 缓存或跨调用有效性缓存。

空的派生输入仍保留冻结的上游时点证明。下游数量等标量结果在所选上游证据于该截止日
得到验证前仍属于历史基线。后来的可用性证明不能越过更早的信息日期或已捕获的
commit/check 上界。已验证输入根据不可变时点标记跳过重复的逐日前缀选择。

初始化的修订计算和首次月分区准备共用调用者指定的有界线程池。分区并发受
保守的缓冲预算限制；已有分区和后续修订保持串行。恢复证据先压缩再取得
SQLite 写锁，校验及发布仍在原事务中完成，恢复时保留记录的完整 schema。

已有未验证历史基线的内部重建，可将独立坐标按行数及保守字节预算分批提交，
保留每行可用日期；已验证证明仍逐日保存，避免冻结截止日的可见性被推迟。
首次发布保持原子性，历史版本和冻结输入不删除。

当前性检查只有在唯一父回执与成功构建证明一致、且递归验证当前有效时才跳过 Item
值读取；父回执过期或存在多个父回执时，继续按原有截止日期和窗口选择。完整性
验证在同一次调用内按提交、分区、预期哈希校验共享批次一次，保留原引用计数；
下次调用重新验证。以上优化不改变回执身份或 PIT 选择。

Typed-row-v1哈希保留原持久化字节和schema身份。标量编码避免重复JSON；字符串缓存按保守估算共享4 MiB总额、每列最多1 MiB，总额进一步受input-frame字节估算限制，从现有受限row iterator惰性填充，不collect全量unique、不新增worker pool、不迁移身份。嵌套/decimal/binary沿用递归canonical JSON，保留时间原生精度、有符号零、NaN/null和无穷差异。

版本核对仅在字段schema及有序值完全相同时跳过hash join；形状、排序、null/value/availability/baseline变化仍走原核对。最终删除检查先投影time/asset键；键完全一致可跳过连接，实际撤回才读取原完整payload。tombstone和旧冻结历史语义不变。

同值内容的可用性证据通过有界行迭代器写入，保留全部原始记录、写入顺序和原子发布。写入过程不再建立整张面板的 Python 字典及 SQL 参数列表；迭代或约束失败时，证据头和逐行证据一并回滚。SQLite 持久性设置及历史回执保持不变。

证据查询先筛选数据集及校验头，再通过索引读取对应记录。普通读取和输入冻结因此不再扫描无关数据集的全部逐行证据。已选证据的内容、顺序、截止日期及回执身份保持不变。

大型 by-date 基线的完整同值核验可保存不可变、按内容寻址的提交证明。使用有界外部排序，将每个原始恢复批次的唯一（记录 ID、载荷哈希、原提交）与全部见证逐项比较，并要求统一可用日期；仅比较数量不能证明完整。保留所有原始 SQL 见证、核验头和批次。冻结记录内嵌证明，只读操作不保存证明；缓存使用前核验哈希和头、提交、批次绑定。部分、混合日期及 general 核验沿用原路径，超出单条容量的内嵌证据在分配前明确失败。旧冻结记录按原表示检查有效性，表示变化本身不会使其失效。回放保留 PIT、摄取时间及提交/核验边界、可用日期 max、见证 ID 和上游收据。证明中的批次观测范围只减少窗口读取，冻结与每次验证仍保留并重新核验全部原批次。临时排序连接及文件及时关闭、删除。

`lake.items.update(..., input_windows={输入别名: (起始日, 结束日)})` 可显式声明依赖观测窗口。Data 校验别名、日期次序和已声明的边界，并将窗口写入真实冻结请求；不改写 DataItem 定义或哈希，不推断任意生产者的滚动/前向填充范围。未指定的依赖保持原范围，general Raw 快照不裁剪。精确复用先比较定义、依赖和输出区间，仍逐次验证保留的原批次。
新窗口冻结使用现有不可变批次的观测/可用日期范围，保留可能入选的批次、晚到修订、相关核验、重叠空区间和无法确定范围的证明。完整提交证明保留全部原始批次引用。旧收据按原表示检查有效性，不重写历史字节或哈希；只读按相同提交、分区和哈希的范围跳过不可能入选的批次，general 初始化基线保留完整。无法证明记录窗口归属的共享提交核验采用保守失效，无 schema 迁移或隐式缩短历史。

冻结裁剪只读取收据/证明中已捕获并校验的范围，旧收据沿用宽范围读取，不使用后来变动的在线摘要改变历史值。类型明确的空 Item 窗口保留已捕获 schema，其他月份首次建立物理 schema 不会为该空窗口增加值；相关数据或依赖变化仍使其失效。

相关外部输出使用 `with items.publication(input_receipt=receipt, config=..., cancelled=...) as publisher`，再逐组调用 `publisher.publish([ItemPublication(...)])`。上下文进入时完整核验固定输入一次；每组发布前检查定义 hash、依赖身份、frame 和范围，再沿用历史写入和完整范围替换。`start/end/complete_at` 须一起提供；报告为每张表最后一步的结果，不是累计行数。只保留当前组 frame，元数据提交串行且必须在创建上下文的线程执行。失败或取消保留已成功提交的数据，但失败会使上下文立即失效，即使调用者捕获异常也不能继续或成功退出。退出再次检查取消，旧 publisher 不能复用；每个新上下文重新核验原始字节。单独 ingest/replace_range 的核验不变。

`inputs.verify(receipt, config=ExecutionOptions(...))` 以显式 worker/in-flight/buffer 预算核验原始 IPC 的 SHA 和结构/类型，采用有界解压、临时映射和一个核验线程池，默认串行。也可传入非空有限原收据列表：逐个检查根身份/digest，逐条检查父依赖 digest，本次调用内共享父收据和批次只核验一次；多根报告列出原始 IDs/digests 和总数。不创建合成收据或跨调用有效性缓存，下一次调用仍重查字节。currentness 与完整性独立。

当前性检查只有在完整捕获的批次／复查／成功构建父凭据均递归有效，且覆盖构建证明属于这些父凭据时，才能省去 Item 值扫描。任何父凭据失效都回退到原有截止日和窗口选择；未被选中的旧父凭据失效不会直接否定结果。缺失构建证明不能使用此捷径，原始字节完整性核验保持独立、每次重新执行。
