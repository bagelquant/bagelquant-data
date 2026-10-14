# DataItem 与外部 producer

显式恢复声明可调用 `lake.items.register(original_spec,
expected_definition_hash=captured_current_hash)`。Data 在该 Item 的 writer lease
及注册事务中核对原先捕获的当前 active hash；缺失或并发改变时，在声明写入前拒绝。
这一检查只保护单个 Item，多个调用不是原子组。调用者仍须证明原始定义、保存恢复
进度和旧回执；注册不会计算输出，也不会创建完成证明。

`lake.items.completed_builds(name)` 读取经过验证的完整范围完成证书、原始来源和待可见记录。
`builds(name)` 也包含普通行写入，不能据此认定完整范围已计算。旧证书缺失时，必须以原完整 producer 报告显式调用 `index_completion` 维护。
`items.recertify(name, start=..., end=..., update_policy=...)` 只为已证明无影响的
完整输出更新来源记录，不初始化、不递归更新依赖、不读取值或执行 producer。依赖先各自
完成 recertify；新增、变化或未知影响在数值工作之前拒绝。显式 policy 必须与已登记的
不可变 producer 合同一致。
`expected_definition=lake.items.get(name)` 绑定消费方验证过的声明，准入及发布再次
检查定义；同一 runtime producer key/revision 已声明的因果 policy 不能替换或移除。
初始未声明的 policy 可后续补充；执行中的 recertify 仍在准入及发布检查当前 policy。
消费方发布已完成结果引用时，可用 `lake.inputs.publication_guard(receipts)` 将 Data writer 锁和同一元数据视图保持到自己的 revision CAS 完成。
它要求可写湖且位于已有只读上下文之外，不校验原始字节，不写 Data 记录，失败或取消后释放锁。

发布的基线传播按每个实际截止日对全部依赖作 OR 判定。先检查所有输入的原记录正向证明，再读取可选时间投影。General 完整快照（包括已知空基线）根据原快照与核验 header、实际截止日和 commit/check 上限选择；任一确定基线即可判定该日，未知证明仍不能建立否定结果。

日频输入仅在所有 scoped 原批次均有统一基线标记、已知观测范围完全位于请求内、且非空批次证明有截止日前的物理可见行时，可通过正向时间证明避免读取 frame。任何已可见的非基线核验都禁止此捷径；完整提交 seal 仍验证。日频 strict/history、旧或未知范围、混合标记、部分重叠均回到实际选择；不据此证明否定结果、不覆盖调用方已提供的 frame，不替代全部原始字节校验。

时间判定读取按尚未判定日期的最大有效截止日，仅通过已冻结且校验的 scoped 日频批次 `min_available` 跳过不可能可见的未来批次；核验副本也受相同截止日限制，可用时间只会后移。general 快照、未知或旧范围仍宽读取，空 Item 依赖按自身截止日递归。普通值读取和收据身份不变，完整字节校验仍包含全部原始批次，包括跳过读取的未来批次。

基线时间判定发现肯定证据后停止读取后续输入。已核验的发布上下文仅按精确有效截止日期缓存最终布尔值；后续核验可能改变判定，不能假设日期单调。否定判定必须检查全部输入。缓存不保留 frame，按每项 1 KiB 保守预留至多四分之一显式 buffer，最多 1024 项，满时淘汰最早项，失败或退出时清空。已完成构建的复用校验也传入调用方资源限制；单独使用仍默认串行。

可写打开湖时，原有初始化路径原子创建派生元数据覆盖索引并进行一次统计分析；持有 writer 锁后再次确认索引不存在，后续打开不重建统计。没有此索引的 schema-eight 湖仍可只读打开。恢复 payload、冻结收据、哈希、历史时点和原始证据不变。

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
注册 callable。依赖 receipt 控制失效；未知影响返回明确阻塞计划，不隐式重建已完成历史；移除坐标发布 tombstone，
保留历史。外部直接结果用 `items.ingest`，共用同一个提交和恢复协议。

声明依赖的直接结果必须传 `inputs.freeze` 生成的 `input_receipt`；Data 校验依赖身份并将
receipt 写入提交证据。`expected_definition_hash` 可拒绝计算期间更改的定义。未明确声明
可用日期的外部结果在采集时才可见；历史基线必须显式标记，严格读取会排除。
完整范围用 `items.replace_range(name, frame, start=..., end=..., available_date=...,
input_receipt=...)` 发布；缺少的坐标新增 null 事件，已冻结历史保持可重现。

首次直接发布的 Panel 如果坐标唯一、时点证明一致，会跨月分区一次原子提交，并保留
每行可用日期。同坐标修订、已有内容复查和混合基线证明仍按可用日期分别发布。
完整范围的 receipt 在输出为空时也保留调用者声明的起止日。

外部生产者已有可信增量影响证明时，可在 `ItemPublication` 同时提供原生完整完成记录
`reuse_build_id`、互不重叠的 `affected_ranges` 和完整 `start/end/complete_at`。
Data 只替换受影响范围（包含删除），复用其余原始版本，并确认新增日期和新成熟 pending
没有遗漏。缺少原生覆盖、范围外后续发布、定义变化或取消不能登记新完整完成。
已经完成的局部单位和旧收据保持不可变，失败后可按原前序记录重试。
空的 `affected_ranges` 只登记当前 Source 的完成证明，不改写数值。重复相同 Source
和原生前序记录时，仍先检查来源、输出、时序、定义与取消边界，再复用完成记录；
仅输出计数相同不足以授权复用。

`selection_identity(..., values_only=True, causal_versions=True)` 用于真正消费
Item 标量 PIT 事件的 versions 计算：同一坐标、可用日期只保留最终 creator，但保留
每个不同的可用日期、baseline 标记和 typed value。任意附加列不在该投影中；它不证明
完整 Source 等价，也不代替上游生产者的完成/currentness 检查。

`inputs.selection_equivalent(original, current_or_none, alias, ...)` 绑定实际
窗口、view、strictness 和信息截止日。current 为 None 时只在本次读取视图捕获该 alias
的当前元数据，不持久化新 freeze。原始 header/witness 完全等价可直接证明；否则读取
可选 canonical 索引。`records_only=True` 仅允许 header 证明；`summaries_only=True`
还允许已登记的 canonical 摘要，但不解码逐行索引。未知仍返回 None。

显式 `inputs.index_selection(receipt, alias, start=..., end=..., view=...,
causal_versions=..., config=...)` 从已有 value 索引登记标量摘要，不补建缺失 batch 索引。
摘要绑定被选择的原始 headers、creator 顺序、witness、未知空 baseline、实际选择合同
及原始注册 Source。无关全局计数、alias、已知空完成记录不会使其失效。
`inputs.verify` 会审计可复用的摘要和原始 Source 闭包，即使调用方提供的是较新的等价
收据。打开 Data 和 freeze 均不会自动补建历史摘要或遍历整段逐行索引。

General 日期轴消费使用 `coordinate_equivalent`，绑定实际 clip、baseline 策略及
完整原子 snapshot；内容改变时需要显式 coordinate 索引。`coordinate_identity` 标识
这个投影。日期相等不证明标量或时点等价；缺少原始/当前证明或预算不足仍是 unknown。
同线程有限 read_context 仅共享不可变元数据与 canonical token；嵌套缓存只在 buffer
预算相同时共享，退出即失效，不保留数值 frames。

`items.builds(name)` 按发布顺序提供成功构建的定义、依赖摘要、范围和冻结输入引用，
注销后仍可读取。`inputs.is_current(receipt)` 在原有窗口及信息截止日下检查当前已提交
证据，不创建新 receipt；Data 递归检查最新选定的 Item 上游证明，保留旧证明不会导致
已重建结果永远失效。已验证内容的同值复查保持 currentness。`inputs.verify` 单独校验
保留的数据字节；新鲜度检查不替代完整性验证。
固定截止日下，currentness 可比较 General 原子事件 header，或 by_date Raw 全部
内容 header 和可见 witness，无需行索引。未来已知复核不影响原截止日；未知空基线、
schema、定义、全部内容批次和可见 witness 仍绑定。内容变化仍需要精确选择证明。
progress=callback 可报告这些仅读取记录的判断。
同一条记录等价路径也利用已捕获边界或原批次已有的边界摘要排除未来物理批次；
边界缺失时保守保留整批，不改变原收据及重放范围。Item 的上游新鲜度仍单独递归检查。

分段完成的 Item 不要求额外建立全范围证书：所有捕获的生产者输入记录均有效，且完整批次范围能证明至少一行落在请求窗口、最晚可用日期不超过截止日、strict 未排除基线时，可直接复用这些记录。仅有物理行数或范围重叠不能证明实际选中数据。空选择或未知选择保留原来的完成证明和已登记索引要求，不隐式读取原始字节或维护索引。

消费方读取完整历史时，经过验证的原生月度完成记录也可重新确认该月旧批次的来源。被确认的同一来源在全部捕获批次与检查中的范围必须完整落在证书范围内，且不晚于证书的输出 commit 和检查 ID 边界；证书 Source 必须有效，截止日不能晚于消费方。范围之外仍使用同一来源、范围未知、后来新增检查或未覆盖的空完成记录，继续保留原有检查要求。同值检查可能不推进输出 commit，因此仍必须检查 ID 边界。此证明只读取登记记录，保留原数值和来源证据。

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

当前性检查复用成功构建证明及递归有效的父凭据；父凭据过期时，继续按原有
截止日期和窗口读取已登记的精确选择索引，不隐式读取 Item 值。完整性
验证在同一次调用内按提交、分区、预期哈希校验共享批次一次，保留原引用计数；
下次调用重新验证。以上优化不改变回执身份或 PIT 选择。

Typed-row-v1哈希保留原持久化字节和schema身份。标量编码避免重复JSON；字符串缓存按保守估算共享4 MiB总额、每列最多1 MiB，总额进一步受input-frame字节估算限制，从现有受限row iterator惰性填充，不collect全量unique、不新增worker pool、不迁移身份。嵌套/decimal/binary沿用递归canonical JSON，保留时间原生精度、有符号零、NaN/null和无穷差异。

版本核对仅在字段schema及有序值完全相同时跳过hash join；形状、排序、null/value/availability/baseline变化仍走原核对。最终删除检查先投影time/asset键；键完全一致可跳过连接，实际撤回才读取原完整payload。tombstone和旧冻结历史语义不变。

同值内容的可用性证据通过有界行迭代器写入，保留全部原始记录、写入顺序和原子发布。写入过程不再建立整张面板的 Python 字典及 SQL 参数列表；迭代或约束失败时，证据头和逐行证据一并回滚。SQLite 持久性设置及历史回执保持不变。

证据查询先筛选数据集及校验头，再通过索引读取对应记录。普通读取和输入冻结因此不再扫描无关数据集的全部逐行证据。已选证据的内容、顺序、截止日期及回执身份保持不变。

大型 by-date 基线的完整同值核验可保存不可变、按内容寻址的提交证明。使用有界外部排序，将每个原始恢复批次的唯一（记录 ID、载荷哈希、原提交）与全部见证逐项比较，并要求统一可用日期；仅比较数量不能证明完整。保留所有原始 SQL 见证、核验头和批次。冻结记录内嵌证明，只读操作不保存证明；缓存使用前核验哈希和头、提交、批次绑定。部分、混合日期及 general 核验沿用原路径，超出单条容量的内嵌证据在分配前明确失败。旧冻结记录按原表示检查有效性，表示变化本身不会使其失效。回放保留 PIT、摄取时间及提交/核验边界、可用日期 max、见证 ID 和上游收据。证明中的批次观测范围只减少窗口读取，冻结与每次验证仍保留并重新核验全部原批次。临时排序连接及文件及时关闭、删除。

`lake.items.update(..., input_windows={输入别名: (起始日, 结束日)})` 可显式声明依赖观测窗口。Data 校验别名、日期次序和已声明的边界，并将窗口写入真实冻结请求；不改写 DataItem 定义或哈希，不推断任意生产者的滚动/前向填充范围。未指定的依赖保持原范围，general Raw 快照不裁剪。精确复用读取定义、依赖和完成区间记录，不读取数值或审核原字节；原始字节审核由 `inputs.verify` 显式执行。已完成范围改变 input_windows 需要选中输入的影响证明，不能绕过增量计划。
`same_date` 生产者只扩大 by-date Raw 窗口上界时，可使用元数据证明：定义、其他请求字段和下界不变，旧上界覆盖旧信息截止日期，并且原提交边界下、截至原核验边界的完整空结果证据一致。Raw 可用日期不会早于观测日期，所以新增未来观测不能改变旧的选中结果，旧结果为空时也一样。已完成前缀的常规数值和时序影响检查继续生效；扩大下界、旧上界早于旧截止日期、General 和 Item/传递依赖仍需原有时序证明。仅仅混合了基线和已验证行，不会阻止已经证明安全的未来扩展。
新窗口冻结使用现有不可变批次的观测/可用日期范围，保留可能入选的批次、晚到修订、相关核验、重叠空区间和无法确定范围的证明。完整提交证明保留全部原始批次引用。旧收据按原表示检查有效性，不重写历史字节或哈希；只读按相同提交、分区和哈希的范围跳过不可能入选的批次，general 初始化基线保留完整。无法证明记录窗口归属的共享提交核验采用保守失效，无 schema 迁移或隐式缩短历史。

冻结裁剪只读取收据/证明中已捕获并校验的范围，旧收据沿用宽范围读取，不使用后来变动的在线摘要改变历史值。类型明确的空 Item 窗口保留已捕获 schema，其他月份首次建立物理 schema 不会为该空窗口增加值；相关数据或依赖变化仍使其失效。

相关外部输出使用 `with items.publication(input_receipt=receipt, config=..., cancelled=...) as publisher`，再逐组调用 `publisher.publish([ItemPublication(...)])`。上下文进入时读取固定输入记录一次；每组发布前检查定义 hash、依赖身份、frame 和范围，再沿用历史写入和完整范围替换。`start/end/complete_at` 须一起提供；报告为每张表最后一步的结果，不是累计行数。只保留当前组 frame，元数据提交串行且必须在创建上下文的线程执行。失败或取消保留已成功提交的数据，但失败会使上下文立即失效，即使调用者捕获异常也不能继续或成功退出。退出再次检查取消，旧 publisher 不能复用；原始字节完整审核保留在显式 inputs.verify；普通发布仍执行类型和身份检查。

完整捕获的批次／复查／成功构建父凭据均递归有效，且原有构建证明属于这些父凭据时，可直接证明父依赖有效。缺少全范围构建证明时，还可使用前述完整非空可见批次证明。任何父凭据失效都回退到原有截止日和窗口选择；未被选中的旧父凭据失效不会直接否定结果。原始字节完整性核验保持独立、每次重新执行。

## 增量 producer 执行

`items.update` 的显式 start、input_windows、force 均经过唯一 owner 计划。
成功区间证书记录完成边界，包括空结果；无变化不冻结输入、不调用 producer。
声明 `ProducerUpdatePolicy("same_date")` 的因果 producer 只计算新增/受影响日期；
forward 允许修订影响后续区间。有限 history_observations 必须绑定完整 General
calendar_input/calendar_field 及可选 calendar_selector=(字段, 整数值)，按完整
计算日历计数，包含没有结果行的日期。声明不改变已保存的数值定义。
Select/Filter/MapValues/Cast 保持观测坐标的链路有内置逐日合同；其他链路缺失
证明时明确阻塞。force 仅重算计划内单元，无变化不重放。初始化/数值定义变化
允许完整计算；依赖定义变化、范围缺口和未知影响不能静默完整重算。

实际消费类型化输入时记录收据绑定的完整日历投影；历史修订缺少该投影时，
显式调用 inputs.index_coordinates(receipt, alias, field="time", selector=("is_open", 1))
维护，普通前置计划不触发维护或读取原字节。纯追加不要求旧投影。
inputs.verify 从原始证据审核投影与捕获范围，保留原收据/历史字节。

BuildContext 区分输出范围、信息截止日和 completed_through。有限 producer
读取必要前置日历窗口，逐日 producer 只读取输出日期。原生非空输入直接使用
实际消费窗口在各信息截止日已选中的 baseline 标记，不为时间判定重新读取无关
旧历史，也不将未消费的旧 baseline 传给已验证的追加结果。空输入仍沿原有传递
时间证明处理，保留父级所需前置窗口和完整 General 事件；外部发布仍验证完整
声明请求的时间。严格 PIT 读取保持真实首次可知边界。定义、callable、runtime policy
在规划、复核和执行中固定，取消不发布未完成的整体区间证书。

inputs.freeze(..., retain_batch_bounds=True) 为完整请求捕获物理批次范围但保留
全部原批次；新 Item 构建使用它跳过旧月份 Parquet。旧收据保持宽读取，在线摘要
变化不能改变已捕获读取；附加物理范围不改变完整源身份或数值依赖 digest。

明确 timing_neutral_inputs 的 General 同内容重复复核，若原选中快照已验证可见，
且每个新边界保持同一序号和已验证时间状态，可只凭该别名的登记证明复用。
原截止日内的重复复核也可复用，但必须保留原首次已知日期；更早的首次可见日期
仍属于历史时间修订，要求显式影响证明。
其他依赖的修订单独规划；无关旧索引缺失不会使日历证明失效。首次基线提升仍
要求旧输入 current、所有输出提交保持统一基线及每个新边界存在可见基线。
时间必须不影响数值或输出 trace，否则要求显式时间证明刷新。历史输出坐标存在时，
General 新内容仍要求选中行影响证明。

只消费 General 指定列的 producer 可声明不可变 runtime policy，例如
`ProducerUpdatePolicy("same_date", timing_neutral_inputs=("inventory",),
general_fields=(("inventory", ("asset_id", "category")),))`，不改数值
DataItemSpec。别名必须是已声明的完整 General Raw 输入。字段证明必须有选中
atomic snapshot（包括类型明确的空快照）；没有可见 event 不能借当前或 union
schema。有选中 atomic event 时，各已捕获 schema 还必须声明相同消费 dtype，
冲突或缺权威明确阻断，包括尚未可见的未来类型变化，不能绕过 no-op 核验。
无选中 event 且没有相关变化的历史完成沿用原元数据证据，不凭空建立字段缺席
证明。原快照已验证且非基线时，
已登记的类型投影相等可证明未消费列变化不影响历史输出。规划逐项检查每个新增或
捕获未来的 atomic commit，包括同一 PIT 日期的多个提交，并保留实际可见边界。
缺摘要明确给出原/当前 `inputs.index_selection` 的 max_commit/as_of 维护提示；
普通规划不读原始值或行索引。消费列真变、首次/更早可见仍阻断，要求显式影响
解决，不隐式全量回放。基线提升、输出 anchors、其他依赖及发布锁各自仍有效。
`update`/`recertify(config=...)` 将调用方预算传入元数据规划和 no-op 来源绑定；
只读 `plan_update` 可沿用显式 `inputs.read_context(config=...)` 的预算。

逐资产 General 查询还可声明 `general_asset_local_inputs=("inventory",)`。每条查询只能
影响同一非空资产键的输出，不能使用总行数、跨资产关系或空键 General 记录改变非空资产
输出；`general_fields` 必须包含 `asset_id`，并声明完整 `output_anchors`。声明本身
不能证明历史可复用。

显式调用现有 `inputs.prepare_general_projections(original_source, "inventory",
("asset_id", "category"), through=target, key_anchors=("daily", "suspensions"),
completed_start=first_completed_day, completed_end=last_completed_day)` 可准备紧凑证明。
原选中状态和新状态必须已知且非基线，定义与消费字段类型一致；所有旧类型化字段元组
必须完全保留，包括空资产键元组及其重数，只允许增加唯一的非空 String 资产键。
逐个检查新原子提交，包括同一 PIT 日期的多次提交；同日更晚提交可以证明，回填到更早
日期或未知可见性不能证明。新增资产必须在原始和当前冻结边界下的所有已完成区间锚点
中都没有坐标；额外自然键及数值为 NULL 的撤回仍算存在。旧字段变化、删除、空键元组
变化和历史锚点晚到均继续阻止增量更新。普通计划和已知证明复用只读元数据，显式
`verify` 沿两份 Source 重算新增键与每个锚点的空选择。后续仍执行原生更新计划的尾部，
不增加逐行索引、新表、迁移或旁路发布流程。
摘要键还绑定当前每个锚点的精确选中证据、请求和截止日期。同一个 General 状态在完整
当前提交边界与较早原子边界下具有不同稀疏可见性证据时，两份证明分别保存，不会覆盖。
无关全局发布不会使已知证明失效。已有仅 General 绑定的摘要只能按其两份原始 Source
进行显式历史审计，普通计划不会回退使用；新摘要正文与键的范围绑定必须完全一致。

same_date producer 可声明 output_anchors=("daily", "suspensions")，输出坐标必须属于
这些已登记 by_date Raw 输入的坐标并集。非法声明在上游计算前拒绝，producer 输出
在发布前验证。所有 anchor 都证明历史区间没有坐标时，可在 General 内容或时间
变化后复用原完整空结果。证明绑定原捕获批次 header 并检查当前全部可能坐标；
未知范围、历史迟到坐标或第二个非空 anchor 都阻止此捷径。它只证明输出不受影响，
不能证明两个输入收据等价。

完整覆盖只由原始 complete-range publication witness 证明。普通 items.ingest
只证明发布的行，不证明其他键为空。证书绑定原 build 与完整 pending visibility
集合（包括显式空集合）；输出在旧截止日后变为可见时，修复其原观测日期。

历史维护显式调用 items.index_completion(ItemCompletionAttestation(...))。
调用方保留原完整 producer report 及 SHA256 引用、完整输出 membership/撤回/
空范围权威，以及原截止日的全部待可见坐标。声明必须精确匹配原 build、定义、
冻结输入 digest/边界和结果 commit；不能从行范围/行数或字节审计推导完整性。
补证后的原完整范围可作为空稀疏 no-op 延续的前置证书，原证明和待可见坐标保持不可变。
须将 `output_receipt_id` 和 `output_receipt_digest` 绑定原完整范围输出快照，并使用其
原检查边界；维护不吸收后来检查，也不按时间戳猜测检查顺序。
来源、定义及输出/检查边界保护仍完整执行；变化范围的稀疏发布仍须原生前置证书。
证明原子写入且不可变，原结果边界不向后推进；后续发布仍会使 currentness 失效。
缺少原报告或 pending 知识仍阻塞，普通前置检查不自动维护。

`items.index_output_reuse(name, end=..., config=..., check_canceled=..., progress=...)`
显式审计后续输出批次与原完整结果，比较数值、观测/信息/版本可用日期及基线，
排除创建者来源元数据，登记实际受影响日期。普通计划只读绑定原完成证明、
批次、截止日的记录，不读取原始 Parquet 数值；输入修订仍独立规划。记录缺失/过期
保留原受影响范围，损坏则失败。已有输出时间核验仍须进一步选中版本证明。
维护不调用 producer、不推进完成边界，取消保留完整登记的批次与全部历史证据。
截止日向后推进时，若批次已登记的最大物理可用日期完全处于维护范围内，且
该 Item 的时间核验边界未改变，则复用证明继续有效。维护未覆盖的未来版本
仍为未知；实际受影响日期始终按请求截止日的可见性过滤。
显式维护在剪枝参照批次之前，先逐原始批次审计日期边界，并绑定完整原始批次
元数据集合；普通计划只核对该绑定，不读取原始字节。原始头信息变化会失败，
不能用未审计边界排除较新的原正确值、从而误复用过期结果。

只读 `items.plan_update(..., update_policy=ProducerUpdatePolicy(...))` 可传入
外部生产者的因果声明，不登记可执行代码或写目录；它必须与已登记的运行期策略
一致，内置中性变换仍使用自己的策略。
`inputs.selection_identity(..., values_only=True, coordinates=keys)` 可按唯一
Date `time` / String `asset_id` 坐标证明实际消费的 scalar，包含缺失键。先完成
可见性和版本选择，再限制坐标并绑定规范顺序的坐标身份；缺索引或预算不足返回
未知，不自动扫描历史原始值。

`inputs.latest(receipt, alias, through=..., config=..., validate_rows=...)`
逐月读取，保留每个 asset 最后的 scalar 观测。through 仅限制观测日期，不替换
原冻结信息截止日，因此仍正确选择晚到修订。必须已有捕获的物理范围，且状态受
显式预算约束。消费者可在每个 typed 窗口合并前按自己的规则校验、筛选行；数值
准入规则仍归消费者。此读取用于初始化或显式 checkpoint 维护，不能用于普通前置。
scalar 摘要仅在捕获范围和所有 witness 日期证明完整事件流已经可见时归一截止日；
未知范围或空 baseline 证据仍绑定实际截止日。
新 batch 索引还记录 availability 相对观测日的最大延迟。在原 content/witness
headers 相同的前提下，这份可选且经过原字节审计的范围证明，能够判断部分 batch
在较短消费观测窗口内是否已经完全可见；不解码行索引，也不把实际截止日改成观测日。
缺范围仍未知，完整 Source freshness 是独立证明。

空前段与保留尾段可由多个原生完成范围分别证明。无界 snapshot/versions 请求即使
捕获了局部完成记录，也必须组合其他原生范围覆盖剩余位置；局部记录不成为全范围
证书。每个捕获来源的全部批次与空检查
必须完整落在证书范围内，并满足输出 commit 与检查 ID 边界；替代 Source 仍须通过
普通 currentness。未知范围或未覆盖位置继续失效，原收据保持不可变。检查先裁剪
无关范围，再按调用方内存预算准入证明元数据。

没有声明上游、也没有捕获生产者或空范围来源的叶子 Item，在元数据完全一致、没有
待成熟输出、完整批次已证明非空且可见时，可直接完成上游检查，无须查询数值选择
索引。空、未知、strict 排除、未来或窗口外选择仍遵循原来的完成与索引要求。

较早的原生完成记录若缺少输出检查边界，须显式调用
`items.index_completion_output(build_id, output_receipt_id=...,
output_receipt_digest=..., report_id=..., report_digest=...)`，提供真正原始发布的
Frozen 输出和生产者报告，不能用新 freeze 替代。不可变补充索引绑定原 native
authority、pending 证明和原检查边界，不改原记录或数值历史；未知/外来范围及
边界后的检查继续阻止复用。

同一冻结 Source 的空稀疏续接，在输出与全部上游依赖不相交时，共用一次受写事务保护的记录有效性检查。每个输出仍检查原始覆盖、完成证据、待成熟坐标、定义及输出提交和检查边界。整批空增量完成记录原子提交；取消或失败会回滚整批新记录并释放锁，不读取或重发历史数值。包含变更范围或上游输出的发布仍使用原有逐项流程。


## 小型存储与完成检查

Schema 8 将全部历史数值和首次可见事件仅保存在原始不可变 Parquet。SQLite 保存
声明、小型文件/变化元数据、节点完成 flag 和冻结引用。`range_identity` 与普通
`selection_identity` 只读元数据，无需原始读取或准备索引。可选精确
`index_selection(s)` 和小型坐标轴需要显式准备，普通身份不因维护变化。
`inputs.verify` 用有界读取器核验原始 Parquet 字节、类型、边界及摘要；原始文件
损坏或缺失必须恢复可信备份。

Raw 与 Item 的 `description` 仍保存在完整声明和导出配置中，但不参与数值定义、
依赖或范围身份。仅修改描述会保留完成 flag 和已有计算结果。其他全部声明字段，
包括 Item 的任意 metadata，仍参与数值当前性判断。

`complete_at` 与 `replace_range(..., available_date=...)` 将检查目标推进到实际
完成日期，观测数据覆盖范围单独保留。仅推进完成日期不会重写相同内容或另造
发布日期。ingest 或发布 frame 中显式提供的历史日期仍是真实 PIT 版本。
