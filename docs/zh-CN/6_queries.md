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
省略 check 上界时，通过运行时 SQLite 协调读取已提交元数据，不复制数据库或 WAL。
进入边界后提交的新 check 不会扩大已捕获的上界。

重复读取同一冻结输入时可进入有限生命周期的只读上下文：

```python
with lake.inputs.read_context(receipt, check_canceled=check_canceled,
                              progress=progress) as reader:
    january = reader.read(receipt, "close", start="2020-01-01",
                          end="2020-01-10").collect()
```

`inputs.window_read_supported(receipt, alias)` 只读取元数据：所有按日期批次都有
已捕获的观测边界、seal 或准确绑定原批次的既有索引摘要时返回 true；General
或缺少旧摘要返回 false。不读取索引行或原始字节、不自动构建/审计索引，也不从
实时批次头猜测旧范围。恢复窗口按 public read_context 的 max_buffer_bytes
预算，在解析摘要和读取原始批次前检查所选行总量与取消。未知支持不能视为有界月读。

观察日期边界还会下推到所选原始 Parquet 和首次可见性事件的扫描，在生成临时
自然键哈希之前过滤无关观测。读取器先从物理文件 footer 检查完整原文件的 schema
和总行数，再消费匹配观测；首次获知日期不等同于观察窗口。General 仍先选择完整
事件，Raw 可用日期下界仍在选择最新版本之后过滤。请求字段在 PIT 选择后投影，
完整字节验证仍为显式审计。

## 可选元数据查询索引维护

`lake.inputs.index_record_lookups(config=ExecutionOptions(...))` 显式安装已完成
build 和冻结 Source 检查边界的 SQLite 查询索引。它只扫描元数据，在同一写事务
中原子完成，支持 `check_canceled`；失败回滚两个索引，不修改原始 receipt、
检查记录、数值或规范身份。排序的临时磁盘空间需单独考虑，内存预算不限制磁盘占用。

新空表初始化这些轻量索引；打开已有非空数据库不会隐式补建。普通前置检查仍只
读取登记的元数据。查询索引不证明数值相等或当前有效性，选择/检查证明维护和
原始字节验真仍是独立的显式操作。


普通 is_current 调用在同一次有限元数据视图内复用精确的检查与提交关系，受调用方预算限制。
事务内写入会使缓存失效；外部 WAL 提交在下一次操作中可见。此复用不会校验历史字节或创建证明。
显式 index_record_lookups 包括完成记录、数据集版本检查和冻结 Source 检查边界查询索引；
任一缺失索引的安装失败都会回滚本次安装，原记录不变。


inputs.index_selection 显式读取有界的原始类型证据并建立标准标量摘要，不持久化历史逐行索引。
coordinates 可传入准确的 Item 日期/股票键，包括缺失值对应的键；共同选择范围只保存一次。
普通检查只复用精确范围的既有记录，缺失证明返回未知；完整 verify 从原始字节校验摘要和选择范围。
维护需要独立写事务、显式资源预算及取消检查，保留全部原始回执。

显式批量消费投影维护使用 `InputSelectionScope` 和
`inputs.index_selections(receipt, scopes, config=...)`，每组同一别名、1–16 个
精确范围，组内共享原始批次传输，使用单范围/已有索引相同的规范选择器。
`coordinate_selector_identity` 只验证并计算实际坐标摘要，不读写数据；首次维护
登记实际掩码，后续显式组可引用该选择器。普通证明仍绑定调用者实际坐标。
各组原子发布并检查合计内存预算，不建立历史逐行索引，不计算上游。

共享前提检查可用 `inputs.selection_descriptors(receipt, scopes, config=...,
check_canceled=...)`。每组同一别名、1–16 个实际坐标 scope，返回可保存为 JSON
的 Source/alias、规范请求和 evidence/request key、fields/coordinate identity、
known/unknown、完整内容 token 及原证明 Source 引用。unknown 不隐式准备；显式
维护仍只有 `index_selections`，其 original_rows 计数表示批次头中的原行数，
与实际传输、标量哈希工作分别记录。

`inputs.resolve_selection_descriptors(receipt, descriptors, ...)` 只重核已登记的
完整原 descriptor、Source 与 selector 头，再对指定 Source 解析同一 scope。
不解码 selector IPC、不重建 membership、不签发新轴。`as_of=None` 使用目标
Source 的信息截止日期，显式 ISO `as_of` 保持固定；`requested_cutoff` 是实际解析
日期，`canonical_cutoff` 是所选流的规范日期。目标摘要缺失返回 unknown，伪造或
未登记的原证明明确失败。消费者还须独立证明 Universe、calendar 和策略轴不变，
才能复用旧 descriptor；首次或变更轴仍传实际键。投影 memo 有限且在上下文结束
或自身写入（包括回滚写入）后失效。

General `coordinate_identity` 可跨 Source 继承完全相同的原 atomic 权威。
显式 `index_coordinates` 保留唯一原日期 payload，并在既有 projection index
登记规范定位键和原 Source 引用；普通读取不会补建可选记录。revision、baseline、
cutoff、field、selector 或 window 不同须有准确证明，消费字段混合 schema 在缓存
正证明之前阻断。`verify` 追溯并审计原 Source/payload，即使由新 Source 消费。
日期 token 不证明 timing 或全行数据相等。

外部 producer 构建前可显式调用 `inputs.prepare_general_projections(original_receipt,
alias, fields, through=end, config=..., check_canceled=..., progress=...)` 准备 General
字段前提。原输入 receipt 来自 `items.completed_builds(name)`，producer runtime
policy 仍由调用者声明。该 API 与普通 planner 共用原状态和全部新可见 atomic 状态
枚举，含同日每个中间 commit。只有缺失的规范 scope 登记捕获 Source 元数据，并走
既有 `index_selections` 单 scope 预算组；已知 scope 不读原行，不改原证明 Source。
取消/预算失败保留已完成组，重试复用。无 eligible schema 记录为 unsupported，
不伪造 typed 空证明。work 分列 reused/prepared、groups/new_sources、原批次头行数
和传输；字段准备不放宽 timing、baseline 或真实消费字段变化，普通 planner 仍决定
producer 是否可运行。该操作不调用 provider 或 producer。

消费日期通道时，可在 `InputSelectionScope`、`index_selection`、
`selection_identity` 或 `selection_equivalent` 传入
`fields=("time", "asset_id", "value", "available_date", "observation_date",
"base_available_date")`。投影使用原来的 PIT、strict、baseline 和版本选择，
绑定字段实际类型和值，空结果仍绑定投影类型。字段必须存在于原冻结 schema，
不会合成日期。typed 空 by-date 发布会在原空检查事务中保留生产者给出的完整 schema，
包括额外列；发布后冻结即可绑定这些类型。只有声明、尚未发布的 Item 仍没有未声明的列。
显式 fields 表示消费投影，普通检查只读取已登记摘要，缺失时返回未知；
默认 `fields=None` 保持原标量证明身份。`selection_equivalent` 还接受实际
`coordinates`，保留不存在的键，并拒绝超出任一原窗口的键。Item 使用 Item 日期轴；
显式 fields 的 by-date Raw snapshot/history 坐标使用观察日 source_time（传入列仍名为
time），原版本 availability time 保留为独立消费字段。额外主键对应的全部已选行都会保留，
默认标量 Raw 证明仍拒绝坐标。
General 也支持显式字段的完整快照投影：传 `view="snapshot"`，不传观察窗口、
坐标或 causal versions。不推断逐日或资产网格，保留全部已选行，额外主键产生的
重复字段组合也不会去重。字段校验和空结果类型使用选中原始快照的 schema，
不能借后来数据集 schema 补列或换类型。已选的类型明确空快照受支持；没有可见
atomic event 时缺少原始选中 schema 权威，字段证明在读原字节或写摘要之前抛出
`ValueError`，不能从当前或 union schema 合成证明。
既有 General 数值读取先拼接保留快照再选 atomic，混合数值类型可能发生转换。
因此字段证明要求所有已捕获 atomic schema 的每个消费字段 dtype 相同；缺权威
或类型冲突在缓存证明捷径、原字节读取和摘要写入之前抛出 `ValueError`。
未消费列的 schema 变化不影响投影。
摘要沿用原 commit/check/batch 权威，字段相等不能证明发布时间或完整 Source
新鲜度。缺证明时按提示对原/当前状态显式 `index_selection(...,
view="snapshot", fields=(...), as_of=...)`；同日中间状态先以提示的原
`max_commit` 冻结，再登记该投影，不能用最终状态相等替代。普通检查不执行维护，
默认标量 General 投影仍不支持。
未消费字段或非成员坐标变化不影响投影；Raw 来源 mode、coverage 和生产者
完整新鲜度仍需各自原始证据。`inputs.verify` 从有界原始证据重建摘要及选择器。
新增独立日期和仍不可见的未来检查不会使已登记的历史字段摘要失效；新回执可
继续复用原摘要，普通证明无需重读原始字节。


## 小型存储与完成检查

Schema 8 将全部历史数值和首次可见事件仅保存在原始不可变 Parquet。SQLite 保存
声明、小型文件/变化元数据、节点完成 flag 和冻结引用。`range_identity` 与普通
`selection_identity` 只读元数据，无需原始读取或准备索引。可选精确
`index_selection(s)` 和小型坐标轴需要显式准备，普通身份不因维护变化。
`inputs.verify` 用有界读取器核验原始 Parquet 字节、类型、边界及摘要；原始文件
损坏或缺失必须恢复可信备份。
