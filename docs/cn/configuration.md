# 配置

最主要的配置是传给 `DataLake.open(...)` 的数据湖根目录。

```python
from bagelquant_data import DataLake

lake = DataLake.open("data")
```

打开数据湖会在该根目录下创建所需存储区域。

## 默认本地布局

```text
data/
    lake/
    staging/
    rejected/
    metadata/lake.db
    tmp/
```

仓库中也包含一个示例配置文件：

```text
config/bagelquant-data.toml
```

当前阶段，Python API 是事实标准。TOML 文件记录预期默认值，可供未来 CLI 或应用集成使用。

## 依赖

核心依赖：

- `polars`
- `pyarrow`

Tushare 可选依赖：

- `pandas`
- `tushare`

安装或同步：

```bash
uv sync
uv sync --extra tushare
```

## 凭证

凭证应在运行时配置，不应写入数据集 YAML、Parquet 文件、提交的配置文件或文档示例。

Tushare 可以直接传 token：

```python
from bagelquant_data.sources.tushare import TushareSource

source = TushareSource(token="...")
```

也可以注册后配置。该方法会把 token 保存到本地数据湖元数据 DB，后续运行只需注册 `TushareSource()`，更新时不必再次传 token：

```python
lake.sources.register(TushareSource())
lake.sources.configure_tushare(token="...")
```

也可以使用环境变量：

```bash
export TUSHARE_TOKEN="..."
```

保存的 source options 仅属于当前 lake root，位于 `data/metadata/lake.db`，在 source 列表中会被脱敏显示。

## 本地开发

运行测试：

```bash
uv run pytest
```

运行类型检查：

```bash
uv run pyright
```

运行轻量 CLI：

```bash
uv run bagelquant-data --root data status
uv run bagelquant-data --root data source-list
uv run bagelquant-data --root data dataset-list --source tushare
```

可复用的 setup/update/monitoring 工作流已迁移到独立的 `manage-data-lake` 仓库。本包只保留可复用的数据湖 API。

## 不需要配置的内容

不要配置旧数据湖布局、遗留迁移路径或兼容性开关。框架只面向新的标准设计。
