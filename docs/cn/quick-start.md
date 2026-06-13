# 快速开始

`bagelquant-data` 是 provider 中立的数据层，用于管理 provider 适配器、本地数据湖快照、元数据和面板形状读取。

## 安装

```bash
uv add bagelquant-data
```

如果需要使用 Tushare：

```bash
uv add "bagelquant-data[tushare]"
```

## 注册 Provider

```python
from bagelquant_data.datasource import DataSourceRegistry, TushareDataSource

registry = DataSourceRegistry()
registry.register(TushareDataSource(token="your-token"))
```

## 创建本地数据湖

```python
from bagelquant_data.lake import DataLakeManager, LocalDataLake

lake = LocalDataLake(".bagelquant-data-lake")
manager = DataLakeManager(lake, registry=registry)
```

## 写入或刷新数据

自定义数据可以直接写入 Polars frame：

```python
import polars as pl

prices = pl.DataFrame(
    {
        "time": ["2024-01-02", "2024-01-02"],
        "asset_id": ["000001.SZ", "600000.SH"],
        "close": [9.83, 7.12],
    }
)
manager.add("custom", "prices", prices)
```

Provider 数据通过 `DataRequest` 读取并保存为本地快照：

```python
from bagelquant_data.datasource import DataRequest

manager.update(
    "tushare",
    DataRequest(
        dataset="daily",
        fields=("ts_code", "trade_date", "close"),
        start_date="2024-01-01",
        end_date="2024-01-31",
    ),
)
```

## 读取数据

```python
from bagelquant_data.loader import Loader

loaded = Loader(registry=registry, lake=lake).source("tushare").load(
    "daily",
    fields=("open", "close"),
    start_date="2024-01-01",
    end_date="2024-01-31",
)

frame = loaded.data
```

研究输入通常读取为 long-form 面板形状：

```python
retrieved = Loader(registry=registry, lake=lake).source("tushare").load_panel(
    dataset="daily",
    field="close",
    universe=["000001.SZ", "600000.SH"],
    start_date="2024-01-01",
    end_date="2024-12-31",
)
```

`RetrievedPanel.data` 是包含 `time`、`asset_id` 和 `value` 列的 Polars frame，同时暴露 calendar 和 universe，由下游包决定如何构建自己的 domain 对象。

