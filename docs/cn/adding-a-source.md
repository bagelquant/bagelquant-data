# 新增数据源

数据源适配器负责获取外部数据，并可选择规划数据源专用请求。新增数据源不应要求修改存储、查询、财务处理或管理模块。

## 数据源协议

实现 `DataSource` 协议：

```python
from collections.abc import Iterable, Mapping
from typing import Any

import polars as pl

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.request import RequestContext


class MySource:
    @property
    def name(self) -> str:
        return "my_source"

    def configure(self, **options: Any) -> None:
        ...

    def test_connection(self) -> None:
        ...

    def fetch(
        self,
        source_dataset: str,
        request: Mapping[str, Any],
    ) -> pl.DataFrame:
        ...

    def plan_requests(
        self,
        dataset: DatasetSpec,
        context: RequestContext,
    ) -> Iterable[Mapping[str, Any]]:
        ...
```

## 注册数据源

```python
from bagelquant_data import DataLake

lake = DataLake.open("data")
lake.sources.register(MySource())
lake.sources.configure("my_source", token="...")
lake.sources.test("my_source")
```

## 凭证规则

凭证应通过以下方式提供：

- 环境变量
- 运行时配置
- 本地未跟踪配置
- 未来的 secret provider 集成

不要把 secret 写入：

- 数据集 YAML
- Parquet 文件
- 已提交文档
- 已提交 TOML 文件
- SQLite 元数据行

## 请求规划

`plan_requests` 接收 `DatasetSpec` 和 `RequestContext`。

对 snapshot API，发出一个请求：

```python
yield {"start_date": context.start, "end_date": context.end}
```

对按资产请求的 API，每个资产发一个请求：

```python
for asset in context.assets or []:
    yield {"asset_id": asset}
```

对分页 API，发出分页参数。核心更新 pipeline 不应知道提供商的分页细节。

## 获取数据

`fetch` 返回 Polars `DataFrame`。如果提供商 SDK 返回 pandas，在适配器内转换：

```python
return pl.from_pandas(response.copy(deep=True))
```

## 标准化边界

数据源适配器应尽量保留源字段。标准命名通过数据集 spec 和 normalizer 完成。

不要在适配器里隐藏有经济意义的记录。有效修订、重述、重复预测和多个 point-in-time 记录都应进入标准存储，除非校验证明它们格式错误。

## 测试建议

推荐测试：

- `repr` 不泄露 token
- `configure` 不持久化 secret
- `test_connection` 抛出有用错误
- `fetch` 将提供商响应转换为 Polars
- `plan_requests` 尊重 `start`、`end`、`assets` 和 `source_options`
