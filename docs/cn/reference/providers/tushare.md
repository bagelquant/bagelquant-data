# Tushare

Tushare 是 V1 的第一个 provider 适配器。

安装可选依赖：

```bash
uv sync --extra tushare
```

显式传入 token：

```python
from bagelquant_data.datasource import TushareDataSource

source = TushareDataSource(token="your-token")
```

也可以通过环境变量提供：

```bash
export TUSHARE_TOKEN=your-token
```

## 支持的数据集

V1 支持：

- `stock_basic`
- `trade_cal`
- `daily`
- `index_daily`
- `generic` with `options={"api_name": "..."}`

Tushare 日期会标准化为 `YYYYMMDD`。Token 不会出现在 `describe()` 输出中。

## 参考资源

Tushare 的 `All` universe 来自 `stock_basic`。刷新时会读取上市、退市和暂停上市状态，并按 `ts_code` 去重，因此退市和暂停上市股票仍可用，避免幸存者偏差。

Tushare 表更新也会使用来自 `trade_cal` 的本地交易日历。参考资源与普通表更新分离，所以增量刷新价格表不会覆盖 `stock_basic` 或 `trade_cal`。

## 更新策略

- `stock_basic` 刷新 All universe，`trade_cal` 刷新 source 交易日历。
- `daily` 和 `index_daily` 按开放交易日逐日抓取，避免 Tushare 行数限制。
- 已存在价格日期通过更新记录表跳过，新增交易日不会重写旧日期。
- 财务类表按代码生成任务，并从该资产已有最新日期继续。
- 默认更新范围为 `2000-01-01` 到今天。
- Provider 请求可以通过 `workers` 并发执行。
