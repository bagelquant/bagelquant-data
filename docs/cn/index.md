# BagelQuant Data

`bagelquant-data` 是 BagelQuant 的数据层，负责 provider 接入、本地数据湖快照、元数据、转换流水线和面板形状的数据读取。

它不负责研究图、组合构建、回测或应用 UI。它的输出边界是 pandas 数据和轻量元数据，方便下游包自行构建 `Domain`、`Panel` 或回测输入。

## 推荐阅读

- [快速开始](quick-start.md)
- [架构与设计](architecture.md)
- [概念](concepts.md)
- [参考文档](reference/index.md)
- [公开 API](reference/public-api.md)
- [内部实现](reference/internals.md)
- [后端 API](reference/backend-api.md)
- [数据契约](reference/contracts.md)
- [Panel 对接约定](reference/panel-agreements.md)
- [Tushare provider](reference/providers/tushare.md)

