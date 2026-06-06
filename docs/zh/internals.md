# 内部实现

本页面面向维护者和扩展作者。

## 依赖方向

`bagelquant-data` 位于生态底层，不应导入 `bagelquant-core`、`bagelquant-bt`、`bagelquant-app` 或网站代码。

与 `bagelquant-core` 的边界是普通数据：`RetrievedPanel` 暴露 DataFrame、排序后的 calendar 和 universe。下游代码自行构建 `Domain` 和 `Panel`。

## 存储布局

本地 V1 存储按 source、table、year、month 分区：

```text
lake-root/
  tushare/
    daily/
      year=2024/
        month=01/
          snapshots/
```

JSON catalog 保存 source 级 id、表元数据、最新快照指针和推断出的 panel 字段。读取时利用分区元数据减少 IO，再做精确过滤。

## 更新流程

普通 provider 刷新：

```text
DataRequest -> DataSource.read -> DataLakeManager.update -> LocalDataLake.write
```

Tushare 生产式刷新：

```text
刷新引用表 -> scan_tushare_updates -> 审核报告 -> 执行任务
```

报告是审核边界。执行阶段应消费报告中的已确认任务，而不是隐式重建计划。

## 失败处理

Provider 和 lake 错误应尽量包含 source、dataset、日期范围和操作名。缺失可选 provider 依赖时，应在构造适配器或首次使用 provider 时失败，而不是在导入包时失败。

