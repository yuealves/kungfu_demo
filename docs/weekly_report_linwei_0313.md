# 周报 - linwei 2026.03.13

## 本周工作

### 1. 华泰新机器线上 Parquet 归档（archive_journal_demo）

开发并在华泰新机器线上部署了每日收盘后自动 dump Parquet 的代码，读取当天全量 journal 数据归档为 tick/order/trade 三类 Parquet 文件。上线四天，导出的parquet数据经验证正确性与csmar一致，且没有丢包问题。

### 2. 华泰新机器线上延迟监控（latency_monitor）

在华泰新机器线上增加了 latency_monitor 监控，用于实时观测行情数据从交易所到本地写入的端到端延迟。上线3天正常运行中。已与韩老师沟通让韩老师对该监控文件增加通知到他的钉钉告警。

### 3. KungFu nanotime bug 修复与线上替换

发现线上行情代码使用的 KungFu 开源库存在 nanotime bug，定位问题后修复了源码，解决了整个项目的编译问题，完成线上代码替换。替换后能成功启动，且验证了 bug 已被修复。


### 4. 实时分钟级 Parquet 写入（realtime_parquet）开发中

开发 `realtime_parquet` 子项目：实时从 Paged journal 读取 L2 行情，按分钟写 tick/order/trade Parquet 文件以及 1min bar数据，做到亚秒级延迟。

核心设计：
- Row Group 预压缩：数据到达时增量构建 Arrow builders，满 50000 行即写出 Row Group（Snappy 压缩），分钟边界只 flush 残余，延迟 < 20ms
- 用 nano_timestamp（journal 写入时间）而非 exchange_time 做分钟切换，避免交易所多 channel 并行导致的 exchange_time 乱序问题
- 自包含打包部署：依赖通过 symlink 引用，`tar -czh` 打包时解引用，可直接上传线上编译

当前状态：本地 replay 测试通过，预计下周中可上线华泰新机器。


## 下周计划

- realtime_parquet 上线华泰新机器，验证实盘环境下的延迟和稳定性
- 在 1min bar数据以外，增加shenrun提供的一些更复杂的因子的实时生成。
