# 周报 - linwei 2026.03.27

## 本周工作

### 1. 华泰新机器上线realtime_kbar

开发并上线了针对华泰实时 tick 数据生成 1min kbar 数据的 realtime_kbar 程序。（相比离线数据合成更烦琐，因为实时 tick 数据没有 channel 信息，会乱序到达）

### 2. 和 shenrun 对接调整 realtime_parquet

按 shenrun 需求调整了 realtime_parquet 生成 parquet 数据的方式，方便 shenrun 下游使用

### 3. 完成实时因子生成架构设计 & 针对 shenrun 提供的 demo 因子开始开发

架构已完成，相关文档已同步。
现针对 shenrun 提供的 demo 因子进行开发调试中。

## 下周计划

- 完成 shenrun 提供的 demo 因子的实时生成