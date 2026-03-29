# 流式 Parquet Replay 实现说明

本文说明当前 parquet replay 的实现方式、时间语义、错误传播和关键日志，便于后续维护与排障。

## 背景

原始 parquet replay 的实现是：

1. 扫描目录下所有 parquet 文件
2. 全量读取为 `ReplayFrame`
3. 全量排序
4. 排序完成后才开始 replay

这个方案语义正确，但会有两个明显问题：

- parquet 很大时，启动后要等待很久才开始写 journal
- 在切换到下一个 parquet 文件时，如果同步读取，会把 parquet I/O 延迟混入 replay 的时间轴

因此当前实现改成了“严格全局有序 + 每路双缓冲预取”的流式回放方案。

## 整体结构

主要代码位于：

- `tools/replay_support.hpp`
- `tools/replay_support.cpp`
- `tools/journal_replayer.cpp`

parquet 模式下的核心分层如下：

### 1. `ParquetFileBuffer`

职责：

- 对应一个 parquet 文件
- 一次性把该文件中的记录还原成 `ReplayFrame`
- 提供当前记录与行游标推进能力

说明：

- 当前仍是“单文件全量读入内存”
- 但已经不再是“整日所有 parquet 全量读入内存”

### 2. `ParquetChannelPrefetcher`

职责：

- 管理某一路消息类型（tick / order / trade）的 parquet 文件序列
- 维护双缓冲：
  - `active_buffer_`：当前正在消费
  - `standby_buffer_`：后台预取好的下一个文件
- 当前文件耗尽时，立即把 `standby` 提升为 `active`
- 再由后台线程异步加载再下一个 parquet 文件

这样可以保证：

- replay 开始前只需要同步准备每路的首个可用文件
- 后续切文件不需要 replay 主线程同步阻塞读盘

### 3. `ParquetReplayStream`

职责：

- 管理三路 `ParquetChannelPrefetcher`
- 维护按 `nano -> msg_type -> channel_index` 排序的最小堆
- 每次返回当前全局最早的一条 `ReplayFrame`

对外接口：

- `hasNext()`
- `firstNano()`
- `popNext()`

这保证了：

- 回放顺序与“全量加载后统一排序”的结果一致
- 但回放可以更早开始

## 时间语义

时间语义与旧 replay 保持一致，没有变化。

对于 replay 写出的目标 journal：

- `extra_nano`：原始历史时间轴，来自 parquet 的 `nano_timestamp`
- `frame.nano`：本次 replay 写入时的墙钟时间

也就是说：

```cpp
writers[idx]->write_frame_extra(..., f.nano);
```

这里传入的 `f.nano` 是历史原始 `nano_timestamp`，最终写入 frame header 的 `extra_nano`。

因此下游读取时仍应优先使用：

```cpp
long extra_nano = frame->getExtraNano();
int64_t nano = (extra_nano > 0) ? extra_nano : frame->getNano();
```

## Resume 语义

resume 语义也保持不变：

- 通过扫描目标 journal 最后一个 `extra_nano` 得到 `resume_nano`
- parquet 流式路径在 channel 推进阶段跳过 `nano <= resume_nano` 的记录

实现上：

- 全量参考函数 `loadParquetFrames()` 保留自己的过滤逻辑，用于测试和对照
- 真正的流式 replay 路径不依赖全量过滤，而是在 `ParquetReplayStream` 推进 channel 时统一做过滤

这样可以保证：

- resume 逻辑和流式切文件、promote standby 一起生效
- 不会因为过滤位置不一致而出现跨文件边界错误

## 错误传播

这是当前实现里比较重要的一个点。

### 为什么不能静默结束

假设：

- 当前最后一条合法 frame 已经返回给调用方
- 但后续 parquet 文件预取失败了

如果 `hasNext()` 只是简单返回 `false`，主循环：

```cpp
while (stream.hasNext()) {
    ReplayFrame f = stream.popNext();
    ...
}
```

就会看起来“正常播完退出”，但实际上是“后续 parquet 失败，少播了数据”。

### 当前做法

当前实现里，后续推进/预取错误会缓存到 `terminal_error_`。

之后：

- `popNext()` 可以先把当前已经合法取出的 frame 返回出去
- 下一次 `hasNext()` 会优先检查 `terminal_error_`
- 如果存在错误，就直接抛异常，而不是返回 `false`

这样可以清晰区分两种状态：

- `hasNext() == false`：真的没有更多数据了
- `hasNext()` 抛异常：本来还应有后续数据，但读取失败了

## 日志说明

当前 parquet 模式下，replayer 会打印几类关键日志。

### 1. 启动阶段

```text
[source] parquet: /path/to/output
[parquet] tick files: 3
[parquet] order files: 4
[parquet] trade files: 0
```

表示三类 parquet 的文件数量。

### 2. 文件打开

```text
[parquet] open tick file 1/3: /path/to/20260319_tick_data_001.parquet
```

表示该 channel 的当前 active parquet 开始被消费。

### 3. 文件完成

```text
[parquet] finish order file 1/4
```

表示该 parquet 已经消费完毕，并将切到下一个 standby 文件。

### 4. 回放进度

```text
[replay] 5000000 | time: 09:25:30
```

表示已经 replay 的 frame 数，以及当前 replay 到的历史时间。

## 当前已知边界

1. 当前是“单文件全量缓冲 + 多文件流式推进”，不是 row-group 级别流式读取。
2. 因此若单个 parquet 文件本身非常大，单文件内存占用仍然不小。
3. trade parquet 文件如果目录中不存在，会显示 `trade files: 0`，这不是错误。
4. 依赖环境里仍存在一些历史 warning（Python 2.7 / Boost / protobuf），与本实现无直接关系。

## 相关验证

当前实现已验证：

- `replay_parquet_test` 覆盖：
  - 基本顺序
  - 多文件归并
  - 双缓冲切文件
  - resume 跨文件过滤
  - payload 回归
  - poison parquet 错误传播
  - progress 日志输出
- 真实目录联调：
  - `archive_journal/build/output`
  - 能较早出现 `[replay] starting`
  - 回放过程中持续看到 `finish/open` 文件切换日志

## 建议的后续排障方式

如果以后 parquet replay 看起来卡住，优先查看：

1. `build/replayer.log` 中是否已经打印 `[parquet] ... files`
2. 是否已经打印 `[replay] starting`
3. 是否持续出现 `[parquet] finish/open ...`
4. 是否有异常直接指出某个 parquet 文件损坏

这样一般可以快速判断问题属于：

- 启动前预热阶段
- 文件预取/切换阶段
- 真实 parquet 文件损坏
