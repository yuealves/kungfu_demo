# Paged 快速上手：启动服务 + 运行 orig_insight_demo

以下步骤已在本地 Docker 环境验证通过。全程约 2 分钟。

## 前置条件

- Docker 容器基于实盘机器镜像（CentOS Stream 8，KungFu v1.0.0 已安装）
- `deps/data/` 下有 journal 文件（或自行替换为其他历史数据）

## 步骤

### 1. 放置 journal 数据

orig_insight_demo 硬编码读取 `/shared/kungfu/journal/user/` 目录，把数据拷过去：

```bash
cp /workspace/valerie/streaming_factor/kungfu_demo/deps/data/yjj.*.journal \
   /shared/kungfu/journal/user/
```

验证：
```bash
ls /shared/kungfu/journal/user/
# 应看到:
# yjj.insight_stock_tick_data.1.journal
# yjj.insight_stock_order_data.1.journal
# yjj.insight_stock_trade_data.1.journal
```

> 如果有多天数据，替换这里的文件即可。文件命名格式必须是 `yjj.<channel>.<pageNum>.journal`。

### 2. 清理残留文件

```bash
rm -f /shared/kungfu/pid/paged.pid \
      /shared/kungfu/socket/paged.sock \
      /shared/kungfu/socket/paged_rconsole.sock
```

如果不清理，Paged 可能因为 PID 文件冲突而启动失败。

### 3. 启动 Paged

```bash
cd /shared/kungfu/runtime && \
KUNGFU_MAIN=/opt/kungfu/master/bin/yjj \
PYTHONPATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost \
LD_LIBRARY_PATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost \
nohup python2 -u /opt/kungfu/master/bin/yjj server > /shared/kungfu/log/paged_stdout.log 2>&1 &
```

验证启动成功：
```bash
# 必须显示 "socket"，不能是 "empty" 或文件不存在
file /shared/kungfu/socket/paged.sock
# 输出: /shared/kungfu/socket/paged.sock: socket
```

### 4. 运行 orig_insight_demo

```bash
cd /workspace/valerie/streaming_factor/kungfu_demo/orig_insight_demo/build && \
LD_LIBRARY_PATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost:../lib \
./data_consumer
```

正常输出（会先出现一些系统消息，然后是实际数据）：
```
start!
can't recognize type: 20      ← 系统消息（MSG_TYPE_PAGED_START），正常，忽略即可
can't recognize type: 33      ← 其他系统消息，正常
...
get 100 tick data              ← 开始读到实际行情数据
L2StockTickDataField { nTime: 91003000, hostTime: 91004368, ... code: 600460 }
get 100 order data
get 100 trade data
```

`Ctrl+C` 停止程序。

### 5. 停止 Paged

```bash
KUNGFU_MAIN=/opt/kungfu/master/bin/yjj \
PYTHONPATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost \
LD_LIBRARY_PATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost \
python2 -u /opt/kungfu/master/bin/yjj shutdown
```

或直接 `kill` 进程：
```bash
kill $(cat /shared/kungfu/pid/paged.pid)
```

## 常见问题

**Q: "can't recognize type: 20/30/31/32/33/34" 是什么？**

正常现象。`createReaderWithSys` 会自动订阅 Paged 的系统 journal，这些是 Paged 写入的系统消息。orig_insight_demo 只处理 type 61/62/63（tick/order/trade），其余打印 "can't recognize"。跳过即可，不影响数据读取。

**Q: 程序启动后很久才出现实际数据？**

因为 reader 按时间戳从小到大合并读取所有 journal。系统 journal 中可能积累了大量历史消息（时间戳更早），需要先读完这些才轮到数据 journal。

**Q: Paged 启动报 "Already running" 或 PID 冲突？**

执行步骤 2 清理残留文件后重试。

**Q: 想读其他目录的数据？**

修改 `orig_insight_demo/data_consumer.h` 第 74 行的 `path` 变量：
```cpp
std::string path = "/your/custom/path/";
```
然后重新编译：`cd build && cmake .. && make`

## 完整流程速查（复制粘贴用）

```bash
# 一键执行：清理 → 拷数据 → 启动 Paged → 等待就绪 → 运行 demo
rm -f /shared/kungfu/pid/paged.pid /shared/kungfu/socket/paged.sock /shared/kungfu/socket/paged_rconsole.sock && \
cp /workspace/valerie/streaming_factor/kungfu_demo/deps/data/yjj.*.journal /shared/kungfu/journal/user/ && \
cd /shared/kungfu/runtime && \
KUNGFU_MAIN=/opt/kungfu/master/bin/yjj \
PYTHONPATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost \
LD_LIBRARY_PATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost \
nohup python2 -u /opt/kungfu/master/bin/yjj server > /shared/kungfu/log/paged_stdout.log 2>&1 & \
sleep 2 && \
file /shared/kungfu/socket/paged.sock && \
cd /workspace/valerie/streaming_factor/kungfu_demo/orig_insight_demo/build && \
LD_LIBRARY_PATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost:../lib \
./data_consumer
```
