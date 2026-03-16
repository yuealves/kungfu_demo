# KungFu 线上服务运维手册

本文档为 KungFu 行情系统线上环境的运维手册，覆盖环境概况、服务管理、日常调度、监控排障等内容。每节独立可用，可直接跳到需要的章节。

> **信息来源**：线上 Docker 环境实际采集（进程、配置、磁盘、crontab）+ 本地源码 + 已有文档

---

## 目录

1. [环境概况](#1-环境概况)
2. [服务架构](#2-服务架构)
3. [关键路径速查表](#3-关键路径速查表)
4. [每日自动调度](#4-每日自动调度)
5. [服务管理命令速查](#5-服务管理命令速查)
6. [监控和健康检查](#6-监控和健康检查)
7. [数据管理](#7-数据管理)
8. [故障恢复](#8-故障恢复)
9. [配置参考](#9-配置参考)

---

## 1. 环境概况

### 1.1 Docker 容器

| 项目 | 值 |
|------|---|
| 基础镜像 | CentOS Stream 8 |
| 文件系统 | overlay（容器层） + host mount（`/data`、`/opt/insight_test`） |
| 网络模式 | host |
| 运行用户 | `bruce`（supervisord）/ `root`（cron 任务） |

### 1.2 软件版本

| 组件 | 版本 | 路径 |
|------|------|------|
| GCC | 11.2.1（Red Hat） | `/usr/bin/g++` |
| Python | 2.7.18 | `/usr/bin/python2.7` |
| KungFu RPM | v1.0.0（Git commit `ebbc9e21`） | `/opt/kungfu/master/` |
| RPM 包名 | `kungfu-0.0.5-20240912141305.x86_64` | `rpm -qi kungfu` 查看 |
| Boost | 1.62.0 | `/opt/kungfu/toolchain/boost-1.62.0/` |
| CMake | 3.8.0 | `/opt/kungfu/toolchain/cmake-3.8.0/` |
| Arrow/Parquet | 21.0.0 | `/usr/local/lib64/` |

### 1.3 磁盘分区和使用

```
Filesystem      Size  Used Avail Use%  Mounted on
overlay         1.1T  713G  387G  65%  /
```

各目录占用：

| 路径 | 大小 | 说明 |
|------|------|------|
| `/shared/kungfu/journal/` | ~43GB | 当日 journal 文件（每日 23:59 清理） |
| `/shared/kungfu/log/` | ~75MB | 日志文件 |
| `/data/shenrun/dump_parquet/` | ~45GB | Parquet 归档数据（累计） |
| `/opt/insight_test/` | ~1.2GB | insight 行情网关及日志 |

每日增量估算：

| 数据类型 | 文件数/天 | 大小/天 |
|----------|----------|---------|
| tick journal | ~66 个（128MB/个） | ~8.3GB |
| order journal | ~154 个 | ~19.3GB |
| trade journal | ~125 个 | ~15.6GB |
| **合计 journal** | **~345 个** | **~43GB** |

---

## 2. 服务架构

### 2.1 进程关系图

```
                      ┌─────────────────────────────┐
                      │  supervisord (PID 常驻)       │
                      │  -c supervisord.conf          │
                      │  user: bruce                  │
                      └──────────┬──────────────────┘
                                 │ 管理
                                 ▼
                      ┌─────────────────────────────┐
                      │  Paged (yjj server)           │
                      │  PageEngine 内存页管理         │
                      │  CPU: ~99%（100Hz 轮询）       │
                      │  常驻运行，开机自启             │
                      └──────────┬──────────────────┘
                                 │ mmap 协调
                    ┌────────────┼────────────┐
                    │            │            │
                    ▼            ▼            ▼
          ┌─────────────┐ ┌──────────────┐ ┌──────────────┐
          │insight_gateway│ │realtime_parquet│ │archive_journal│
          │  行情写入端   │ │ 实时分钟Parquet│ │ Parquet 归档  │
          │  09:10 cron  │ │ 09:12 cron   │ │ 16:00 cron   │
          │  CPU: ~99%   │ │ 单线程主循环  │ │ 收盘后执行    │
          └──────────────┘ └──────────────┘ └──────────────┘
                    │            │
                    │   写入      │   读取
                    ▼            ▼
          ┌──────────────────────────────┐
          │  journal 文件（mmap 共享内存） │
          │  /shared/kungfu/journal/user/ │
          └──────────────────────────────┘
```

### 2.2 各进程职责

| 进程 | 职责 | 启动方式 |
|------|------|---------|
| **supervisord** | 进程管理器，负责拉起和管理 Paged 服务 | 容器启动时自动运行 |
| **Paged (yjj server)** | KungFu journal 系统核心，管理 mmap 内存映射和页面分配 | supervisord 管理，autostart=true |
| **insight_gateway** | 连接华泰 Insight 行情服务器，接收 L2 行情写入 journal | cron 09:10 启动 |
| **realtime_parquet** | 实时读取 journal，每分钟输出 tick/order/trade Parquet 文件 | cron 09:12 启动，15:51 停止 |
| **monitor_latency** | 监控行情延迟，输出 CSV 供外网检查 | cron 09:15 启动 |
| **archive_journal** | 收盘后将当日 journal 归档为 Parquet | cron 16:00 启动 |
| **data_fetcher** | ~~从 journal 按时间间隔导出 Parquet~~（已停用） | cron 已注释 |

### 2.3 通信方式

| 通信路径 | 机制 |
|----------|------|
| Paged ↔ insight_gateway/data_fetcher | Unix socket (`paged.sock`) + mmap 共享内存 (`PAGE_ENGINE_COMM`) |
| insight_gateway → journal → data_fetcher | mmap 共享内存，通过 `volatile status` 字段轮询发现新数据 |
| insight_gateway ← Insight 服务器 | TCP 长连接 + TLS 加密 + protobuf 序列化 |

数据**不经过 Paged**。Paged 只负责 mmap 映射管理和换页，writer/reader 之间的数据交换完全通过共享内存实现。

---

## 3. 关键路径速查表

### 3.1 KungFu 安装目录 `/opt/kungfu/master/`

| 路径 | 内容 |
|------|------|
| `bin/yjj` | YiJinJing CLI 工具（Python 2.7 脚本） |
| `bin/kungfuctl` | supervisorctl 封装脚本 |
| `bin/journal_tool` | Journal 查看工具 |
| `bin/journal_dumper` | Journal 导出工具 |
| `lib/yijinjing/libpaged.so` | PageEngine C++ 实现 |
| `lib/yijinjing/libjournal.so.1.1` | Journal 读写库 |
| `lib/yijinjing/libkflog.so` | 日志库 |
| `lib/boost/` | Boost 1.62.0 Python 绑定 |
| `etc/supervisor/supervisord.conf` | Supervisor 主配置 |
| `etc/supervisor/conf.d/yijinjing.conf` | Paged 进程配置 |
| `etc/log4cplus/default.properties` | 日志格式配置 |
| `etc/sysconfig/kungfu` | 环境变量定义文件 |
| `etc/systemd/user/kungfu.service` | systemd 服务文件 |

### 3.2 运行时目录 `/shared/kungfu/`

| 路径 | 内容 |
|------|------|
| `journal/PAGE_ENGINE_COMM` | 通信缓冲区（137KB，支持 1000 并发客户端） |
| `journal/TEMP_PAGE` | 临时页面（128MB） |
| `journal/system/yjj.SYSTEM.*.journal` | Paged 系统 journal |
| `journal/user/yjj.insight_stock_*.journal` | 行情数据 journal（每日清理） |
| `socket/paged.sock` | Paged Unix socket |
| `socket/paged_rconsole.sock` | RConsole 远程管理 socket |
| `socket/kungfu-supervisor.sock` | Supervisor 控制 socket |
| `pid/paged.pid` | Paged PID 文件 |
| `pid/kungfu-supervisord.pid` | Supervisord PID 文件 |
| `runtime/` | yjj server 工作目录 |
| `log/page_engine.log` | PageEngine 运行日志（20MB 滚动，保留 5 份） |
| `log/supervisor/supervisord.log` | Supervisor 日志 |
| `log/supervisor/yjj.log` | yjj 进程 stdout/stderr |

### 3.3 行情网关目录 `/opt/insight_test/`

| 路径 | 内容 |
|------|------|
| `config.json` | Insight 连接配置（用户名、密码、服务器地址） |
| `cert/` | TLS 证书目录 |
| `start_insight.sh` | insight_gateway 启动脚本 |
| `start_fetcher.sh` | data_fetcher 启动脚本（3 个实例） |
| `delete_old_data.sh` | 旧数据清理脚本 |
| `insight.log` | insight_gateway 日志（可达 50MB+） |
| `fetcher.log` | data_fetcher 日志 |
| `market_gateway/` | 主项目目录（源码 + 构建） |
| `market_gateway/build/src/brokers/insight_tcp/insight_gateway` | 行情网关二进制 |
| `market_gateway/build/src/brokers/insight_tcp/data_fetcher` | 数据导出二进制 |

### 3.4 数据输出目录 `/data/shenrun/`

| 路径 | 内容 |
|------|------|
| `dump_parquet/` | archive_journal 输出的 Parquet 归档文件（~45GB 累计） |
| `dump_1m_parquet/` | realtime_parquet 输出的分钟级 Parquet 文件 |
| `latency_moniter/` | monitor_latency 输出的延迟监控 CSV |

---

## 4. 每日自动调度

### 4.1 Crontab 时序表

所有定时任务仅工作日执行（周一至周五）：

```
分 时  日 月 周  命令
10 9  * * 1-5  bash /opt/insight_test/start_insight.sh >> /opt/insight_test/insight.log 2>&1
12 9  * * 1-5  nohup /root/lw_dev/realtime_parquet/build/realtime_parquet -o /data/shenrun/dump_1m_parquet >> /data/shenrun/dump_1m_parquet/realtime_parquet.log 2>&1 &
15 9  * * 1-5  nohup /root/lw_dev/monitor_latency/build/monitor_latency -o /data/shenrun/latency_moniter >> /data/shenrun/latency_moniter/monitor.log 2>&1 &
# 20 9 * * 1-5  bash /opt/insight_test/start_fetcher.sh >> /opt/insight_test/fetcher.log 2>&1   ← data_fetcher 已停用
22 14 * * 1-5  bash /opt/insight_test/delete_old_data.sh
50 15 * * 1-5  pkill -f monitor_latency
51 15 * * 1-5  pkill -f realtime_parquet
0  16 * * 1-5  cd /root/lw_dev/archive_journal_demo/build && ./archive_journal -i /shared/kungfu/journal/user -o /data/shenrun/dump_parquet -s 256 >> /var/log/archive_journal.log 2>&1
59 23 * * 1-5  rm -f /shared/kungfu/journal/user/*.journal
```

### 4.2 时序图

```
09:10  ─── start_insight.sh ─────→ 启动 insight_gateway（先 kill 旧进程）
           │                        连接 Insight 服务器，准备接收行情
09:12  ─── realtime_parquet ─────→ 启动实时分钟 Parquet 写入
           │                        读取 journal 全量数据 + 实时跟随
09:15  ─── monitor_latency ──────→ 启动延迟监控，每秒采样写 CSV
           │
09:25  ─── 集合竞价开始 ─────────→ insight_gateway 开始接收并写入 journal
09:30  ─── 连续竞价开始 ─────────→ 全速运行
  :
11:30  ─── 上午收盘 ─────────────→ 午休，行情暂停
13:00  ─── 下午开盘 ─────────────→ 行情恢复
  :
14:22  ─── delete_old_data.sh ───→ 清理 /data/dump 下 >7天 的旧文件
15:00  ─── 收盘 ─────────────────→ 行情结束
15:30  ─── 行情停止 ─────────────→ gateway 停止接收数据
15:50  ─── pkill monitor_latency → 停止延迟监控
15:51  ─── pkill realtime_parquet→ 停止实时 Parquet（SIGTERM → flush + rename）
16:00  ─── archive_journal ──────→ 当日 journal 转 Parquet 归档
           │                        输出到 /data/shenrun/dump_parquet/
23:59  ─── rm *.journal ─────────→ 清理当日所有 journal 文件
```

### 4.3 各任务详细说明

**09:10 — insight_gateway 启动**

`start_insight.sh` 会先 `kill -9` 所有已有的 `insight_gateway` 进程，然后在 `/opt/insight_test/market_gateway/build/src/brokers/insight_tcp/` 目录下启动新实例。提前于 09:25 集合竞价 15 分钟启动，留出连接 Insight 服务器和登录的时间。

**09:12 — realtime_parquet 启动**

从 journal 开头读取全量数据（09:10 gateway 启动后写入的 2 分钟数据），追赶完成后进入实时跟随模式。每分钟输出 tick/order/trade 各一个 Parquet 文件到 `/data/shenrun/dump_1m_parquet/YYYYMMDD/`。写入中的文件使用 dot 前缀（如 `.tick_0930.parquet`），close 后 rename 为正常文件名（`tick_0930.parquet`）。

假期（无数据）时空转等待，15:51 被 pkill 正常退出，不创建任何文件。

**09:15 — 延迟监控启动**

`monitor_latency` 持续读取 tick/order/trade 三个频道的最新 journal 时间戳和交易所时间戳，每秒追加写入 `/data/shenrun/latency_moniter/latency_YYYYMMDD.csv`，供公司 IT 从外网检查行情延迟。

**~~09:20 — data_fetcher 启动~~（已停用）**

crontab 中已注释。原功能由 realtime_parquet 替代。

**14:22 — 旧数据清理**

`delete_old_data.sh` 默认清理 `/data/dump` 下修改时间超过 7 天的文件。

**16:00 — Journal 归档**

`archive_journal` 读取当日 `/shared/kungfu/journal/user/` 下的 journal 文件，按数据类型输出为 Parquet 文件（单文件上限 256MB，超出自动分片），存储到 `/data/shenrun/dump_parquet/`。文件名格式：`20260310_tick_data.parquet`。

**23:59 — Journal 清理**

直接删除 `/shared/kungfu/journal/user/` 下所有 `.journal` 文件。此时当日数据已由 16:00 的 archive_journal 归档为 Parquet，journal 原文件可以安全删除。

### 4.4 日数据量估算

| 数据 | 每日增量 |
|------|---------|
| Journal 文件（tick + order + trade） | ~43GB |
| Parquet 归档（archive_journal 输出） | ~2-5GB（压缩后） |
| data_fetcher Parquet 输出 | 增量累积 |
| 延迟监控 CSV | <1MB |
| insight.log | ~50MB |

---

## 5. 服务管理命令速查

### 5.1 环境变量设置

所有 KungFu 相关命令执行前需要设置环境变量：

```bash
export KUNGFU_HOME=/opt/kungfu
export KUNGFU_MAIN=/opt/kungfu/master/bin/yjj
export PYTHONPATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost
export LD_LIBRARY_PATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost
```

或一次性 source：

```bash
source /opt/kungfu/master/etc/sysconfig/kungfu
```

### 5.2 Paged 服务管理

**方式一：通过 supervisorctl（推荐）**

```bash
# 进入 supervisorctl 交互界面
/opt/kungfu/master/bin/kungfuctl

# 或直接执行命令
supervisorctl -c /opt/kungfu/master/etc/supervisor/supervisord.conf status
supervisorctl -c /opt/kungfu/master/etc/supervisor/supervisord.conf start yijinjing
supervisorctl -c /opt/kungfu/master/etc/supervisor/supervisord.conf stop yijinjing
supervisorctl -c /opt/kungfu/master/etc/supervisor/supervisord.conf restart yijinjing
```

**方式二：通过 yjj CLI**

```bash
# 查看 Paged 状态（输出 JSON：客户端列表、文件锁、任务状态）
python2 -u /opt/kungfu/master/bin/yjj status

# 进入交互式 console（可调用 engine.status() 等）
python2 -u /opt/kungfu/master/bin/yjj interact

# 优雅停止 Paged
python2 -u /opt/kungfu/master/bin/yjj shutdown
```

**方式三：supervisord 启停**

```bash
# 启动 supervisord（会自动拉起 Paged）
/usr/bin/python2 /usr/bin/supervisord -c /opt/kungfu/master/etc/supervisor/supervisord.conf

# 关闭 supervisord（会停止所有子进程）
supervisorctl -c /opt/kungfu/master/etc/supervisor/supervisord.conf shutdown
```

### 5.3 insight_gateway 管理

```bash
# 启动（与 cron 相同的方式）
bash /opt/insight_test/start_insight.sh >> /opt/insight_test/insight.log 2>&1

# 停止
for pid in $(ps -ef | grep insight_gateway | grep -v grep | awk '{print $2}'); do
    kill -9 $pid
done

# 查看状态
ps -ef | grep insight_gateway | grep -v grep

# 查看日志（实时）
tail -f /opt/insight_test/insight.log
```

### 5.4 data_fetcher 管理

```bash
# 启动（与 cron 相同）
bash /opt/insight_test/start_fetcher.sh >> /opt/insight_test/fetcher.log 2>&1

# 停止所有实例
for pid in $(ps -ef | grep data_fetcher | grep -v grep | awk '{print $2}'); do
    kill -9 $pid
done

# 查看状态（应看到 3 个进程）
ps -ef | grep data_fetcher | grep -v grep

# 查看日志
tail -f /opt/insight_test/fetcher.log
```

### 5.5 realtime_parquet 管理

```bash
# 启动
nohup /root/lw_dev/realtime_parquet/build/realtime_parquet \
    -o /data/shenrun/dump_1m_parquet \
    >> /data/shenrun/dump_1m_parquet/realtime_parquet.log 2>&1 &

# 停止（SIGTERM 触发 flush + rename，最后一个文件正常完成）
pkill -f realtime_parquet

# 查看状态
ps -ef | grep realtime_parquet | grep -v grep

# 查看日志
tail -f /data/shenrun/dump_1m_parquet/realtime_parquet.log

# 查看输出文件（注意 dot 前缀的文件是正在写入的）
ls /data/shenrun/dump_1m_parquet/$(date +%Y%m%d)/
```

### 5.6 monitor_latency 管理

```bash
# 启动
nohup /root/lw_dev/monitor_latency/build/monitor_latency \
    -o /data/shenrun/latency_moniter \
    >> /data/shenrun/latency_moniter/monitor.log 2>&1 &

# 停止
pkill -f monitor_latency

# 查看输出
tail -f /data/shenrun/latency_moniter/latency_$(date +%Y%m%d).csv
```

### 5.7 archive_journal 手动执行

```bash
cd /root/lw_dev/archive_journal_demo/build

# 归档当日 journal
./archive_journal \
    -i /shared/kungfu/journal/user \
    -o /data/shenrun/dump_parquet \
    -s 256 \
    >> /var/log/archive_journal.log 2>&1

# 查看归档日志
tail -f /var/log/archive_journal.log

# 查看输出文件
ls -la /data/shenrun/dump_parquet/
```

### 5.8 完整重启流程（按顺序）

当需要完全重启所有服务时，按以下顺序执行：

```bash
# ===== 1. 停止所有行情相关进程 =====
# 停止 realtime_parquet（SIGTERM 触发 flush + rename）
pkill -f realtime_parquet

# 停止 insight_gateway
for pid in $(ps -ef | grep insight_gateway | grep -v grep | awk '{print $2}'); do
    kill -9 $pid
done

# 停止 monitor_latency
pkill -f monitor_latency

# ===== 2. 停止 Paged =====
supervisorctl -c /opt/kungfu/master/etc/supervisor/supervisord.conf stop yijinjing

# ===== 3. 清理残留文件 =====
rm -f /shared/kungfu/pid/paged.pid \
      /shared/kungfu/socket/paged.sock \
      /shared/kungfu/socket/paged_rconsole.sock

# ===== 4. 重新启动 Paged =====
supervisorctl -c /opt/kungfu/master/etc/supervisor/supervisord.conf start yijinjing

# 等待 Paged 就绪（检查 socket 文件）
sleep 3
file /shared/kungfu/socket/paged.sock
# 应输出: paged.sock: socket

# ===== 5. 启动 insight_gateway =====
bash /opt/insight_test/start_insight.sh >> /opt/insight_test/insight.log 2>&1

# ===== 6. 启动 realtime_parquet =====
sleep 3
nohup /root/lw_dev/realtime_parquet/build/realtime_parquet \
    -o /data/shenrun/dump_1m_parquet \
    >> /data/shenrun/dump_1m_parquet/realtime_parquet.log 2>&1 &

# ===== 7. 启动 monitor_latency =====
nohup /root/lw_dev/monitor_latency/build/monitor_latency \
    -o /data/shenrun/latency_moniter \
    >> /data/shenrun/latency_moniter/monitor.log 2>&1 &

# ===== 8. 验证所有进程 =====
ps -ef | grep -E "yjj server|insight_gateway|realtime_parquet|monitor_latency" | grep -v grep
```

---

## 6. 监控和健康检查

### 6.1 确认服务运行状态

```bash
# 一键检查所有关键进程
ps -ef | grep -E "supervisord|yjj server|insight_gateway|realtime_parquet|monitor_latency" | grep -v grep
```

预期输出（开盘时段）：

```
bruce   3229     0  supervisord -c /opt/kungfu/master/etc/supervisor/supervisord.conf
bruce   3230  3229  python -u /opt/kungfu/master/bin/yjj server
bruce   3231  3230  python -u /opt/kungfu/master/bin/yjj server    ← Paged 实际进程
root   24430 24420  ./insight_gateway                               ← 行情写入
root   24500     1  ./realtime_parquet -o /data/shenrun/dump_1m_parquet  ← 实时分钟 Parquet
```

**检查 Paged socket 是否正常：**

```bash
file /shared/kungfu/socket/paged.sock
# 正确: paged.sock: socket
# 异常: paged.sock: empty  或  文件不存在
```

**检查 Paged 详细状态：**

```bash
source /opt/kungfu/master/etc/sysconfig/kungfu
python2 -u /opt/kungfu/master/bin/yjj status
```

输出示例（关注 Client 列表和 Task 状态）：

```python
{
    'Client': {
        'paged': {'pid': 3231, 'is_writing': True},
        'insight_gateway': {'pid': 24430, 'is_writing': True}
    },
    'Task': ('running', 10000, {
        'KfController': {
            'engine_starts': ['2026-03-11 09:15:00'],
            'engine_ends': ['2026-03-11 16:30:00']
        }
    })
}
```

### 6.2 日志查看

| 日志 | 路径 | 查看命令 |
|------|------|---------|
| PageEngine 日志 | `/shared/kungfu/log/page_engine.log` | `tail -f /shared/kungfu/log/page_engine.log` |
| Supervisor/yjj 日志 | `/shared/kungfu/log/supervisor/yjj.log` | `tail -f /shared/kungfu/log/supervisor/yjj.log` |
| Supervisord 日志 | `/shared/kungfu/log/supervisor/supervisord.log` | `tail -100 /shared/kungfu/log/supervisor/supervisord.log` |
| insight_gateway 日志 | `/opt/insight_test/insight.log` | `tail -f /opt/insight_test/insight.log` |
| realtime_parquet 日志 | `/data/shenrun/dump_1m_parquet/realtime_parquet.log` | `tail -f /data/shenrun/dump_1m_parquet/realtime_parquet.log` |
| monitor_latency 日志 | `/data/shenrun/latency_moniter/monitor.log` | `tail -f /data/shenrun/latency_moniter/monitor.log` |
| archive_journal 日志 | `/var/log/archive_journal.log` | `tail -f /var/log/archive_journal.log` |

**PageEngine 日志关键信息：**

```
# 正常注册客户端
[RegClient] (name)insight_gateway (writer?)1
[RegJournal] (client)insight_gateway (idx)0
[InPage] (folder)/shared/kungfu/journal/user/ (jname)insight_stock_tick_data (pNum)1

# 正常换页
[InPage] (folder)/shared/kungfu/journal/user/ (jname)insight_stock_tick_data (pNum)2

# 客户端退出（PID 检测）
[RmClient] (name)data_fetcher pid:24463
```

### 6.3 Journal 文件增长检查

```bash
# 查看当前 journal 文件数量和总大小
du -sh /shared/kungfu/journal/user/
ls /shared/kungfu/journal/user/ | wc -l

# 查看各类型 journal 文件数
ls /shared/kungfu/journal/user/yjj.insight_stock_tick_data.*.journal 2>/dev/null | wc -l
ls /shared/kungfu/journal/user/yjj.insight_stock_order_data.*.journal 2>/dev/null | wc -l
ls /shared/kungfu/journal/user/yjj.insight_stock_trade_data.*.journal 2>/dev/null | wc -l

# 查看最新 journal 文件的修改时间（判断是否在持续写入）
ls -lt /shared/kungfu/journal/user/ | head -5
```

开盘期间，最新文件的修改时间应该在几秒之内。如果最新文件时间停滞超过 1 分钟，说明 insight_gateway 可能已停止写入。

### 6.4 延迟监控

```bash
# 查看最新延迟数据
tail -10 /data/shenrun/latency_moniter/latency_$(date +%Y%m%d).csv
```

CSV 格式：

```csv
write_time,monitor_item,journal_time,exchange_time
2026-03-11 09:30:15.123,tick,2026-03-11 09:30:14.456,09:30:15.230
```

关注 `write_time` 和 `journal_time` 的差值，正常应在秒级以内。

---

## 7. 数据管理

### 7.1 Journal 生命周期

```
写入阶段（09:10 ~ 15:00）
  insight_gateway 持续写入 → journal 文件持续增长（每个 128MB）
      │
归档阶段（16:00）
  archive_journal 读取当日 journal → 输出 Parquet 到 /data/shenrun/dump_parquet/
      │
清理阶段（23:59）
  cron 删除 /shared/kungfu/journal/user/*.journal
```

### 7.2 磁盘空间监控

```bash
# 总体磁盘使用
df -h /

# 各关键目录大小
du -sh /shared/kungfu/journal/
du -sh /shared/kungfu/log/
du -sh /data/shenrun/dump_parquet/
du -sh /opt/insight_test/

# 按时间排序查看最大的 parquet 文件
ls -lhS /data/shenrun/dump_parquet/ | head -20
```

**磁盘预警阈值**：当使用率超过 80%（约 880GB），需要手动清理旧数据。

### 7.3 手动清理方法

```bash
# 1. 清理超过 N 天的 Parquet 归档文件（示例：清理 30 天前）
find /data/shenrun/dump_parquet/ -type f -mtime +30 -exec rm -f {} \;

# 2. 清理超过 7 天的 data_fetcher 导出文件（与 cron 中 delete_old_data.sh 相同）
find /data/dump -type f -mtime +7 -exec rm -f {} \;

# 3. 清理 insight.log（过大时截断）
> /opt/insight_test/insight.log

# 4. 清理 fetcher.log
> /opt/insight_test/fetcher.log

# 5. 紧急清理当日 journal（注意：这会导致 data_fetcher 无法读取当日数据）
rm -f /shared/kungfu/journal/user/*.journal
```

### 7.4 Parquet 归档文件命名规则

```
/data/shenrun/dump_parquet/
├── 20260310_tick_data.parquet
├── 20260310_order_data.parquet
├── 20260310_trade_data.parquet
├── 20260311_tick_data.parquet
├── 20260311_tick_data_001.parquet    ← 超过 256MB 时自动分片
├── 20260311_tick_data_002.parquet
└── ...
```

---

## 8. 故障恢复

### 8.1 Paged 崩溃

**症状**：`ps -ef | grep "yjj server"` 无输出，`file /shared/kungfu/socket/paged.sock` 显示 empty 或文件不存在。

**恢复步骤**：

```bash
# 1. 清理残留文件（必须，否则重启会失败）
rm -f /shared/kungfu/pid/paged.pid \
      /shared/kungfu/socket/paged.sock \
      /shared/kungfu/socket/paged_rconsole.sock

# 2. 重启 Paged
supervisorctl -c /opt/kungfu/master/etc/supervisor/supervisord.conf start yijinjing

# 3. 验证
sleep 3
file /shared/kungfu/socket/paged.sock
# 应输出: socket

# 4. 重启依赖 Paged 的进程（如果当前在交易时段）
bash /opt/insight_test/start_insight.sh >> /opt/insight_test/insight.log 2>&1
sleep 10
bash /opt/insight_test/start_fetcher.sh >> /opt/insight_test/fetcher.log 2>&1
```

> **注意**：Paged 的 supervisor 配置 `autorestart=false`，所以 Paged 崩溃后不会自动重启，需要手动操作。

### 8.2 insight_gateway 异常

**症状**：insight.log 出现连接断开、登录失败等错误，或进程已退出。

**恢复步骤**：

```bash
# 1. 检查进程是否存在
ps -ef | grep insight_gateway | grep -v grep

# 2. 如果进程还在但异常，先 kill
for pid in $(ps -ef | grep insight_gateway | grep -v grep | awk '{print $2}'); do
    kill -9 $pid
done

# 3. 重新启动
bash /opt/insight_test/start_insight.sh >> /opt/insight_test/insight.log 2>&1

# 4. 检查日志确认连接成功
tail -20 /opt/insight_test/insight.log
# 应看到: OnLoginSuccess
```

如果是在非交易时段，可以不处理，次日 09:10 cron 会自动启动。

### 8.3 data_fetcher 异常

```bash
# 1. 检查是否有 3 个实例在运行
ps -ef | grep data_fetcher | grep -v grep

# 2. 全部 kill 重启
for pid in $(ps -ef | grep data_fetcher | grep -v grep | awk '{print $2}'); do
    kill -9 $pid
done
bash /opt/insight_test/start_fetcher.sh >> /opt/insight_test/fetcher.log 2>&1

# 3. 验证
ps -ef | grep data_fetcher | grep -v grep
# 应看到 3 个进程
```

### 8.4 .so 替换和回滚

替换 `libjournal.so`（例如修复 getNanoTime bug）：

```bash
# 1. 备份原文件
cp /opt/kungfu/master/lib/yijinjing/libjournal.so.1.1 \
   /opt/kungfu/master/lib/yijinjing/libjournal.so.1.1.bak

# 2. 替换新文件
cp /path/to/new/libjournal.so.1.1 \
   /opt/kungfu/master/lib/yijinjing/libjournal.so.1.1

# 3. 重启依赖该 .so 的进程
# Paged（通过 supervisord）
supervisorctl -c /opt/kungfu/master/etc/supervisor/supervisord.conf restart yijinjing
sleep 3

# insight_gateway 和 data_fetcher 也链接了这个 .so
bash /opt/insight_test/start_insight.sh >> /opt/insight_test/insight.log 2>&1
sleep 10
bash /opt/insight_test/start_fetcher.sh >> /opt/insight_test/fetcher.log 2>&1

# 4. 验证新 .so 被加载
ldd /opt/insight_test/market_gateway/build/src/brokers/insight_tcp/insight_gateway | grep journal
```

**回滚**：

```bash
cp /opt/kungfu/master/lib/yijinjing/libjournal.so.1.1.bak \
   /opt/kungfu/master/lib/yijinjing/libjournal.so.1.1
# 然后重复步骤 3 重启所有进程
```

### 8.5 完整灾难恢复流程

当容器重启或所有服务异常时的完整恢复：

```bash
# ===== Step 1: 确认环境 =====
cat /etc/centos-release
rpm -qi kungfu | head -5
df -h /

# ===== Step 2: 清理所有残留 =====
# 停止所有行情进程
for proc in insight_gateway realtime_parquet monitor_latency; do
    for pid in $(ps -ef | grep "$proc" | grep -v grep | awk '{print $2}'); do
        kill -9 $pid 2>/dev/null
    done
done

# 停止 supervisord（如果在运行）
supervisorctl -c /opt/kungfu/master/etc/supervisor/supervisord.conf shutdown 2>/dev/null

# 清理 socket 和 PID 文件
rm -f /shared/kungfu/pid/paged.pid \
      /shared/kungfu/pid/kungfu-supervisord.pid \
      /shared/kungfu/socket/paged.sock \
      /shared/kungfu/socket/paged_rconsole.sock \
      /shared/kungfu/socket/kungfu-supervisor.sock

# ===== Step 3: 启动 supervisord + Paged =====
/usr/bin/python2 /usr/bin/supervisord -c /opt/kungfu/master/etc/supervisor/supervisord.conf

# 等待 Paged 就绪
sleep 5
file /shared/kungfu/socket/paged.sock
# 确认输出: socket

# ===== Step 4: 验证 Paged 状态 =====
source /opt/kungfu/master/etc/sysconfig/kungfu
python2 -u /opt/kungfu/master/bin/yjj status

# ===== Step 5: 如果在交易时段，启动行情服务 =====
bash /opt/insight_test/start_insight.sh >> /opt/insight_test/insight.log 2>&1
sleep 3
nohup /root/lw_dev/realtime_parquet/build/realtime_parquet \
    -o /data/shenrun/dump_1m_parquet \
    >> /data/shenrun/dump_1m_parquet/realtime_parquet.log 2>&1 &
nohup /root/lw_dev/monitor_latency/build/monitor_latency \
    -o /data/shenrun/latency_moniter \
    >> /data/shenrun/latency_moniter/monitor.log 2>&1 &

# ===== Step 6: 最终验证 =====
ps -ef | grep -E "supervisord|yjj server|insight_gateway|realtime_parquet|monitor_latency" | grep -v grep
echo "--- Journal 文件检查 ---"
ls -lt /shared/kungfu/journal/user/ | head -5
```

### 8.6 实战案例：2026-03-11 Paged 重启（替换 libjournal.so 修复 getNanoTime bug）

**背景**：`getNanoTime()` 存在 ~750ms 时钟偏移 bug，需要替换 `libjournal.so.1.1` 并重启 Paged 使新 .so 生效。操作在收盘后进行。

**Step 1: 确认环境安全**

```bash
ps -ef | grep -E 'insight_gateway|data_fetcher' | grep -v grep
# 确认无活跃业务进程，只有 Paged 在运行
```

**Step 2: 查看 supervisord 中 Paged 的程序名**

```bash
supervisorctl -c /opt/kungfu/master/etc/supervisor/supervisord.conf status
# md_ctp      STOPPED
# md_xtp      STOPPED
# td_ctp      STOPPED
# td_xtp      STOPPED
# yijinjing   RUNNING   pid 3230, uptime 40 days, 0:50:17
```

Paged 在 supervisord 里的名字是 **`yijinjing`**。

**Step 3: 备份并替换 .so**

```bash
cp /opt/kungfu/master/lib/yijinjing/libjournal.so.1.1{,.bak}
cp /root/lw_dev/libjournal.so.1.1 /opt/kungfu/master/lib/yijinjing/libjournal.so.1.1
```

**Step 4: 重启 Paged — 第一次失败**

```bash
supervisorctl -c /opt/kungfu/master/etc/supervisor/supervisord.conf restart yijinjing
# yijinjing: stopped
# yijinjing: ERROR (abnormal termination)
```

**Step 5: 排查原因**

```bash
tail /shared/kungfu/log/page_engine.log
# PageEngine Caught signal: 15  (SIGTERM — 正常停止信号)
# PageEngine Caught signal: 11  (SIGSEGV — 段错误！)
# PageEngine Caught signal: 6   (SIGABRT)
```

Paged 在关闭过程中崩溃（SIGSEGV），残留的 pid/socket 文件阻止了新实例启动。

**Step 6: 清理残留文件，重新启动 — 成功**

```bash
rm -f /shared/kungfu/pid/paged.pid \
      /shared/kungfu/socket/paged.sock \
      /shared/kungfu/socket/paged_rconsole.sock

supervisorctl -c /opt/kungfu/master/etc/supervisor/supervisord.conf start yijinjing
# yijinjing: started
```

**Step 7: 清理孤儿进程**

`restart` 只 kill 了父进程（PID 3230），旧 worker（PID 3231, 99% CPU）变成孤儿进程，SIGTERM 无效，需要 `kill -9`：

```bash
kill -9 3231 3128 3129 3130
```

> 注意：PID 3229（supervisord 本身）必须保留。

**Step 8: 验证**

```bash
supervisorctl -c /opt/kungfu/master/etc/supervisor/supervisord.conf status yijinjing
# yijinjing  RUNNING  pid 34338, uptime 0:00:20

LD_LIBRARY_PATH=/root/lw_dev:/opt/kungfu/toolchain/boost-1.62.0/lib ./verify_nanotime
# ALL PASS, max diff = -770 ns
```

**次日确认（3/12 开盘）**：所有服务通过 cron 正常拉起，latency CSV 确认 journal_time 与 exchange_time 偏差已在正常范围。

**教训总结**：

1. `supervisorctl restart` 不可靠 — Paged 关闭时可能 SIGSEGV，留下残留文件导致新实例启动失败
2. 推荐操作顺序：**stop → 清理残留 → kill 孤儿进程 → start**，不要用 `restart`
3. 旧 worker 进程可能忽略 SIGTERM，必须 `kill -9`

**推荐的 Paged 重启步骤（精简版）**：

```bash
# 1. 停止
supervisorctl -c /opt/kungfu/master/etc/supervisor/supervisord.conf stop yijinjing

# 2. 清理残留
rm -f /shared/kungfu/pid/paged.pid \
      /shared/kungfu/socket/paged.sock \
      /shared/kungfu/socket/paged_rconsole.sock

# 3. 杀孤儿 worker
ps -ef | grep 'yjj server' | grep -v grep
# 如有残留: kill -9 <pid>

# 4. 启动
supervisorctl -c /opt/kungfu/master/etc/supervisor/supervisord.conf start yijinjing

# 5. 验证
supervisorctl -c /opt/kungfu/master/etc/supervisor/supervisord.conf status yijinjing
file /shared/kungfu/socket/paged.sock   # 应输出: socket
```

---

## 9. 配置参考

### 9.1 supervisord.conf

路径：`/opt/kungfu/master/etc/supervisor/supervisord.conf`

```ini
[unix_http_server]
file = /shared/kungfu/socket/kungfu-supervisor.sock
chmod = 0777

[supervisord]
logfile = /shared/kungfu/log/supervisor/supervisord.log
logfile_maxbytes = 50MB
logfile_backups=10
loglevel = info
pidfile = /shared/kungfu/pid/kungfu-supervisord.pid
nodaemon = false
minfds = 1024
minprocs = 200
umask = 022
user = bruce
identifier = supervisor
directory = /shared/kungfu/log/supervisor
nocleanup = true
childlogdir = /shared/kungfu/log/supervisor
strip_ansi = false

[supervisorctl]
serverurl = unix:///shared/kungfu/socket/kungfu-supervisor.sock
prompt = kungfu

[include]
files = /opt/kungfu/master/etc/supervisor/conf.d/*.conf

[rpcinterface:supervisor]
supervisor.rpcinterface_factory = supervisor.rpcinterface:make_main_rpcinterface
```

### 9.2 yijinjing.conf（Paged 进程配置）

路径：`/opt/kungfu/master/etc/supervisor/conf.d/yijinjing.conf`

```ini
[program:yijinjing]
command= /opt/kungfu/master/bin/yjj server
process_name=%(program_name)s
numprocs=1
directory=/shared/kungfu/runtime
umask=022
priority=999
autostart=true
autorestart=false
startsecs=10
startretries=0
exitcodes=0
stopsignal=TERM
stopwaitsecs=10
user=bruce
stdout_logfile=/shared/kungfu/log/supervisor/yjj.log
stdout_logfile_maxbytes=10MB
stdout_logfile_backups=10
stdout_capture_maxbytes=1MB
stdout_events_enabled=false
redirect_stderr=true
stderr_logfile=/shared/kungfu/log/supervisor/yjj.log
serverurl=AUTO
```

> **注意**：`autorestart=false` 意味着 Paged 崩溃后不会自动重启。其他 conf.d 下的配置（md_ctp、md_xtp、td_ctp、td_xtp）均为 `autostart=false`，未启用。

### 9.3 config.json 字段说明

路径：`/opt/insight_test/config.json`（insight_gateway 使用）

```json
{
  "paths": {
    "journal_path": "/shared/kungfu/journal/user/",
    "dump_path": "/data/dump_5m/",
    "symbol_snapshot_path": "/data/dump_5m/symbol_snapshot/"
  },
  "channels": {
    "tick_channel": "insight_stock_tick_data",
    "order_channel": "insight_stock_order_data",
    "trade_channel": "insight_stock_trade_data",
    "l2_tick_channel": "l2_tick_data",
    "sh_order_channel": "sh_order_data",
    "sz_order_channel": "sz_order_data",
    "sh_trade_channel": "sh_trade_data",
    "sz_trade_channel": "sz_trade_data"
  },
  "time_settings": {
    "default_interval_minutes": 5,
    "default_start_time": "09:26",
    "default_end_time": "15:01",
    "trading_times": {
      "morning_start": "09:26",
      "morning_end": "11:31",
      "afternoon_start": "13:00",
      "afternoon_end": "15:01"
    }
  },
  "performance": {
    "file_format": "parquet",
    "thread_pool_size": 5
  },
  "insight_config": {
    "user": "<INSIGHT_USER>",
    "password": "<INSIGHT_PASSWORD>",
    "ip": "<INSIGHT_SERVER_IP>",
    "port": 9362,
    "cert_folder": "./cert",
    "export_folder": "./export_folder"
  }
}
```

| 字段 | 说明 |
|------|------|
| `paths.journal_path` | insight_gateway 写入 journal 的目录 |
| `paths.dump_path` | data_fetcher 导出 Parquet 的目录 |
| `channels.*` | 各数据类型的 journal 频道名，对应 `yjj.<channel>.*.journal` 文件名 |
| `time_settings` | data_fetcher 的导出时间窗口和间隔配置 |
| `performance.file_format` | 导出格式：`parquet` 或 `csv` |
| `insight_config` | Insight 服务器连接信息（**已脱敏**） |

### 9.4 start_insight.sh

路径：`/opt/insight_test/start_insight.sh`

```bash
cd /opt/insight_test/market_gateway/build/src/brokers/insight_tcp

export LD_LIBRARY_PATH=/opt/insight_test/market_gateway/build/src/brokers/insight_tcp:\
/opt/insight_test/market_gateway/src/brokers/insight_tcp/lib:\
/opt/kungfu/toolchain/boost-1.62.0/lib:\
/opt/kungfu/master/lib/yijinjing:$LD_LIBRARY_PATH

# 先 kill 旧进程
echo "existing insight_gateway job"
echo `ps -ef | grep insight_gateway | grep -v grep`
for pid in $(ps -ef | grep insight_gateway | grep -v grep | awk '{print $2}')
    do kill -9 $pid;
done

echo "starting insight_gateway job"
./insight_gateway
```

### 9.5 start_fetcher.sh

路径：`/opt/insight_test/start_fetcher.sh`

```bash
cd /opt/insight_test/market_gateway/build/src/brokers/insight_tcp

export LD_LIBRARY_PATH=/opt/insight_test/market_gateway/build/src/brokers/insight_tcp:\
/opt/insight_test/market_gateway/src/brokers/insight_tcp/lib:\
/opt/kungfu/toolchain/boost-1.62.0/lib:\
/opt/kungfu/master/lib/yijinjing:$LD_LIBRARY_PATH

# 先 kill 旧进程
for pid in $(ps -ef | grep data_fetcher | grep -v grep | awk '{print $2}')
    do kill -9 $pid;
done

# 启动三个实例，后台运行
./data_fetcher --tick-only --interval 1 &
./data_fetcher --order-only --interval 1 &
./data_fetcher --trade-only --interval 1 &
```

### 9.6 delete_old_data.sh

路径：`/opt/insight_test/delete_old_data.sh`

```bash
#!/bin/bash
default_folder="/data/dump"
default_days=7
target_folder=${1:-$default_folder}
days=${2:-$default_days}

if [ ! -d "$target_folder" ]; then
    echo "指定的文件夹 $target_folder 不存在。"
    exit 1
fi

find "$target_folder" -type f -mtime +$days -exec rm -f {} \;
echo "已完成删除操作，修改时间超过 $days 天的文件已被删除。"
```

### 9.7 环境变量

路径：`/opt/kungfu/master/etc/sysconfig/kungfu`

```bash
KUNGFU_HOME=/opt/kungfu
YJJ_HOME=/opt/kungfu/master
LONGFIST_HOME=/opt/kungfu/master
WINGCHUN_HOME=/opt/kungfu/master
JAVA_HOME=/usr/lib/jvm/java-1.8.0
BOOST_LIB=/opt/kungfu/master/lib/boost
YJJ_LIB=/opt/kungfu/master/lib/yijinjing
API_LIB=/opt/kungfu/master/lib64/api
PYTHONPATH=/opt/kungfu/master/lib/boost:/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib64/api:/opt/kungfu/master/lib/wingchun
LD_LIBRARY_PATH=/opt/kungfu/master/lib/boost:/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib64/api:/opt/kungfu/master/lib/wingchun
```

insight_gateway / data_fetcher 额外需要：

```bash
export LD_LIBRARY_PATH=/opt/insight_test/market_gateway/build/src/brokers/insight_tcp:\
/opt/insight_test/market_gateway/src/brokers/insight_tcp/lib:\
/opt/kungfu/toolchain/boost-1.62.0/lib:\
/opt/kungfu/master/lib/yijinjing:$LD_LIBRARY_PATH
```

### 9.8 systemd 服务文件

路径：`/opt/kungfu/master/etc/systemd/user/kungfu.service`

```ini
[Unit]
Description=Kungfu Master Trading System Daemon
After=syslog.target

[Service]
Type=forking
EnvironmentFile=-/opt/kungfu/master/etc/sysconfig/kungfu
ExecStart=/usr/bin/supervisord -c /opt/kungfu/master/etc/supervisor/supervisord.conf
ExecReload=/usr/bin/supervisorctl -c /opt/kungfu/master/etc/supervisor/supervisord.conf reload
ExecStop=/usr/bin/supervisorctl -c /opt/kungfu/master/etc/supervisor/supervisord.conf shutdown
LimitDATA=infinity
LimitSTACK=infinity
LimitMEMLOCK=infinity

[Install]
WantedBy=multi-user.target
```

> 当前线上未使用 systemd 管理（Docker 容器内直接运行 supervisord），此文件仅作参考。

### 9.9 log4cplus 日志配置

路径：`/opt/kungfu/master/etc/log4cplus/default.properties`

```properties
log4cplus.rootLogger=INFO, STDOUT

log4cplus.appender.STDOUT=log4cplus::ConsoleAppender
log4cplus.appender.STDOUT.layout=log4cplus::PatternLayout
log4cplus.appender.STDOUT.layout.ConversionPattern=%D{%y-%m-%d %H:%M:%S} %-5p %c{2} %%%x%% - %m %n

log4cplus.appender.PE=log4cplus::RollingFileAppender
log4cplus.appender.PE.File=/shared/kungfu/log/page_engine.log
log4cplus.appender.PE.MaxFileSize=20MB
log4cplus.appender.PE.MaxBackupIndex=5
log4cplus.appender.PE.layout=log4cplus::PatternLayout
log4cplus.appender.PE.layout.ConversionPattern=%D{%y-%m-%d %H:%M:%S} %-5p %c{2} %%%x%% - %m %n

log4cplus.logger.PageEngine=INFO, PE
```

PageEngine 日志滚动策略：单文件 20MB，保留 5 份备份，总共最多 ~120MB。

### 9.10 yjj CLI 命令大全

```bash
# 前置：设置环境变量
source /opt/kungfu/master/etc/sysconfig/kungfu

# 启动 PageEngine（通常由 supervisord 调用，不直接手动执行）
python2 -u /opt/kungfu/master/bin/yjj server

# 查看 Paged 运行状态（客户端列表、文件锁、任务）
python2 -u /opt/kungfu/master/bin/yjj status

# 进入交互式 Python console
# 可执行: engine.status(), engine.getClient() 等
python2 -u /opt/kungfu/master/bin/yjj interact

# 优雅停止 Paged
python2 -u /opt/kungfu/master/bin/yjj shutdown

# 查看 journal 内容
python2 -u /opt/kungfu/master/bin/yjj journal

# 导出 journal 数据
python2 -u /opt/kungfu/master/bin/yjj dump

# 启动带 KungFu 环境的 Python 交互式 shell
python2 -u /opt/kungfu/master/bin/yjj shell
```

### 9.10 kungfuctl

路径：`/opt/kungfu/master/bin/kungfuctl`

```bash
#!/usr/bin/env bash
/usr/bin/supervisorctl -c /opt/kungfu/master/etc/supervisor/supervisord.conf
```

直接执行 `kungfuctl` 进入 supervisorctl 交互界面，等价于带 `-c` 参数的 `supervisorctl`。
