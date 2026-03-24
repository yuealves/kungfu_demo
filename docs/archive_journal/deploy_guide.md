# archive_journal_demo 部署指南

## 打包

在开发机上执行：

```bash
cd /workspace/valerie/streaming_factor/kungfu_demo
tar czf archive_journal_demo.tar.gz --exclude='build' archive_journal_demo/
```

产物 `archive_journal_demo.tar.gz` 约 26MB（主要是 `third_party/` 中的 Arrow 源码和依赖）。

## 上传到线上

```bash
scp archive_journal_demo.tar.gz <user>@<host>:/path/to/deploy/
```

## 线上部署

### 1. 解压

```bash
cd /path/to/deploy
tar xzf archive_journal_demo.tar.gz
cd archive_journal_demo
```

### 2. 安装 Arrow + Parquet（首次部署需要，后续跳过）

项目依赖 Apache Arrow 21.0.0，`third_party/` 中已包含完整的离线源码和依赖包，无需联网：

```bash
bash setup_parquet.sh
```

该脚本会：
- 安装 `libatomic`（如缺失）
- 从 `third_party/apache-arrow-21.0.0.tar.gz` 离线编译 Arrow + Parquet
- 安装到 `/usr/local/`（需要 root 权限）
- 编译耗时约 5-10 分钟

安装完成后可验证：

```bash
ls /usr/local/lib64/libparquet.so
```

### 3. 编译

```bash
mkdir -p build && cd build
cmake .. && make -j$(nproc)
```

产物：`build/archive_journal` 可执行文件。

## 使用

### 命令行参数

```
./archive_journal -i <journal_dir> [-o <output_dir>] [-s <target_mb>] [-t <threads>]
```

| 参数 | 必选 | 说明 | 默认值 |
|------|------|------|--------|
| `-i <dir>` | 是 | journal 文件目录 | - |
| `-o <dir>` | 否 | parquet 输出目录 | `./output/` |
| `-s <mb>` | 否 | 单个 parquet 文件目标大小（MB），0=不分片 | 128 |
| `-t <n>` | 否 | 并行线程数 | 16 |

### 示例

```bash
# 归档当天 journal 数据
./archive_journal -i /shared/kungfu/journal/user/ -o /data/parquet/

# 指定 4 线程，单文件 256MB
./archive_journal -i /shared/kungfu/journal/user/ -o /data/parquet/ -t 4 -s 256
```

输出文件自动以东八区当日日期命名：

```
/data/parquet/20260310_tick_data.parquet
/data/parquet/20260310_order_data.parquet
/data/parquet/20260310_trade_data.parquet
```

数据量超出单文件上限时自动分片：`20260310_tick_data_001.parquet`、`20260310_tick_data_002.parquet`...

### 每日定时归档（crontab）

收盘后自动执行，例如每天 16:00：

```bash
0 16 * * 1-5 cd /path/to/archive_journal_demo/build && ./archive_journal -i /shared/kungfu/journal/user/ -o /data/parquet/ >> /var/log/archive_journal.log 2>&1
```

## 更新代码后重新部署

本地修改代码后，重新打包上传即可：

```bash
# 本地打包
cd /workspace/valerie/streaming_factor/kungfu_demo
tar czf archive_journal_demo.tar.gz --exclude='build' archive_journal_demo/

# 上传
scp archive_journal_demo.tar.gz <user>@<host>:/path/to/deploy/

# 线上解压 + 重新编译
cd /path/to/deploy
tar xzf archive_journal_demo.tar.gz
cd archive_journal_demo/build
cmake .. && make -j$(nproc)
```

不需要重新执行 `setup_parquet.sh`（Arrow 已安装到系统目录）。
