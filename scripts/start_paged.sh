#!/bin/bash
# 启动 Paged (PageEngine) 服务
# Paged 是 KungFu journal 系统的核心服务，负责管理 journal 文件的 mmap 分配

set -e

PAGED_PID_FILE="/shared/kungfu/pid/paged.pid"
PAGED_SOCK="/shared/kungfu/socket/paged.sock"

# 检查是否已在运行
if [ -S "$PAGED_SOCK" ]; then
    echo "[paged] already running (socket exists)"
    echo "[paged] use scripts/stop_paged.sh to stop first"
    exit 0
fi

# 清理残留文件
echo "[paged] cleaning up residual files..."
rm -f "$PAGED_PID_FILE" \
      "$PAGED_SOCK" \
      /shared/kungfu/socket/paged_rconsole.sock

# 清理 user journal 目录中的旧 insight 数据（replayer 会重新写入）
echo "[paged] cleaning old insight journals..."
rm -f /shared/kungfu/journal/user/yjj.insight_stock_*

# 启动 Paged
echo "[paged] starting..."
cd /shared/kungfu/runtime

export KUNGFU_MAIN=/opt/kungfu/master/bin/yjj
export LD_LIBRARY_PATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost
export PYTHONPATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost

nohup python2 -u /opt/kungfu/master/bin/yjj server \
    > /shared/kungfu/log/paged_stdout.log 2>&1 &

# 等待 socket 就绪
for i in $(seq 1 10); do
    if [ -S "$PAGED_SOCK" ]; then
        echo "[paged] started (pid=$(cat $PAGED_PID_FILE 2>/dev/null || echo '?'))"
        exit 0
    fi
    sleep 0.5
done

echo "[paged] ERROR: failed to start (socket not found after 5s)"
echo "[paged] check /shared/kungfu/log/paged_stdout.log"
exit 1
