#!/bin/bash
# 启动 Paged 服务
set -e

PAGED_SOCK="/shared/kungfu/socket/paged.sock"
PAGED_PID_FILE="/shared/kungfu/pid/paged.pid"

# 检查是否真正在运行（socket 存在 + 进程存活）
if [ -S "$PAGED_SOCK" ] && [ -f "$PAGED_PID_FILE" ] && kill -0 "$(cat "$PAGED_PID_FILE")" 2>/dev/null; then
    echo "[paged] already running (pid=$(cat "$PAGED_PID_FILE"))"
    exit 0
fi

echo "[paged] cleaning up..."
rm -f /shared/kungfu/pid/paged.pid "$PAGED_SOCK" /shared/kungfu/socket/paged_rconsole.sock
rm -f /shared/kungfu/journal/user/yjj.insight_stock_*

echo "[paged] starting..."
cd /shared/kungfu/runtime
export KUNGFU_MAIN=/opt/kungfu/master/bin/yjj
export LD_LIBRARY_PATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost
export PYTHONPATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost
nohup python2 -u /opt/kungfu/master/bin/yjj server > /shared/kungfu/log/paged_stdout.log 2>&1 &

for i in $(seq 1 10); do
    [ -S "$PAGED_SOCK" ] && echo "[paged] started" && exit 0
    sleep 0.5
done
echo "[paged] ERROR: failed to start, check /shared/kungfu/log/paged_stdout.log"
exit 1
