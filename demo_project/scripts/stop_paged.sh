#!/bin/bash
# 停止 Paged 服务
PAGED_PID_FILE="/shared/kungfu/pid/paged.pid"
if [ -f "$PAGED_PID_FILE" ]; then
    PID=$(cat "$PAGED_PID_FILE")
    kill -0 "$PID" 2>/dev/null && kill "$PID" && sleep 1
    kill -0 "$PID" 2>/dev/null && kill -9 "$PID"
fi
rm -f "$PAGED_PID_FILE" /shared/kungfu/socket/paged.sock /shared/kungfu/socket/paged_rconsole.sock
echo "[paged] stopped"
