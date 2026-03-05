#!/bin/bash
# 停止 Paged 服务

PAGED_PID_FILE="/shared/kungfu/pid/paged.pid"

if [ -f "$PAGED_PID_FILE" ]; then
    PID=$(cat "$PAGED_PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo "[paged] stopping pid=$PID..."
        kill "$PID"
        sleep 1
        if kill -0 "$PID" 2>/dev/null; then
            echo "[paged] force killing..."
            kill -9 "$PID"
        fi
    fi
fi

# 清理残留
rm -f "$PAGED_PID_FILE" \
      /shared/kungfu/socket/paged.sock \
      /shared/kungfu/socket/paged_rconsole.sock

echo "[paged] stopped"
