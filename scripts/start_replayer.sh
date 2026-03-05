#!/bin/bash
# 启动 journal_replayer —— 从本地历史数据按时间间隔写入 Paged 管理的 journal
#
# 用法: ./scripts/start_replayer.sh [speed]
#   speed: 回放倍速，默认 1（实时），10 表示 10 倍速
#
# 前台运行，Ctrl+C 停止

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$PROJECT_DIR/build"
BINARY="$BUILD_DIR/journal_replayer"

SPEED="${1:-1}"

# 检查 Paged
if [ ! -S /shared/kungfu/socket/paged.sock ]; then
    echo "[replayer] ERROR: Paged not running, start it first:"
    echo "  ./scripts/start_paged.sh"
    exit 1
fi

# 检查构建
if [ ! -f "$BINARY" ]; then
    echo "[replayer] binary not found, building..."
    cd "$BUILD_DIR" && cmake .. && make journal_replayer
fi

echo "[replayer] starting at ${SPEED}x speed..."

export LD_LIBRARY_PATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost:$PROJECT_DIR/lib
cd "$BUILD_DIR"
exec ./journal_replayer "$SPEED"
