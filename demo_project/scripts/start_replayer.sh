#!/bin/bash
# 启动 journal_replayer
# 用法: ./scripts/start_replayer.sh [speed]
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$PROJECT_DIR/build"
SPEED="${1:-10}"

if [ ! -S /shared/kungfu/socket/paged.sock ]; then
    echo "[replayer] ERROR: Paged not running, run ./scripts/start_paged.sh first"
    exit 1
fi

if [ ! -f "$BUILD_DIR/journal_replayer" ]; then
    echo "[replayer] building..."
    mkdir -p "$BUILD_DIR" && cd "$BUILD_DIR" && cmake .. && make journal_replayer
fi

echo "[replayer] starting at ${SPEED}x..."
export LD_LIBRARY_PATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost:$PROJECT_DIR/lib:$BUILD_DIR
cd "$BUILD_DIR"
exec ./journal_replayer "$SPEED"
