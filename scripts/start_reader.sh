#!/bin/bash
# 启动 live_reader —— 通过 Paged 实时读取 journal 数据
#
# 用法: ./scripts/start_reader.sh [print_interval]
#   print_interval: 每隔多少条打印详细数据，默认 10000
#
# 前台运行，Ctrl+C 停止

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$PROJECT_DIR/build"
BINARY="$BUILD_DIR/live_reader"

INTERVAL="${1:-10000}"

# 检查 Paged
if [ ! -S /shared/kungfu/socket/paged.sock ]; then
    echo "[reader] ERROR: Paged not running, start it first:"
    echo "  ./scripts/start_paged.sh"
    exit 1
fi

# 检查构建
if [ ! -f "$BINARY" ]; then
    echo "[reader] binary not found, building..."
    cd "$BUILD_DIR" && cmake .. && make live_reader
fi

echo "[reader] starting (print every $INTERVAL records)..."

export LD_LIBRARY_PATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost:$PROJECT_DIR/lib
cd "$BUILD_DIR"
exec ./live_reader "$INTERVAL"
