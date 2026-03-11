#!/bin/bash
# 启动 monitor_latency —— 监控行情延迟
#
# 用法: ./scripts/start_monitor.sh -o <output_dir> [-i <seconds>] [-d <journal_dir>]
#
# 前台运行，Ctrl+C 停止

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$PROJECT_DIR/build"
BINARY="$BUILD_DIR/monitor_latency"

# 检查 Paged
if [ ! -S /shared/kungfu/socket/paged.sock ]; then
    echo "[monitor] ERROR: Paged not running, start it first:"
    echo "  kungfu_demo/scripts/start_paged.sh"
    exit 1
fi

# 检查构建
if [ ! -f "$BINARY" ]; then
    echo "[monitor] binary not found, building..."
    mkdir -p "$BUILD_DIR"
    cd "$BUILD_DIR" && cmake .. && make
fi

export LD_LIBRARY_PATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost:$LD_LIBRARY_PATH

cd "$BUILD_DIR"
exec ./monitor_latency "$@"
