#!/bin/bash
# 一键启动: 编译 → Paged → example_factor → Replayer
# 用法: ./scripts/run_all.sh [speed]
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$PROJECT_DIR/build"
SPEED="${1:-10}"

cleanup() {
    echo ""
    echo "[run_all] shutting down..."
    [ -n "$FACTOR_PID" ] && kill "$FACTOR_PID" 2>/dev/null
    bash "$SCRIPT_DIR/stop_paged.sh"
}
trap cleanup EXIT

# 编译
echo "[run_all] building..."
mkdir -p "$BUILD_DIR" && cd "$BUILD_DIR"
cmake .. > /dev/null 2>&1 && make -j$(nproc) 2>&1 | tail -5
echo ""

# 启动 Paged
bash "$SCRIPT_DIR/stop_paged.sh" 2>/dev/null || true
bash "$SCRIPT_DIR/start_paged.sh"
echo ""

# 启动 example_factor（后台）
echo "[run_all] starting example_factor (log: build/factor.log)..."
export LD_LIBRARY_PATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost:$PROJECT_DIR/lib:$BUILD_DIR
cd "$BUILD_DIR"
./example_factor > factor.log 2>&1 &
FACTOR_PID=$!
sleep 1
echo ""

# 启动 Replayer（前台）
echo "[run_all] starting replayer at ${SPEED}x, Ctrl+C to stop"
echo "========================================"
./journal_replayer "$SPEED"

# Replayer 结束
echo ""
echo "========================================"
echo "[run_all] factor output (last 20 lines):"
echo "========================================"
sleep 2
kill "$FACTOR_PID" 2>/dev/null
FACTOR_PID=""
wait 2>/dev/null
tail -20 "$BUILD_DIR/factor.log"
