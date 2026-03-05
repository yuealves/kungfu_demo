#!/bin/bash
# 一键启动完整回放环境：Paged → Reader → Replayer
#
# 用法: ./scripts/run_all.sh [speed] [print_interval]
#   speed:          回放倍速，默认 10
#   print_interval: reader 打印间隔，默认 10000
#
# 启动后:
#   - Paged 在后台
#   - Reader 在后台（输出到 build/reader.log）
#   - Replayer 在前台（Ctrl+C 停止）
#   - Replayer 结束后自动停止 Reader 和 Paged

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$PROJECT_DIR/build"

SPEED="${1:-10}"
INTERVAL="${2:-10000}"

cleanup() {
    echo ""
    echo "[run_all] shutting down..."
    [ -n "$READER_PID" ] && kill "$READER_PID" 2>/dev/null && echo "[run_all] reader stopped"
    bash "$SCRIPT_DIR/stop_paged.sh"
    echo "[run_all] done"
}
trap cleanup EXIT

# Step 0: 编译
echo "========================================"
echo "[run_all] building..."
echo "========================================"
cd "$BUILD_DIR" && cmake .. -DCMAKE_BUILD_TYPE=Release > /dev/null 2>&1 && make -j$(nproc) 2>&1 | tail -5
echo ""

# Step 1: 启动 Paged
echo "========================================"
echo "[run_all] step 1: starting Paged"
echo "========================================"
bash "$SCRIPT_DIR/stop_paged.sh" 2>/dev/null || true
bash "$SCRIPT_DIR/start_paged.sh"
echo ""

# Step 2: 启动 Reader（后台）
echo "========================================"
echo "[run_all] step 2: starting Reader (log: build/reader.log)"
echo "========================================"
export LD_LIBRARY_PATH=/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost:$PROJECT_DIR/lib
cd "$BUILD_DIR"
./live_reader "$INTERVAL" > reader.log 2>&1 &
READER_PID=$!
echo "[reader] pid=$READER_PID"
sleep 1
echo ""

# Step 3: 启动 Replayer（前台）
echo "========================================"
echo "[run_all] step 3: starting Replayer at ${SPEED}x"
echo "[run_all] Ctrl+C to stop"
echo "========================================"
./journal_replayer "$SPEED"

# Replayer 结束，打印 reader 结果
echo ""
echo "========================================"
echo "[run_all] replayer finished, reader log tail:"
echo "========================================"
sleep 2  # 等 reader 消费完剩余数据
kill "$READER_PID" 2>/dev/null
READER_PID=""
wait 2>/dev/null
tail -20 "$BUILD_DIR/reader.log"
