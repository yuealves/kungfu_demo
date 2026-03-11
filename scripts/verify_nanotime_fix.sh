#!/bin/bash
# 验证新编译的 libjournal.so 的 getNanoTime() 修复
#
# 用法:
#   ./scripts/verify_nanotime_fix.sh /path/to/new/libjournal.so.1.1
#
# 两种验证方式：
#   方法一：独立测试 — 直接对比 getNanoTime() 与 clock_gettime，最直接
#   方法二：replayer+reader 流程 — 通过完整的 journal 写入/读取链路验证

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$PROJECT_DIR/build"

NEW_SO="$1"

if [ -z "$NEW_SO" ]; then
    echo "用法: $0 /path/to/new/libjournal.so.1.1"
    echo ""
    echo "示例:"
    echo "  $0 /tmp/libjournal.so.1.1"
    echo "  $0 ../kungfu_source/build/yijinjing/journal/libjournal.so.1.1"
    exit 1
fi

if [ ! -f "$NEW_SO" ]; then
    echo "[ERROR] 文件不存在: $NEW_SO"
    exit 1
fi

NEW_SO_DIR="$(cd "$(dirname "$NEW_SO")" && pwd)"
NEW_SO_FILE="$(basename "$NEW_SO")"

# 确保有 libjournal.so 符号链接
if [ ! -f "$NEW_SO_DIR/libjournal.so" ]; then
    echo "[prep] 创建符号链接: libjournal.so -> $NEW_SO_FILE"
    ln -sf "$NEW_SO_FILE" "$NEW_SO_DIR/libjournal.so"
fi

# LD_LIBRARY_PATH: 新 .so 目录在最前面
export LD_LIBRARY_PATH="$NEW_SO_DIR:/opt/kungfu/master/lib/yijinjing:/opt/kungfu/master/lib/boost:$PROJECT_DIR/lib"

echo "========================================"
echo "  getNanoTime() 修复验证"
echo "========================================"
echo "新 .so: $NEW_SO"
echo "LD_LIBRARY_PATH: $LD_LIBRARY_PATH"
echo ""

# ---------- 编译 ----------
echo "[build] 编译验证程序..."
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"
cmake .. -DCMAKE_BUILD_TYPE=Release > /dev/null 2>&1
make verify_journal_time -j$(nproc) 2>&1 | tail -3
echo ""

# 编译独立测试程序
g++ -std=c++11 -O2 -o verify_nanotime "$PROJECT_DIR/tests/verify_nanotime.cpp" -ldl

# ---------- 方法一：独立测试 ----------
echo "========================================"
echo "  方法一：独立精度测试"
echo "========================================"
echo ""
./verify_nanotime
echo ""

# ---------- 方法二提示 ----------
echo "========================================"
echo "  方法二：replayer + reader 验证（可选）"
echo "========================================"
echo ""
echo "如需通过完整 journal 链路验证，请在 3 个终端分别执行："
echo ""
echo "  终端 1 - 启动 Paged:"
echo "    bash $SCRIPT_DIR/start_paged.sh"
echo ""
echo "  终端 2 - 启动验证 reader:"
echo "    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH"
echo "    cd $BUILD_DIR && ./verify_journal_time"
echo ""
echo "  终端 3 - 启动 replayer:"
echo "    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH"
echo "    cd $BUILD_DIR && ./journal_replayer 10"
echo ""
echo "或者用一键脚本（使用 live_reader 而非 verify_journal_time）:"
echo "    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH"
echo "    bash $SCRIPT_DIR/run_all.sh 10 1000"
