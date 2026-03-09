#!/bin/bash
# setup_parquet.sh
# Docker 上一次性运行：从项目内 Arrow 21.0.0 源码 + 预下载依赖，完全离线编译安装 Parquet
# 前提：third_party/ 已放入所有依赖文件
#
# 用法: bash setup_parquet.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
THIRD_PARTY="${SCRIPT_DIR}/third_party"
ARROW_TARBALL="${THIRD_PARTY}/apache-arrow-21.0.0.tar.gz"
BUILD_DIR="/tmp/arrow-parquet-build"
NPROC=$(nproc)

# ============ Docker 环境依赖路径（与 kungfu_demo 一致）============
TOOLCHAIN_DIR="/opt/kungfu/toolchain"
BOOST_ROOT="${TOOLCHAIN_DIR}/boost-1.62.0"

echo "=== Arrow Parquet 离线编译安装脚本 ==="

# [0] 安装 libatomic（Arrow 链接需要，CentOS 8 默认未装）
LIBATOMIC_SO="${THIRD_PARTY}/libatomic.so.1.2.0"
if [ ! -f /usr/lib64/libatomic.so.1.2.0 ]; then
    if [ -f "${LIBATOMIC_SO}" ]; then
        echo "[0/4] 安装 libatomic..."
        cp "${LIBATOMIC_SO}" /usr/lib64/
        ln -sf /usr/lib64/libatomic.so.1.2.0 /usr/lib64/libatomic.so.1
        ln -sf /usr/lib64/libatomic.so.1.2.0 /usr/lib64/libatomic.so
        ldconfig
    else
        echo "ERROR: /usr/lib64/libatomic.so.1.2.0 不存在，且 ${LIBATOMIC_SO} 也未找到"
        exit 1
    fi
else
    echo "[0/4] libatomic 已存在，跳过"
fi

# 检查必要文件
if [ ! -f "${ARROW_TARBALL}" ]; then
    echo "ERROR: Arrow 源码不存在: ${ARROW_TARBALL}"
    exit 1
fi

# [1] 解压源码
echo "[1/4] 解压 Arrow 源码..."
rm -rf "${BUILD_DIR}"
mkdir -p "${BUILD_DIR}"
tar xzf "${ARROW_TARBALL}" -C "${BUILD_DIR}" --strip-components=1

ARROW_CPP_DIR="${BUILD_DIR}/cpp"
BUILD_OUT="${BUILD_DIR}/cpp/build"
mkdir -p "${BUILD_OUT}"

# [2] CMake 配置（完全离线：依赖用 file:// URL，Boost 用系统已有的）
echo "[2/4] CMake 配置（离线模式）..."
cd "${BUILD_OUT}"

cmake "${ARROW_CPP_DIR}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=/usr/local \
    -DARROW_BUILD_STATIC=OFF \
    -DARROW_BUILD_SHARED=ON \
    -DARROW_PARQUET=ON \
    -DARROW_WITH_SNAPPY=ON \
    -DARROW_WITH_ZLIB=ON \
    -DARROW_SIMD_LEVEL=NONE \
    -DARROW_COMPUTE=OFF \
    -DARROW_CSV=OFF \
    -DARROW_DATASET=OFF \
    -DARROW_FILESYSTEM=ON \
    -DARROW_FLIGHT=OFF \
    -DARROW_JSON=OFF \
    -DARROW_TESTING=OFF \
    -DARROW_BUILD_TESTS=OFF \
    -DARROW_BUILD_EXAMPLES=OFF \
    -DARROW_DEPENDENCY_SOURCE=BUNDLED \
    -DBoost_SOURCE=SYSTEM \
    -DBOOST_ROOT="${BOOST_ROOT}" \
    -DARROW_THRIFT_URL="file://${THIRD_PARTY}/thrift-0.22.0.tar.gz" \
    -DARROW_SNAPPY_URL="file://${THIRD_PARTY}/snappy-1.2.2.tar.gz" \
    -DARROW_ZLIB_URL="file://${THIRD_PARTY}/zlib-1.3.1.tar.gz" \
    -DARROW_MIMALLOC_URL="file://${THIRD_PARTY}/mimalloc-2.2.4.tar.gz" \
    -DARROW_RAPIDJSON_URL="file://${THIRD_PARTY}/rapidjson-232389d.tar.gz" \
    -DARROW_XSIMD_URL="file://${THIRD_PARTY}/xsimd-13.0.0.tar.gz"

# [3] 编译
echo "[3/4] 编译 (${NPROC} jobs)..."
make -j"${NPROC}"

# [4] 安装
echo "[4/4] 安装到 /usr/local..."
make install
ldconfig

echo ""
echo "=== 安装完成 ==="
echo "Arrow + Parquet 已安装到 /usr/local/"
echo ""
echo "编译 archive_journal_demo:"
echo "  cd ${SCRIPT_DIR} && mkdir -p build && cd build"
echo "  cmake .. && make -j\$(nproc)"
