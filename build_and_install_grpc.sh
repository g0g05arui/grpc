#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ -f "${ROOT_DIR}/CMakeLists.txt" ]]; then
  SRC_DIR="${ROOT_DIR}"
elif [[ -f "${ROOT_DIR}/cmake/CMakeLists.txt" ]]; then
  SRC_DIR="${ROOT_DIR}/cmake"
else
  echo "Could not find CMakeLists.txt in ${ROOT_DIR} or ${ROOT_DIR}/cmake" >&2
  exit 1
fi

BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build}"
BUILD_TYPE="${BUILD_TYPE:-RelWithDebInfo}"
INSTALL_PREFIX="${INSTALL_PREFIX:-/usr/local}"
JOBS="${JOBS:-$(nproc)}"

echo "==> Configuring gRPC"
cmake -S "${SRC_DIR}" -B "${BUILD_DIR}" \
  -DgRPC_BUILD_TESTS=OFF \
  -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" \
  -DCMAKE_INSTALL_PREFIX="${INSTALL_PREFIX}"

echo "==> Building gRPC (jobs=${JOBS})"
cmake --build "${BUILD_DIR}" -j"${JOBS}"

echo "==> Installing gRPC to ${INSTALL_PREFIX}"
if [[ "$(id -u)" -eq 0 ]]; then
  sudo cmake --install "${BUILD_DIR}"
  ldconfig || true
else
  sudo cmake --install "${BUILD_DIR}"
  sudo ldconfig || true
fi

echo "==> Done"
