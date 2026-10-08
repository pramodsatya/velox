#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Runs velox_cudf_jit_fusion_benchmark (CudfJitFusionBenchmark.cpp) and
# summarizes it as Markdown tables:
#   1. hot timings of arms A, B and C, checked against the CPU first;
#   2. kernels, copies and allocations per evaluation at --rows, under nsys;
#   3. compiling and first evaluating each case and arm in fresh processes,
#      with empty kernel caches, at 1,000 rows;
#   4. if a second build directory is given, arm A on that build (A0), which
#      should be one without the cuDF JIT call-node patch.
#
# Usage:
#   jit-fusion-benchmark.sh <build dir> <output dir> [<A0 build dir>]
# Environment: ROWS (default 10000000) and PROFILE_ITERATIONS (default 20) for
# 2, COLD_RUNS (default 3) for 3, and BENCHMARK_FLAGS, added to every run of 1
# and 4.

set -euo pipefail

BUILD_DIR=$(realpath "${1:?build directory}")
OUT_DIR=${2:?output directory}
A0_BUILD_DIR=${3:-}
ROWS=${ROWS:-10000000}
PROFILE_ITERATIONS=${PROFILE_ITERATIONS:-20}
COLD_RUNS=${COLD_RUNS:-3}
read -r -a FLAGS <<<"${BENCHMARK_FLAGS:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BINARY=velox/experimental/cudf/benchmarks/velox_cudf_jit_fusion_benchmark
BIN="${BUILD_DIR}/${BINARY}"
mkdir -p "${OUT_DIR}"
OUT_DIR=$(realpath "${OUT_DIR}")

# A kernel cache that outlives the processes, so that only the cold runs
# compile from scratch.
export LIBCUDF_KERNEL_CACHE_PATH="${OUT_DIR}/kernel-cache"

"${BIN}" --mode=list >"${OUT_DIR}/cases.csv"

echo "== hot timings"
"${BIN}" --mode=time "${FLAGS[@]}" >"${OUT_DIR}/time.csv"

if [[ -n ${A0_BUILD_DIR} ]]; then
  echo "== arm A on ${A0_BUILD_DIR}"
  "${A0_BUILD_DIR}/${BINARY}" --mode=time --arms=A "${FLAGS[@]}" >"${OUT_DIR}/time-a0.csv"
fi

echo "== nsys at ${ROWS} rows"
nsys profile --trace=cuda,nvtx --sample=none --cpuctxsw=none \
  --force-overwrite=true --export=sqlite -o "${OUT_DIR}/profile" \
  "${BIN}" --mode=profile --rows="${ROWS}" \
  --profile_iterations="${PROFILE_ITERATIONS}" >"${OUT_DIR}/profile.log" 2>&1

echo "== cold compiles"
cases=$(tail -n +2 "${OUT_DIR}/cases.csv" | cut -d, -f1)
echo "record,case,rows,arm,process_first_jit_ms,process_first_custom_op_jit_ms,compile_ms,first_eval_ms,hot_eval_ms" \
  >"${OUT_DIR}/cold.csv"
for _ in $(seq "${COLD_RUNS}"); do
  for case in ${cases}; do
    for arm in A B C; do
      cache=$(mktemp -d)
      LIBCUDF_KERNEL_CACHE_PATH="${cache}" LIBCUDF_JIT_DISABLE_CUDA_CACHE=1 \
        "${BIN}" --mode=cold --cases="${case}" --rows=1000 --null_pcts=0 --arms="${arm}" |
        grep '^cold,' >>"${OUT_DIR}/cold.csv"
      rm -rf "${cache}"
    done
  done
done

python3 "${SCRIPT_DIR}/summarize-jit-fusion-benchmark.py" "${OUT_DIR}" \
  --rows="${ROWS}" --profile-iterations="${PROFILE_ITERATIONS}" |
  tee "${OUT_DIR}/summary.md"
