/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/// Microbenchmark of JIT custom ops (CudfConfig::jitCustomOpsEnabled), which
/// run Presto functions that cuDF has no operator for as Velox device code
/// inside the JIT kernel of the expression around them.
///
/// Each case is one expression, compiled and evaluated as CudfFilterProject
/// does it (expression::optimize, createCudfExpression, eval with finalize),
/// in one of three arms:
///   A  custom ops disabled, which is how OSS Velox evaluates the expression:
///      a JIT kernel for what the AST covers, and cuDF function calls before
///      it for the rest;
///   B  custom ops enabled but not fused: every custom op call is a JIT kernel
///      of its own, whose result the expression reads as a column;
///   C  custom ops enabled and fused: one JIT kernel.
/// B against A is the gain from evaluating a function as one kernel, C against
/// B the gain from fusion alone.
///
/// Modes, each printing CSV:
///   time     checks every arm against the Velox CPU, then times the arms,
///            interleaved within each round, as an evaluation and a stream
///            synchronization, and counts the bytes each evaluation allocates;
///   profile  evaluates each arm --profile_iterations times inside an NVTX
///            range named "<case>/<rows>/<null pct>/<arm>", for nsys;
///   cold     times compiling and first evaluating one case and arm in a
///            process whose kernel caches start empty, after a JIT kernel and
///            a JIT kernel with a custom op that pay what a process pays once;
///   list     prints the cases.
///
/// Only --arms=A works on a build without custom ops, whose CudfConfig ignores
/// the custom op keys.

#include "velox/experimental/cudf/CudfConfig.h"
#include "velox/experimental/cudf/exec/GpuResources.h"
#include "velox/experimental/cudf/exec/ToCudf.h"
#include "velox/experimental/cudf/exec/VeloxCudfInterop.h"
#include "velox/experimental/cudf/expression/ExpressionEvaluator.h"
#include "velox/experimental/cudf/expression/PrestoFunctions.h"
#include "velox/experimental/cudf/tests/utils/ExpressionTestUtil.h"

#include "velox/common/memory/Memory.h"
#include "velox/core/QueryCtx.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

#include <cudf/utilities/default_stream.hpp>
#include <cudf/utilities/memory_resource.hpp>

#include <nvtx3/nvtx3.hpp>

#include <fmt/format.h>
#include <folly/Benchmark.h>
#include <folly/String.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include <algorithm>
#include <bit>
#include <chrono>
#include <cmath>
#include <iostream>
#include <random>

DEFINE_string(mode, "time", "time, profile, cold or list");
DEFINE_string(cases, "1,3,4,5,7,8,9,10,11", "Cases to run");
DEFINE_string(rows, "1000,100000,1000000,10000000", "Row counts");
DEFINE_string(
    null_pcts,
    "0,10",
    "Percentages of null rows, for the cases that run with nulls (5, 7, 8); "
    "the other cases run without nulls");
DEFINE_string(arms, "A,C,B", "Arms, in the order every round runs them");
DEFINE_int32(rounds, 7, "Timed rounds per case and row count");
DEFINE_double(
    round_ms,
    100,
    "Least time an arm runs per round, which sets its iterations");
DEFINE_int32(warmup, 3, "Untimed evaluations per arm before the rounds");
DEFINE_bool(verify, true, "Check each arm against the Velox CPU first");
DEFINE_int32(profile_iterations, 20, "Evaluations per NVTX range");
DEFINE_int32(cold_hot_iterations, 9, "Evaluations after the first, in cold");
DEFINE_uint64(seed, 42, "Seed of the input data");

using namespace facebook::velox;
using namespace facebook::velox::cudf_velox;

namespace {

struct Case {
  int id;
  const char* sql;
  // Whether the case runs with every --null_pcts, rather than without nulls.
  bool withNulls;
};

const std::vector<Case>& cases() {
  static const std::vector<Case> kCases = {
      {1, "date_trunc('month', d)", false},
      {3, "week(d)", false},
      {4, "year(d)", false},
      {5, "year(d) * 100 + month(d)", true},
      {7, "round(price * (1.0 - discount), cast(2 as integer))", true},
      {8, "year(d) = 1995 AND quarter(d) = 2", true},
      {9, "date_trunc('month', d) = date_trunc('month', d2)", false},
      {10,
       "date_trunc('quarter', d) BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'",
       false},
      {11, "price * (1.0 - discount) * (1.0 + tax)", false},
  };
  return kCases;
}

struct Arm {
  std::string name;
  std::unordered_map<std::string, std::string> config;
};

Arm arm(const std::string& name) {
  if (name == "A") {
    return {name, {{"cudf.jit_custom_ops_enabled", "false"}}};
  }
  if (name == "B") {
    return {
        name,
        {{"cudf.jit_custom_ops_enabled", "true"},
         {"cudf.jit_custom_ops_fused", "false"}}};
  }
  VELOX_USER_CHECK_EQ(name, "C", "Unknown arm");
  return {
      name,
      {{"cudf.jit_custom_ops_enabled", "true"},
       {"cudf.jit_custom_ops_fused", "true"}}};
}

template <typename T>
std::vector<T> parseList(const std::string& list) {
  std::vector<T> values;
  folly::splitTo<T>(',', list, std::back_inserter(values), true);
  return values;
}

struct Env {
  Env() {
    memory::MemoryManager::initialize(memory::MemoryManager::Options{});
    functions::prestosql::registerAllScalarFunctions();
    parse::registerTypeResolver();
    registerCudf();
    registerPrestoFunctions(CudfConfig::getInstance().functionNamePrefix);
    pool = memory::memoryManager()->addLeafPool("jit_fusion_benchmark");
    queryCtx = core::QueryCtx::create();
    execCtx = std::make_unique<core::ExecCtx>(pool.get(), queryCtx.get());
  }

  std::shared_ptr<memory::MemoryPool> pool;
  std::shared_ptr<core::QueryCtx> queryCtx;
  std::unique_ptr<core::ExecCtx> execCtx;
};

Env& env() {
  static Env env;
  return env;
}

// TPC-H-like columns: dates uniform over 1900-2100, prices of 900 to 105,000,
// discounts up to 0.10 and taxes up to 0.08, as DOUBLE because DECIMAL stays
// out of the JIT. Each column has nullPct percent of null rows.
RowVectorPtr makeInput(vector_size_t rows, int nullPct) {
  auto* pool = env().pool.get();
  std::mt19937_64 rng(FLAGS_seed);
  std::bernoulli_distribution isNull(nullPct / 100.0);
  const int32_t firstDay = DATE()->toDays("1900-01-01");
  const int32_t lastDay = DATE()->toDays("2100-12-31");

  auto makeColumn = [&](const TypePtr& type, auto&& value) {
    using T = std::decay_t<decltype(value())>;
    auto vector = BaseVector::create<FlatVector<T>>(type, rows, pool);
    auto* raw = vector->mutableRawValues();
    for (vector_size_t row = 0; row < rows; ++row) {
      raw[row] = value();
      if (nullPct > 0 && isNull(rng)) {
        vector->setNull(row, true);
      }
    }
    return vector;
  };
  auto date = [&] {
    return std::uniform_int_distribution<int32_t>(firstDay, lastDay)(rng);
  };
  auto uniform = [&](double low, double high) {
    return [&rng, low, high] {
      return std::uniform_real_distribution<double>(low, high)(rng);
    };
  };
  std::vector<VectorPtr> children = {
      makeColumn(DATE(), date),
      makeColumn(DATE(), date),
      makeColumn(DOUBLE(), uniform(900.0, 105'000.0)),
      makeColumn(DOUBLE(), uniform(0.0, 0.10)),
      makeColumn(DOUBLE(), uniform(0.0, 0.08))};
  return std::make_shared<RowVector>(
      pool,
      ROW({"d", "d2", "price", "discount", "tax"},
          {DATE(), DATE(), DOUBLE(), DOUBLE(), DOUBLE()}),
      nullptr,
      rows,
      std::move(children));
}

// The input on the GPU.
struct GpuInput {
  std::vector<std::unique_ptr<cudf::column>> columns;
  std::vector<cudf::column_view> views;
};

GpuInput upload(const RowVectorPtr& input) {
  auto stream = cudf::get_default_stream();
  GpuInput gpu;
  gpu.columns = with_arrow::toCudfTable(
                    input,
                    env().pool.get(),
                    stream,
                    cudf::get_current_device_resource_ref())
                    ->release();
  for (const auto& column : gpu.columns) {
    gpu.views.push_back(column->view());
  }
  stream.sync();
  return gpu;
}

core::TypedExprPtr parse(const std::string& sql, const RowTypePtr& rowType) {
  return cudf_velox::test_utils::optimizeTypedExpr(
      sql, rowType, env().queryCtx.get(), env().execCtx.get());
}

std::shared_ptr<CudfExpression> compile(
    const Arm& arm,
    const core::TypedExprPtr& expr,
    const RowTypePtr& rowType) {
  auto config = arm.config;
  CudfConfig::getInstance().initialize(std::move(config));
  return createCudfExpression(expr, rowType, env().pool.get());
}

ColumnOrView evaluate(CudfExpression& expr, const GpuInput& input) {
  auto stream = cudf::get_default_stream();
  auto result = expr.eval(
      input.views, stream, cudf::get_current_device_resource_ref(), true);
  stream.sync();
  return result;
}

VectorPtr evaluateOnCpu(
    const core::TypedExprPtr& expr,
    const RowVectorPtr& input) {
  exec::ExprSet exprSet({expr}, env().execCtx.get());
  exec::EvalCtx evalCtx(env().execCtx.get(), &exprSet, input.get());
  SelectivityVector rows(input->size());
  std::vector<VectorPtr> results(1);
  exprSet.eval(rows, evalCtx, results);
  return results[0];
}

// Checks that a GPU result equals the CPU's: equal nulls, and equal values,
// bit for bit for DOUBLE except that all NaNs are equal.
void verify(
    const Case& c,
    const Arm& arm,
    const core::TypedExprPtr& expr,
    ColumnOrView& result,
    const VectorPtr& expected) {
  auto stream = cudf::get_default_stream();
  const auto actual = with_arrow::toVeloxColumn(
                          cudf::table_view({asView(result)}),
                          env().pool.get(),
                          ROW({"result"}, {expr->type()}),
                          "",
                          stream,
                          cudf::get_current_device_resource_ref())
                          ->childAt(0);
  stream.sync();
  VELOX_CHECK_EQ(actual->size(), expected->size());
  DecodedVector cpu(*expected);
  DecodedVector gpu(*actual);
  const bool isDouble = expr->type()->kind() == TypeKind::DOUBLE;
  for (vector_size_t row = 0; row < expected->size(); ++row) {
    bool equal = cpu.isNullAt(row) == gpu.isNullAt(row);
    if (equal && !cpu.isNullAt(row)) {
      if (isDouble) {
        const auto x = cpu.valueAt<double>(row);
        const auto y = gpu.valueAt<double>(row);
        equal = (std::isnan(x) && std::isnan(y)) ||
            std::bit_cast<uint64_t>(x) == std::bit_cast<uint64_t>(y);
      } else {
        equal = expected->equalValueAt(actual.get(), row, row);
      }
    }
    VELOX_CHECK(
        equal,
        "Case {} arm {} differs from the CPU at row {}: {} on the CPU, {} on "
        "the GPU",
        c.id,
        arm.name,
        row,
        expected->toString(row),
        actual->toString(row));
  }
}

struct Allocations {
  int64_t bytes;
  int64_t count;
  int64_t peakBytes;
  int64_t resultBytes;
};

// What one evaluation allocates through the statistics adaptor that wraps the
// current device resource, which also serves the output and temporary
// allocations.
Allocations countAllocations(CudfExpression& expr, const GpuInput& input) {
  VELOX_CHECK(statsMr_.has_value());
  statsMr_->push_counters();
  auto result = evaluate(expr, input);
  const auto [bytes, allocations] = statsMr_->pop_counters();
  return {bytes.total, allocations.total, bytes.peak, bytes.value};
}

double elapsedMs(std::chrono::steady_clock::time_point start) {
  return std::chrono::duration<double, std::milli>(
             std::chrono::steady_clock::now() - start)
      .count();
}

// Mean time of an evaluation and a stream synchronization, over iterations.
double timeEvaluations(CudfExpression& expr, const GpuInput& input, int n) {
  const auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < n; ++i) {
    auto result = evaluate(expr, input);
    folly::doNotOptimizeAway(result);
  }
  return elapsedMs(start) / n;
}

struct Config {
  const Case* c;
  vector_size_t rows;
  int nullPct;
};

std::vector<Config> configs() {
  std::vector<Config> result;
  const auto ids = parseList<int>(FLAGS_cases);
  for (const auto& c : cases()) {
    if (std::find(ids.begin(), ids.end(), c.id) == ids.end()) {
      continue;
    }
    for (const auto rows : parseList<vector_size_t>(FLAGS_rows)) {
      for (const auto nullPct :
           c.withNulls ? parseList<int>(FLAGS_null_pcts) : std::vector{0}) {
        result.push_back({&c, rows, nullPct});
      }
    }
  }
  return result;
}

void runTime() {
  std::cout << "record,case,rows,null_pct,arm,round,iterations,ms_per_eval,"
               "alloc_bytes,allocs,peak_bytes,result_bytes"
            << std::endl;
  const auto arms = parseList<std::string>(FLAGS_arms);
  for (const auto& config : configs()) {
    const auto input = makeInput(config.rows, config.nullPct);
    const auto rowType = asRowType(input->type());
    const auto gpuInput = upload(input);
    const auto expr = parse(config.c->sql, rowType);
    const auto expected = FLAGS_verify ? evaluateOnCpu(expr, input) : nullptr;

    std::vector<std::shared_ptr<CudfExpression>> compiled;
    std::vector<int> iterations;
    for (const auto& name : arms) {
      const auto a = arm(name);
      compiled.push_back(compile(a, expr, rowType));
      auto& cudfExpr = *compiled.back();
      if (expected) {
        auto result = evaluate(cudfExpr, gpuInput);
        verify(*config.c, a, expr, result, expected);
      }
      for (int i = 0; i < FLAGS_warmup; ++i) {
        evaluate(cudfExpr, gpuInput);
      }
      const auto allocations = countAllocations(cudfExpr, gpuInput);
      std::cout << fmt::format(
                       "alloc,{},{},{},{},,,,{},{},{},{}",
                       config.c->id,
                       config.rows,
                       config.nullPct,
                       name,
                       allocations.bytes,
                       allocations.count,
                       allocations.peakBytes,
                       allocations.resultBytes)
                << std::endl;
      const double estimateMs = timeEvaluations(cudfExpr, gpuInput, 3);
      iterations.push_back(
          std::clamp<int>(std::ceil(FLAGS_round_ms / estimateMs), 1, 10'000));
    }
    for (int round = 0; round < FLAGS_rounds; ++round) {
      for (size_t i = 0; i < arms.size(); ++i) {
        const double ms =
            timeEvaluations(*compiled[i], gpuInput, iterations[i]);
        std::cout << fmt::format(
                         "time,{},{},{},{},{},{},{:.6f},,,,",
                         config.c->id,
                         config.rows,
                         config.nullPct,
                         arms[i],
                         round,
                         iterations[i],
                         ms)
                  << std::endl;
      }
    }
  }
}

void runProfile() {
  const auto arms = parseList<std::string>(FLAGS_arms);
  for (const auto& config : configs()) {
    const auto input = makeInput(config.rows, config.nullPct);
    const auto rowType = asRowType(input->type());
    const auto gpuInput = upload(input);
    const auto expr = parse(config.c->sql, rowType);
    for (const auto& name : arms) {
      auto cudfExpr = compile(arm(name), expr, rowType);
      for (int i = 0; i < FLAGS_warmup; ++i) {
        evaluate(*cudfExpr, gpuInput);
      }
      const auto range = fmt::format(
          "{}/{}/{}/{}", config.c->id, config.rows, config.nullPct, name);
      nvtx3::scoped_range scope{range.c_str()};
      for (int i = 0; i < FLAGS_profile_iterations; ++i) {
        evaluate(*cudfExpr, gpuInput);
      }
    }
  }
  std::cout << "profiled " << configs().size() << " configurations"
            << std::endl;
}

// For one case and arm, in a process whose kernel caches start empty: the
// first JIT kernel and the first JIT kernel with a custom op in the process,
// both unrelated to the case, which pay what a process pays once for each kind
// (NVRTC compiles the second kind to LTO-IR, with other options), then
// compiling the case, its first evaluation, and the median of the evaluations
// after it.
void runCold() {
  const auto all = configs();
  const auto arms = parseList<std::string>(FLAGS_arms);
  VELOX_USER_CHECK(
      all.size() == 1 && arms.size() == 1,
      "cold runs one case, row count and arm");
  const auto& config = all.front();
  const auto input = makeInput(config.rows, config.nullPct);
  const auto rowType = asRowType(input->type());
  const auto gpuInput = upload(input);

  auto firstJitMs = [&](const char* armName, const char* sql) {
    const auto start = std::chrono::steady_clock::now();
    evaluate(*compile(arm(armName), parse(sql, rowType), rowType), gpuInput);
    return elapsedMs(start);
  };
  const double processMs = firstJitMs("A", "tax * 3.0 + 1.0");
  const double processCustomOpMs = firstJitMs("C", "month(d2) + 7");

  const auto expr = parse(config.c->sql, rowType);
  auto start = std::chrono::steady_clock::now();
  auto cudfExpr = compile(arm(arms.front()), expr, rowType);
  const double compileMs = elapsedMs(start);
  start = std::chrono::steady_clock::now();
  evaluate(*cudfExpr, gpuInput);
  const double firstEvalMs = elapsedMs(start);
  std::vector<double> hot;
  for (int i = 0; i < FLAGS_cold_hot_iterations; ++i) {
    hot.push_back(timeEvaluations(*cudfExpr, gpuInput, 1));
  }
  std::sort(hot.begin(), hot.end());
  std::cout << "record,case,rows,arm,process_first_jit_ms,"
               "process_first_custom_op_jit_ms,compile_ms,first_eval_ms,"
               "hot_eval_ms"
            << std::endl
            << fmt::format(
                   "cold,{},{},{},{:.3f},{:.3f},{:.3f},{:.3f},{:.6f}",
                   config.c->id,
                   config.rows,
                   arms.front(),
                   processMs,
                   processCustomOpMs,
                   compileMs,
                   firstEvalMs,
                   hot[hot.size() / 2])
            << std::endl;
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  if (FLAGS_mode != "list") {
    env();
  }
  if (FLAGS_mode == "time") {
    runTime();
  } else if (FLAGS_mode == "profile") {
    runProfile();
  } else if (FLAGS_mode == "cold") {
    runCold();
  } else if (FLAGS_mode == "list") {
    std::cout << "case,with_nulls,sql" << std::endl;
    for (const auto& c : cases()) {
      std::cout << fmt::format("{},{},\"{}\"", c.id, c.withNulls, c.sql)
                << std::endl;
    }
  } else {
    VELOX_USER_FAIL("Unknown --mode {}", FLAGS_mode);
  }
  return 0;
}
