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

#include "velox/experimental/cudf/CudfConfig.h"
#include "velox/experimental/cudf/exec/ToCudf.h"
#include "velox/experimental/cudf/exec/VeloxCudfInterop.h"
#include "velox/experimental/cudf/expression/AstExpression.h"
#include "velox/experimental/cudf/expression/ExpressionEvaluator.h"
#include "velox/experimental/cudf/expression/JitExpression.h"
#include "velox/experimental/cudf/expression/PrestoFunctions.h"
#include "velox/experimental/cudf/tests/utils/ExpressionTestUtil.h"

#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/expression/Expr.h"
#include "velox/vector/DecodedVector.h"

#include <cudf/utilities/default_stream.hpp>
#include <cudf/utilities/memory_resource.hpp>

#include <folly/Random.h>

#include <bit>
#include <cmath>
#include <limits>

using namespace facebook::velox;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::cudf_velox;
using namespace facebook::velox::cudf_velox::test_utils;

namespace {

// How JIT custom ops are configured: disabled, enabled and fused with the
// expression around them, or enabled and run as kernels of their own.
enum class Mode { kDisabled, kFused, kUnfused };

std::string modeName(Mode mode) {
  switch (mode) {
    case Mode::kDisabled:
      return "disabled";
    case Mode::kFused:
      return "fused";
    case Mode::kUnfused:
      return "unfused";
  }
  VELOX_UNREACHABLE();
}

class CudfJitCustomOpsTest : public OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
    savedConfig_ = CudfConfig::getInstance();
    CudfConfig::getInstance().allowCpuFallback = false;
    registerCudf();
    registerPrestoFunctions(CudfConfig::getInstance().functionNamePrefix);
    queryCtx_ = core::QueryCtx::create();
    execCtx_ = std::make_unique<core::ExecCtx>(pool(), queryCtx_.get());
  }

  void TearDown() override {
    execCtx_.reset();
    queryCtx_.reset();
    unregisterFunctions();
    unregisterCudf();
    CudfConfig::getInstance() = savedConfig_;
    OperatorTestBase::TearDown();
  }

  static void setMode(Mode mode) {
    CudfConfig::getInstance().jitCustomOpsEnabled = mode != Mode::kDisabled;
    CudfConfig::getInstance().jitCustomOpsFused = mode != Mode::kUnfused;
  }

  core::TypedExprPtr parse(const std::string& sql, const RowTypePtr& rowType) {
    return optimizeTypedExpr(sql, rowType, queryCtx_.get(), execCtx_.get());
  }

  std::shared_ptr<CudfExpression> compile(
      const std::string& sql,
      const RowTypePtr& rowType) {
    return createCudfExpression(parse(sql, rowType), rowType, pool());
  }

  // Evaluates sql over input on the GPU, as the operators do, and on the CPU,
  // and checks that the results are identical: equal nulls, and equal values,
  // bit for bit for DOUBLE except that all NaNs are equal.
  void assertMatchesCpu(const std::string& sql, const RowVectorPtr& input) {
    SCOPED_TRACE(sql);
    const auto rowType = asRowType(input->type());
    const auto expr = parse(sql, rowType);

    exec::ExprSet exprSet({expr}, execCtx_.get());
    exec::EvalCtx evalCtx(execCtx_.get(), &exprSet, input.get());
    SelectivityVector rows(input->size());
    std::vector<VectorPtr> expected(1);
    exprSet.eval(rows, evalCtx, expected);

    auto stream = cudf::get_default_stream();
    auto mr = cudf::get_current_device_resource_ref();
    auto table = with_arrow::toCudfTable(input, pool(), stream, mr);
    auto columns = table->release();
    std::vector<cudf::column_view> views;
    views.reserve(columns.size());
    for (const auto& column : columns) {
      views.push_back(column->view());
    }
    auto cudfExpr = createCudfExpression(expr, rowType, pool());
    auto result = cudfExpr->eval(views, stream, mr, /*finalize=*/true);
    auto actual = with_arrow::toVeloxColumn(
                      cudf::table_view({asView(result)}),
                      pool(),
                      ROW({"result"}, {expr->type()}),
                      "",
                      stream,
                      mr)
                      ->childAt(0);
    stream.sync();

    ASSERT_EQ(expected[0]->size(), actual->size());
    ASSERT_TRUE(expected[0]->type()->equivalent(*actual->type()))
        << expected[0]->type()->toString() << " vs "
        << actual->type()->toString();
    DecodedVector cpu(*expected[0]);
    DecodedVector gpu(*actual);
    const bool isDouble = expr->type()->kind() == TypeKind::DOUBLE;
    for (vector_size_t row = 0; row < input->size(); ++row) {
      ASSERT_EQ(cpu.isNullAt(row), gpu.isNullAt(row))
          << "row " << row << ": " << input->toString(row);
      if (cpu.isNullAt(row)) {
        continue;
      }
      if (isDouble) {
        const auto x = cpu.valueAt<double>(row);
        const auto y = gpu.valueAt<double>(row);
        if (std::isnan(x) && std::isnan(y)) {
          continue;
        }
        ASSERT_EQ(std::bit_cast<uint64_t>(x), std::bit_cast<uint64_t>(y))
            << "row " << row << ": " << input->toString(row) << " -> " << x
            << " on the CPU, " << y << " on the GPU";
      } else {
        ASSERT_TRUE(expected[0]->equalValueAt(actual.get(), row, row))
            << "row " << row << ": " << input->toString(row) << " -> "
            << expected[0]->toString(row) << " on the CPU, "
            << actual->toString(row) << " on the GPU";
      }
    }
  }

  // Dates: uniform over 1900-2100, then edge cases: the epoch, leap days,
  // century years, ISO weeks that cross a year, and the ends of years 1 and
  // 9999. Every tenth row is null.
  VectorPtr makeDates(vector_size_t size, uint32_t seed) {
    std::vector<int32_t> edges;
    for (const auto* date :
         {"1970-01-01", "1969-12-31", "1970-01-05", "1900-01-01", "1900-02-28",
          "1900-03-01", "2000-02-29", "2000-03-01", "2100-12-31", "2004-12-31",
          "2005-01-01", "2005-01-02", "2005-01-03", "2008-12-28", "2008-12-29",
          "2020-12-31", "2021-01-03", "2021-01-04", "2024-12-30", "2026-12-31",
          "0001-01-01", "0001-12-31", "9999-01-01", "9999-12-31"}) {
      edges.push_back(DATE()->toDays(date));
    }
    const int32_t first = DATE()->toDays("1900-01-01");
    const int32_t last = DATE()->toDays("2100-12-31");
    folly::Random::DefaultGenerator rng(seed);
    return makeFlatVector<int32_t>(
        size,
        [&](auto row) {
          return row < edges.size() ? edges[row]
                                    : first +
                  static_cast<int32_t>(folly::Random::rand32(
                      last - first + 1, rng));
        },
        [](auto row) { return row % 10 == 9; },
        DATE());
  }

  // Doubles for round: TPC-H-like prices, then values whose rounding is
  // delicate: halves, binary approximations of decimals, the 2^44 threshold
  // where the CPU switches algorithms, non-finite values, and extremes.
  VectorPtr makeDoubles(vector_size_t size, uint32_t seed) {
    const std::vector<double> edges = {
        0.0,
        -0.0,
        0.5,
        -0.5,
        1.5,
        2.5,
        -2.5,
        2.675,
        -2.675,
        1.005,
        0.125,
        123.456789,
        -123.456789,
        1e15 + 0.3,
        17592186044415.5,
        17592186044416.5,
        -17592186044415.25,
        1e300,
        -1e300,
        std::numeric_limits<double>::denorm_min(),
        std::numeric_limits<double>::max(),
        std::numeric_limits<double>::lowest(),
        std::numeric_limits<double>::quiet_NaN(),
        std::numeric_limits<double>::infinity(),
        -std::numeric_limits<double>::infinity()};
    folly::Random::DefaultGenerator rng(seed);
    return makeFlatVector<double>(
        size,
        [&](auto row) {
          return row < edges.size()
              ? edges[row]
              : 900.0 + folly::Random::randDouble01(rng) * 104'100.0;
        },
        [](auto row) { return row % 10 == 9; });
  }

  RowVectorPtr makeInput(vector_size_t size) {
    folly::Random::DefaultGenerator rng(7);
    return makeRowVector(
        {"d", "d2", "x", "price", "discount", "tax"},
        {makeDates(size, 1),
         makeDates(size, 2),
         makeDoubles(size, 3),
         makeDoubles(size, 4),
         makeFlatVector<double>(
             size,
             [&](auto) { return folly::Random::randDouble01(rng) * 0.1; }),
         makeFlatVector<double>(size, [&](auto) {
           return folly::Random::randDouble01(rng) * 0.08;
         })});
  }

  // The precomputed columns the JIT kernel of `expr` reads. Fails unless
  // `expr` is a JitExpression.
  static const std::vector<PrecomputeInstruction>& precomputes(
      const std::shared_ptr<CudfExpression>& expr) {
    const auto* jit = dynamic_cast<const JitExpression*>(expr.get());
    VELOX_CHECK_NOT_NULL(jit, "Not a JitExpression");
    return jit->precomputeInstructions();
  }

  CudfConfig savedConfig_;
  std::shared_ptr<core::QueryCtx> queryCtx_;
  std::unique_ptr<core::ExecCtx> execCtx_;
};

class CudfJitCustomOpsMatchCpuTest
    : public CudfJitCustomOpsTest,
      public ::testing::WithParamInterface<Mode> {};

TEST_P(CudfJitCustomOpsMatchCpuTest, functions) {
  setMode(GetParam());
  const auto input = makeInput(10'000);
  for (const auto* sql :
       {"year(d)",
        "month(d)",
        "quarter(d)",
        "week(d)",
        "week_of_year(d)",
        "date_trunc('week', d)",
        "date_trunc('month', d)",
        "date_trunc('quarter', d)",
        "date_trunc('year', d)",
        "date_trunc('Month', d)",
        "date_trunc('day', d)",
        "round(x)",
        "round(x, cast(0 as integer))",
        "round(x, cast(2 as integer))",
        "round(x, cast(5 as integer))",
        "round(x, cast(-2 as integer))",
        "round(x, cast(400 as integer))",
        "round(x, cast(-400 as integer))"}) {
    assertMatchesCpu(sql, input);
  }
}

// The expressions of CudfJitFusionBenchmark: custom ops inside arithmetic,
// comparisons, a conjunction and between, and a control without any.
TEST_P(CudfJitCustomOpsMatchCpuTest, compositions) {
  setMode(GetParam());
  const auto input = makeInput(10'000);
  for (
      const auto* sql :
      {"year(d) * 100 + month(d)",
       "round(price * (1.0 - discount), cast(2 as integer))",
       "year(d) = 1995 AND quarter(d) = 2",
       "date_trunc('month', d) = date_trunc('month', d2)",
       "date_trunc('quarter', d) BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'",
       "week(d) + quarter(d2) * 100",
       "price * (1.0 - discount) * (1.0 + tax)"}) {
    assertMatchesCpu(sql, input);
  }
}

// Dates far outside years 1-9999 that the CPU still converts exactly. week is
// left out: the CPU computes it through date::year_month_day, whose year is a
// short.
TEST_P(CudfJitCustomOpsMatchCpuTest, extremeDates) {
  setMode(GetParam());
  if (GetParam() == Mode::kDisabled) {
    GTEST_SKIP() << "cuDF's datetime functions keep years in 16 bits";
  }
  auto input = makeRowVector(
      {"d"},
      {makeFlatVector<int32_t>(
          {-1'000'000'000,
           -500'000'001,
           -719'163,
           2'932'897,
           500'000'000,
           1'000'000'000},
          DATE())});
  for (const auto* sql :
       {"year(d)",
        "month(d)",
        "quarter(d)",
        "date_trunc('week', d)",
        "date_trunc('month', d)",
        "date_trunc('quarter', d)",
        "date_trunc('year', d)"}) {
    assertMatchesCpu(sql, input);
  }
}

// The Presto operators, with project and filter.
TEST_P(CudfJitCustomOpsMatchCpuTest, filterProject) {
  setMode(GetParam());
  const std::vector<RowVectorPtr> input{makeInput(5'000)};
  const auto plan =
      PlanBuilder()
          .values(input)
          .filter("year(d) = 1995 AND quarter(d) = 2")
          .project(
              {"year(d) * 100 + month(d) AS ym",
               "date_trunc('month', d) AS month_start",
               "week(d) AS week",
               "round(price * (1.0 - discount), cast(2 as integer)) AS net"})
          .planNode();
  const auto gpu = AssertQueryBuilder(plan).copyResults(pool());
  unregisterCudf();
  const auto cpu = AssertQueryBuilder(plan).copyResults(pool());
  registerCudf();
  ASSERT_GT(cpu->size(), 0);
  facebook::velox::test::assertEqualVectors(cpu, gpu);
}

INSTANTIATE_TEST_SUITE_P(
    JitCustomOps,
    CudfJitCustomOpsMatchCpuTest,
    ::testing::Values(Mode::kDisabled, Mode::kFused, Mode::kUnfused),
    [](const auto& info) { return modeName(info.param); });

TEST_F(CudfJitCustomOpsTest, onlyJitCallsCustomOps) {
  const auto rowType = ROW({"d"}, {DATE()});
  const auto expr = parse("year(d)", rowType);
  setMode(Mode::kFused);
  EXPECT_TRUE(JitExpression::canEvaluate(expr));
  EXPECT_FALSE(ASTExpression::canEvaluate(expr));
  setMode(Mode::kDisabled);
  EXPECT_FALSE(JitExpression::canEvaluate(expr));
}

// The TIMESTAMP rule for AST/JIT still holds: no custom op takes a TIMESTAMP.
TEST_F(CudfJitCustomOpsTest, timestampStaysOutOfJit) {
  setMode(Mode::kFused);
  const auto rowType = ROW({"ts"}, {TIMESTAMP()});
  EXPECT_FALSE(JitExpression::canEvaluate(parse("year(ts)", rowType)));
  EXPECT_FALSE(
      JitExpression::canEvaluate(parse("date_trunc('month', ts)", rowType)));
  EXPECT_NE(
      dynamic_cast<FunctionExpression*>(compile("year(ts)", rowType).get()),
      nullptr);
}

TEST_F(CudfJitCustomOpsTest, routing) {
  const auto rowType = ROW({"d", "x"}, {DATE(), DOUBLE()});

  // Disabled: the JIT kernel reads year(d) and month(d), which cuDF functions
  // compute before it.
  setMode(Mode::kDisabled);
  auto expr = compile("year(d) * 100 + month(d)", rowType);
  ASSERT_EQ(precomputes(expr).size(), 2);
  for (const auto& precompute : precomputes(expr)) {
    EXPECT_NE(
        dynamic_cast<FunctionExpression*>(precompute.cudf_expression.get()),
        nullptr);
  }

  // Unfused: each call is a JIT kernel of its own that reads only columns.
  setMode(Mode::kUnfused);
  expr = compile("year(d) * 100 + month(d)", rowType);
  ASSERT_EQ(precomputes(expr).size(), 2);
  for (const auto& precompute : precomputes(expr)) {
    EXPECT_TRUE(precomputes(precompute.cudf_expression).empty());
  }
  expr = compile("round(x * 2.0, cast(1 as integer))", rowType);
  ASSERT_EQ(precomputes(expr).size(), 1);
  EXPECT_TRUE(precomputes(precomputes(expr)[0].cudf_expression).empty());

  // Fused: one JIT kernel computes everything.
  setMode(Mode::kFused);
  for (
      const auto* sql :
      {"year(d)",
       "year(d) * 100 + month(d)",
       "round(x * 2.0, cast(1 as integer))",
       "date_trunc('quarter', d) BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'"}) {
    SCOPED_TRACE(sql);
    EXPECT_TRUE(precomputes(compile(sql, rowType)).empty());
  }
}

} // namespace
