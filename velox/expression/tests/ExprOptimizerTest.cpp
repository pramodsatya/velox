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

#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/expression/ExprOptimizer.h"
#include "velox/expression/ExprRewriteRegistry.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/functions/prestosql/types/JsonType.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"

namespace facebook::velox::expression {
namespace {

class ExprOptimizerTest : public functions::test::FunctionBaseTest {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    functions::prestosql::registerAllScalarFunctions("");
    parse::registerTypeResolver();
    options_.parseDecimalAsDouble = false;
  }

  void TearDown() override {
    expression::ExprRewriteRegistry::instance().clear();
  }

  /// Validates the result of optimizing expression of given type matches the
  /// expected expression.
  /// @param expression Input expression to be optimized.
  /// @param expected Expected expression after optimization.
  /// @param type Type of input and expected expressions.
  void testExpression(
      const core::TypedExprPtr& expression,
      const core::TypedExprPtr& expected,
      const RowTypePtr& type) {
    auto optimized =
        expression::optimize(expression, queryCtx_.get(), pool(), false);
    SCOPED_TRACE(fmt::format(
        "Input: {}\nOptimized: {}\nExpected: {}",
        expression->toString(),
        optimized->toString(),
        expected->toString()));
    ASSERT_TRUE(*optimized == *expected);

    const auto data = fuzzFlat(type);
    const auto optimizedResult = evaluate(optimized, data);
    const auto expectedResult = evaluate(expected, data);
    test::assertEqualVectors(optimizedResult, expectedResult);
  }

  /// Validates expression optimization and provides an option to specify input
  /// types for lambda expressions in the input and expected expressions.
  /// @param input Input expression to be optimized.
  /// @param expected Expected expression after optimization.
  /// @param type Row type of 'input' and 'expected'.
  /// @param lambdaInputTypes Input types for any lambda sub-expressions in .
  /// @param lambdaInputTypes Types of inputs to lambda expression that could be
  /// present in 'input'.
  /// @param expectedLambdaInputTypes Types of inputs to lambda expression that
  /// could be present in 'expected'.
  /// @param error Expected error message upon evaluating 'input'.
  void testExpression(
      const std::string& expression,
      const std::string& expected,
      const RowTypePtr& type = ROW({}),
      const std::vector<TypePtr>& lambdaInputTypes = {},
      const std::vector<TypePtr>& expectedLambdaInputTypes = {}) {
    const auto typedExpr = makeTypedExpr(expression, type, lambdaInputTypes);
    const auto expectedTypedExpr =
        makeTypedExpr(expected, type, expectedLambdaInputTypes);
    testExpression(typedExpr, expectedTypedExpr, type);
  }

  /// Validates that the subexpressions that throw an exception during
  /// optimization are replaced with a fail expression when optimize is called
  /// with 'failResult = true'. Also validates that the
  /// optimized expression containing fail expression(s) will throw the expected
  /// error upon evaluation.
  /// @param input Input expression to be optimized.
  /// @param expected Expected expression as a string.
  /// @param type Type of input expression.
  /// @param lambdaInputTypes Types of inputs to lambda expression that could be
  /// present in 'input'.
  void assertFailCall(
      const std::string& input,
      const std::string& expected,
      const std::string& error,
      const RowTypePtr& type = ROW({}),
      const std::vector<TypePtr>& lambdaInputTypes = {}) {
    const auto expr = makeTypedExpr(input, type, lambdaInputTypes);
    auto optimized = expression::optimize(expr, queryCtx_.get(), pool(), true);
    ASSERT_EQ(optimized->toString(), expected);
    VELOX_ASSERT_THROW(evaluate(optimized, fuzzFlat(type)), error);
  }

  void setQueryTimeZone(const std::string& timeZone) {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kSessionTimezone, timeZone},
         {core::QueryConfig::kAdjustTimestampToTimezone, "true"}});
  }

 private:
  /// Helper method to parse SQL expression into a typed expression tree using
  /// DuckDB's SQL parser.
  /// @param expression String representation of SQL expression to be parsed.
  /// @param rowType Row type of 'expression'.
  /// @param lambdaInputTypes Input types for any lambda sub-expressions.
  core::TypedExprPtr makeTypedExpr(
      const std::string& expression,
      const RowTypePtr& rowType,
      const std::vector<TypePtr>& lambdaInputTypes) {
    auto untyped = parse::parseExpr(expression, options_);
    return core::Expressions::inferTypes(
        untyped, rowType, lambdaInputTypes, execCtx_->pool());
  }

  RowVectorPtr fuzzFlat(const RowTypePtr& rowType) {
    VectorFuzzer::Options options;
    options.vectorSize = 100;
    VectorFuzzer fuzzer(options, pool());
    return fuzzer.fuzzInputFlatRow(rowType);
  }

  std::shared_ptr<core::QueryCtx> queryCtx_{core::QueryCtx::create()};
  std::unique_ptr<core::ExecCtx> execCtx_{
      std::make_unique<core::ExecCtx>(pool_.get(), queryCtx_.get())};
  parse::ParseOptions options_;
};

/// Test for constant folding.
TEST_F(ExprOptimizerTest, constantFolding) {
  // basic.
  testExpression("IF(2 = 2, 3, 4)", "3");
  testExpression("IF(cast(null AS BOOLEAN), 3, 4)", "4");
  testExpression("IF('hello' = 'foo', cast(null AS BIGINT), 4)", "4");
  testExpression(
      "MAP(ARRAY [ARRAY[1 + 2, 1]], ARRAY['a'])[ARRAY[3 * 1, 1]]", "'a'");

  // comparison.
  testExpression("'a' = substr('abc', 1, 1)", "true");
  testExpression("'a' = cast(null as varchar)", "cast(null AS BOOLEAN)");
  testExpression(
      "cast((12345678901234567890.000 + 0.123) AS DECIMAL(23, 3)) = cast(12345678901234567890.123 AS DECIMAL(23, 3))",
      "true");
  testExpression("'0ab01c' LIKE '%ab%c%'", "true");
  testExpression("3 between (4 - 2) and 2 * 2", "true");
  testExpression(
      "3 between cast(null AS BIGINT) and 4", "cast(null AS BOOLEAN)");

  // cast.
  testExpression("cast('-123' as BIGINT)", "-123");
  testExpression("cast(DECIMAL '1234567890.123' as BIGINT)", "1234567890");
  testExpression("cast('t' as BOOLEAN)", "true");
  testExpression("cast('f' as BOOLEAN)", "false");
  testExpression("cast('1' as BOOLEAN)", "true");
  testExpression("cast('0' as BOOLEAN)", "false");
  testExpression("cast(-123.456E0 as VARCHAR)", "'-123.456'");
  testExpression("cast(cast('abcxyz' as VARCHAR) as VARCHAR)", "'abcxyz'");
}

/// Test to validate the body of lambda expression is constant folded.
TEST_F(ExprOptimizerTest, lambdaConstantFolding) {
  auto type = ROW({"a"}, {BIGINT()});
  testExpression(
      "(x) -> x + (1 + 2)", "(x) -> x + 3", type, {BIGINT()}, {BIGINT()});
  testExpression(
      "(x) -> abs(x + (1 + 2))",
      "(x) -> abs(x + 3)",
      type,
      {BIGINT()},
      {BIGINT()});
  testExpression(
      "(x) -> 1 * (4 / 2)", "(x) -> 2", type, {BIGINT()}, {BIGINT()});

  type = ROW({"a", "b"}, {BIGINT(), BIGINT()});
  testExpression(
      "(x, y) -> (x + (3 - 2)) * (abs(y) * (4 / 2))",
      "(x, y) -> (x + 1) * (abs(y) * 2)",
      type,
      {BIGINT(), BIGINT()},
      {BIGINT(), BIGINT()});
  testExpression(
      "(x, y) -> (x + (3 - 2)) * (abs(y) * (4 / 2))",
      "(x, y) -> (x + 1) * (abs(y) * 2)",
      type,
      {BIGINT(), BIGINT()},
      {BIGINT(), BIGINT()});

  testExpression(
      "array_sort(c0, x -> (3 * 2))",
      "array_sort(c0, x -> 6)",
      ROW({"c0"}, {ARRAY(VARCHAR())}),
      {ARRAY(VARCHAR())},
      {ARRAY(VARCHAR())});
  testExpression(
      "(x) -> length(x) + (1 + 2)",
      "(x) -> length(x) + 3",
      ROW({"a"}, {VARCHAR()}),
      {VARCHAR()},
      {VARCHAR()});
  testExpression(
      "(x, y) -> cast((length(x) + (3 - 2)) AS INTEGER) * (abs(y) * (4 / 2))",
      "(x, y) -> cast((length(x) + 1) AS INTEGER) * (abs(y) * 2)",
      ROW({"a", "b"}, {VARCHAR(), INTEGER()}),
      {VARCHAR(), INTEGER()},
      {VARCHAR(), INTEGER()});
}

/// Test for partial constant folding in the expression tree.
TEST_F(ExprOptimizerTest, partialConstantFolding) {
  testExpression(
      "a = 'z' and b = (1 + 1)",
      "a = 'z' and b = 2",
      ROW({"a", "b"}, {VARCHAR(), BIGINT()}));
  testExpression(
      "concat(substr(a, 1 + 5 - 2), substr(b, 2 * 3))",
      "concat(substr(a, 4), substr(b, 6))",
      ROW({"a", "b"}, {VARCHAR(), VARCHAR()}));
  testExpression(
      "a = 'z' and b = (1 + 1) or c = (5 - 1) * 3",
      "a = 'z' and b = 2 or c = 12",
      ROW({"a", "b", "c"}, {VARCHAR(), BIGINT(), INTEGER()}));
  testExpression(
      "strpos(substr(a, (1 + 5 - 2)), substr(b, 2 * 3)) + c - (4 - 1)",
      "strpos(substr(a, 4), substr(b, 6)) + c - 3",
      ROW({"a", "b", "c"}, {VARCHAR(), VARCHAR(), INTEGER()}));

  auto type = ROW({"a"}, {BIGINT()});
  testExpression("(123 * 10) + 4 = a", "1234 = a", type);
  testExpression(
      "1234 between a and 2000 + 1", "1234 between a and 2001", type);

  type = ROW({"a"}, {VARCHAR()});
  testExpression(
      "a='z' and 2 * 3 = 6 / 1 and 2 * 1 = 3 - 2 and abs(-4) = 2 * 2",
      "a='z' and true and false and true",
      type);
  testExpression(
      "a='z' or 3 - 1 = 4 - 2 or 4 - 2 = 8 / 4 or 8 / 2 = 2 * 3",
      "a='z' or true or true or false",
      type);
}

/// Test combination of expression rewrites and constant folding.
TEST_F(ExprOptimizerTest, rewritesWithConstantFolding) {
  auto type = ROW({"c0"}, {ARRAY(VARCHAR())});
  std::vector<TypePtr> lambdaTypes = {VARCHAR()};
  std::vector<TypePtr> expectedLambdaTypes = {VARCHAR()};
  testExpression(
      "array_sort(c0, x -> length(x) + (3 * 2))",
      "array_sort(c0, x -> length(x) + 6)",
      type,
      lambdaTypes,
      expectedLambdaTypes);
  testExpression(
      "array_sort(c0, x -> length(x) * abs(-1))",
      "array_sort(c0, x -> length(x) * 1)",
      type,
      lambdaTypes,
      expectedLambdaTypes);

  lambdaTypes = {VARCHAR(), VARCHAR()};
  testExpression(
      "array_sort(c0, (x, y) -> if(length(x) < length(y), (2 - 3), if(length(x) > length(y), (-2 + 3), 0 * 3)))",
      "array_sort(c0, x -> length(x))",
      type,
      lambdaTypes,
      expectedLambdaTypes);
  testExpression(
      "array_sort(c0, (x, y) -> if(length(x) < length(y), (-1 * 1), if(length(x) = length(y), abs(0), (2 / 2))))",
      "array_sort(c0, x -> length(x))",
      type,
      lambdaTypes,
      expectedLambdaTypes);
  testExpression(
      "array_sort(c0, (x, y) -> if(length(x) < length(y), abs(-1), if(length(x) > length(y), -4 / 3, 0 / 3)))",
      "array_sort_desc(c0, x -> length(x))",
      type,
      lambdaTypes,
      expectedLambdaTypes);

  type = ROW({"c0"}, {ARRAY(DOUBLE())});
  lambdaTypes = {DOUBLE(), DOUBLE()};
  testExpression(
      "reduce(c0, (89.0E0 + 11.0E0), (s, x) -> s + x * (0.2E0 - 0.1E0), s -> (s < (100.0E0 + 1.0E0)))",
      "reduce(c0, 100.0E0, (s, x) -> s + x * 0.1E0, s -> (s < 101.0E0))",
      type,
      lambdaTypes,
      {DOUBLE(), DOUBLE()});
  testExpression(
      "reduce(c0, abs(-8.0E0), (s, x) -> (s + (4.0E0 / 2.0E0)) - x, s -> s)",
      "8.0E0 + cast(array_sum_propagate_element_null(transform(c0, x -> 2.0E0 - x)) AS DOUBLE)",
      type,
      lambdaTypes,
      {DOUBLE()});

  type = ROW({"c0"}, {ARRAY(INTEGER())});
  lambdaTypes = {INTEGER(), INTEGER()};
  testExpression(
      "reduce(c0, 100 / 10, (s, x) -> s + x, s -> s + (3 - 2))",
      "reduce(c0, 10, (s, x) -> s + x, s -> s + 1)",
      type,
      lambdaTypes,
      lambdaTypes);
  testExpression(
      "reduce(c0, 8 / 2, (s, x) -> if(x % 2 = 0, s + 1, s), s -> s)",
      "4 + cast(array_sum_propagate_element_null(transform(c0, x -> if(x % 2 = 0, 1, 0))) AS BIGINT)",
      type,
      lambdaTypes,
      {INTEGER()});

  lambdaTypes = {SMALLINT(), SMALLINT()};
  testExpression(
      "reduce(c0, 1 - 1, (s, x) -> s + x, s -> coalesce(s, abs(2 - 10), 5 * 3) * (5 * 2))",
      "reduce(c0, 0, (s, x) -> s + x, s -> coalesce(s, 8, 15) * 10)",
      ROW({"c0"}, {ARRAY(SMALLINT())}),
      lambdaTypes,
      lambdaTypes);
  testExpression(
      "reduce(c0, 15 * 3, (s, x) -> s + x * abs(-2), s -> s)",
      "45 + cast(array_sum_propagate_element_null(transform(c0, x -> x * 2)) AS BIGINT)",
      ROW({"c0"}, {ARRAY(TINYINT())}),
      {TINYINT(), TINYINT()},
      {TINYINT()});
}

/// Test to ensure session queryCtx is used during expression optimization.
TEST_F(ExprOptimizerTest, queryCtx) {
  setQueryTimeZone("Pacific/Apia");
  testExpression("hour(from_unixtime(9.98489045321E8))", "3");
  testExpression("minute(from_unixtime(9.98489045321E8))", "4");

  setQueryTimeZone("America/Los_Angeles");
  testExpression("hour(from_unixtime(9.98489045321E8))", "7");
  testExpression("minute(from_unixtime(9.98489045321E8))", "4");
}

/// Test cast optimization that avoids expression evaluation when input to
/// cast is same as the type of cast expression.
TEST_F(ExprOptimizerTest, castOptimization) {
  auto testCast = [&](const TypePtr& type) {
    const auto rowType = ROW({"a"}, {type});
    if (type->isPrimitiveType()) {
      testExpression(
          fmt::format("cast(a as {})", type->toString()), "a", rowType);
    } else {
      std::vector<core::TypedExprPtr> inputs = {
          std::make_shared<core::FieldAccessTypedExpr>(type, "a")};
      const auto castExpr =
          std::make_shared<core::CastTypedExpr>(type, inputs, false);
      testExpression(castExpr, inputs.at(0), rowType);
    }
  };

  // Primitive types.
  testCast(TINYINT());
  testCast(INTEGER());
  testCast(BIGINT());
  testCast(VARCHAR());
  testCast(VARBINARY());
  testCast(DOUBLE());
  testCast(DECIMAL(5, 2));
  testCast(DECIMAL(25, 10));
  testCast(DATE());
  testCast(TIMESTAMP_WITH_TIME_ZONE());

  // Complex types.
  testCast(ARRAY(DATE()));
  testCast(MAP(DOUBLE(), VARCHAR()));
  testCast(ROW({BIGINT(), DECIMAL(5, 2), TIME()}));
}

/// Test to validate failing subexpressions are replaced with fail expression.
TEST_F(ExprOptimizerTest, failExpression) {
  // Primitive types.
  const auto divideByZero = "division by zero";
  assertFailCall(
      "0 / 0", "cast(fail(division by zero) as BIGINT)", divideByZero);
  assertFailCall(
      "CAST(4000000000 as INTEGER)",
      "cast(fail(Cannot cast BIGINT '4000000000' to INTEGER. Overflow during arithmetic conversion: ) as INTEGER)",
      "Cannot cast BIGINT '4000000000' to INTEGER. Overflow during arithmetic conversion: ");
  assertFailCall(
      "CAST(-4000000000 as INTEGER)",
      "cast(fail(Cannot cast BIGINT '-4000000000' to INTEGER. Negative overflow during arithmetic conversion: ) as INTEGER)",
      "Cannot cast BIGINT '-4000000000' to INTEGER. Negative overflow during arithmetic conversion: ");
  assertFailCall(
      "(INTEGER '400000' * INTEGER '200000')",
      "cast(fail(integer overflow: 400000 * 200000) as INTEGER)",
      "integer overflow: 400000 * 200000");

  // Partial constant folding with fail expression.
  auto type = ROW({"a"}, {BIGINT()});
  assertFailCall(
      "a + (0 / 0)",
      "plus(ROW[\"a\"],cast(fail(division by zero) as BIGINT))",
      divideByZero,
      type);
  assertFailCall(
      "if(false, 2 * 1, 0 / 0)",
      "if(false,2,cast(fail(division by zero) as BIGINT))",
      divideByZero,
      type);
  assertFailCall(
      "case true when a = 2 + 3 then 2 / 1 when 0 / 0 = 0 then 2 else 30 + 2 end",
      "switch(eq(true,eq(ROW[\"a\"],5)),2,eq(true,eq(cast(fail(division by zero) as BIGINT),0)),2,32)",
      divideByZero,
      type);
  assertFailCall(
      "CASE abs(-1234) WHEN a THEN 1 WHEN 0 / 0 THEN 2 ELSE 1 END",
      "switch(eq(1234,ROW[\"a\"]),1,eq(1234,cast(fail(division by zero) as BIGINT)),2,1)",
      divideByZero,
      type);

  type = ROW({"a"}, {BOOLEAN()});
  assertFailCall(
      "coalesce(0 / 0 > 1, a, 0 / 0 = 0)",
      "coalesce(gt(cast(fail(division by zero) as BIGINT),1),ROW[\"a\"],eq(cast(fail(division by zero) as BIGINT),0))",
      divideByZero,
      type);
  assertFailCall(
      "case when a then 10 / 2 when 0 / 0 = 0 then 2 end",
      "switch(ROW[\"a\"],5,eq(cast(fail(division by zero) as BIGINT),0),2)",
      divideByZero,
      type);
  assertFailCall(
      "case when a then 0 / 0 else 1 end",
      "if(ROW[\"a\"],cast(fail(division by zero) as BIGINT),1)",
      divideByZero,
      type);
  assertFailCall(
      "case when IF(a, true, false) then 1 else 0 / 0  end",
      "if(if(ROW[\"a\"],true,false),1,cast(fail(division by zero) as BIGINT))",
      divideByZero,
      type);

  type = ROW({"a"}, {JSON()});
  assertFailCall(
      "json_array_get(a, 1 / 0)",
      "json_array_get(ROW[\"a\"],cast(fail(division by zero) as BIGINT))",
      divideByZero,
      type);
  type = ROW({"a", "b"}, {JSON(), VARCHAR()});
  assertFailCall(
      "json_extract(a, substr(b, 1 / 0))",
      "json_extract(ROW[\"a\"],substr(ROW[\"b\"],cast(fail(division by zero) as BIGINT)))",
      divideByZero,
      type);
  assertFailCall(
      "json_extract_scalar(a, substring(b, 1 / 0))",
      "json_extract_scalar(ROW[\"a\"],substring(ROW[\"b\"],cast(fail(division by zero) as BIGINT)))",
      divideByZero,
      type);

  // Complex types.
  type = ROW({"a"}, {ARRAY(BIGINT())});
  assertFailCall(
      "transform(a, x -> x / 0)",
      "transform(ROW[\"a\"],lambda ROW<x:BIGINT> -> divide(ROW[\"x\"],0))",
      divideByZero,
      type,
      {BIGINT()});
  assertFailCall(
      "filter(a, x -> (x / 0) > 1)",
      "filter(ROW[\"a\"],lambda ROW<x:BIGINT> -> gt(divide(ROW[\"x\"],0),1))",
      divideByZero,
      type,
      {BIGINT()});

  type = ROW({"a"}, {MAP(VARCHAR(), BIGINT())});
  assertFailCall(
      "map_top_n(a, 1 / 0)",
      "map_top_n(ROW[\"a\"],cast(fail(division by zero) as BIGINT))",
      divideByZero,
      type);
  assertFailCall(
      "map_top_n_keys(a, 1 / 0)",
      "map_top_n_keys(ROW[\"a\"],cast(fail(division by zero) as BIGINT))",
      divideByZero,
      type);
}

} // namespace
} // namespace facebook::velox::expression
