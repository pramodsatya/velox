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
#include <fmt/format.h>
#include <gtest/gtest.h>

#include "velox/expression/Expr.h"
#include "velox/expression/ExprOptimizer.h"
#include "velox/expression/ExprRewriteRegistry.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::expression {
namespace {

class ExprOptimizerTest : public testing::Test,
                          public velox::test::VectorTestBase {
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

  void testExpression(
      const std::string& expression,
      const std::string& expected,
      const RowTypePtr& type = ROW({})) {
    testExpression(expression, expected, type, type);
  }

  void testExpression(
      const std::string& expression,
      const RowTypePtr& type = ROW({})) {
    testExpression(expression, expression, type);
  }

  /// TODO: Add UNKNOWN to other type casts: @czentgr
  ///  Compare expr eval results and converted expr shape after casts added.
  void assertFailCall(
      const std::string& expression,
      const std::string& expected,
      const RowTypePtr& type = ROW({})) {
    auto optimized = optimize(expression, type, true);
    ASSERT_EQ(optimized->toString(), expected);
  }

  void setQueryTimeZone(const std::string& timeZone) {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kSessionTimezone, timeZone},
         {core::QueryConfig::kAdjustTimestampToTimezone, "true"}});
  }

 private:
  core::TypedExprPtr makeTypedExpr(
      const std::string& expression,
      const RowTypePtr& type) {
    auto untyped = parse::parseExpr(expression, options_);
    return core::Expressions::inferTypes(untyped, type, execCtx_->pool());
  }

  VectorPtr evaluate(const core::TypedExprPtr& expr, const RowVectorPtr& data) {
    auto exprSet = exec::ExprSet({expr}, execCtx_.get());
    exec::EvalCtx context(execCtx_.get(), &exprSet, data.get());
    SelectivityVector rows(data->size());
    std::vector<VectorPtr> result(1);
    exprSet.eval(rows, context, result);

    return result[0];
  }

  core::TypedExprPtr optimize(
      const std::string& expression,
      const RowTypePtr& type = ROW({}),
      bool replaceEvalErrorWithFailExpr = false) {
    const auto expr = makeTypedExpr(expression, type);
    return expression::optimize(
        expr, queryCtx_.get(), pool(), replaceEvalErrorWithFailExpr);
  }

  void testExpression(
      const std::string& input,
      const std::string& expected,
      const RowTypePtr& inputType,
      const RowTypePtr& expectedType) {
    // test shape
    auto optimized = optimize(input, inputType);
    auto expectedExpr = makeTypedExpr(expected, expectedType);
    LOG(ERROR) << "Good input " << input;
    if (*optimized != *expectedExpr) {
      LOG(ERROR) << "Input " << input << " Optimized " << optimized->toString()
                 << " " << optimized->type()->toString() << " Expected "
                 << expectedExpr->toString() << " "
                 << expectedExpr->type()->toString();
    }
    ASSERT_TRUE(*optimized == *expectedExpr);

    SCOPED_TRACE(fmt::format(
        "Input: {}\nOptimized: {}\nExpected: {}",
        input,
        optimized->toString(),
        expected));

    const auto optimizedResult =
        evaluate(optimized, makeRowVector(inputType, {}));
    const auto expectedResult =
        evaluate(expectedExpr, makeRowVector(expectedType, {}));
  }

  std::shared_ptr<core::QueryCtx> queryCtx_{core::QueryCtx::create()};
  std::unique_ptr<core::ExecCtx> execCtx_{
      std::make_unique<core::ExecCtx>(pool_.get(), queryCtx_.get())};
  parse::ParseOptions options_;
};

TEST_F(ExprOptimizerTest, abs) {
  testExpression("abs(-5)", "5");
  testExpression("abs(-10-5)", "15");
  testExpression("abs(-1234 + 1)", "1233");
  testExpression("abs(-1234 + 1)", "1233");
  testExpression("abs(-1234 + BIGINT '1')", "1233");
  testExpression("abs(-1234)", "1234");
  testExpression("abs(a)", "abs(a)", ROW({"a"}, {BIGINT()}));
  testExpression("abs(a + 1)", "abs(a + 1)", ROW({"a"}, {BIGINT()}));
}

TEST_F(ExprOptimizerTest, arrayConstructor) {
  const auto type = ROW({"a"}, {BIGINT()});
  testExpression(
      "ARRAY [(a + 0), (a + 1), (a + 2)]",
      "array_constructor((a + 0), (a + 1), (a + 2))",
      type);
  testExpression(
      "ARRAY [(1234 + 0), (a + 1), (1234 + 2)]",
      "array_constructor(1234, (a + 1), 1236)",
      type);
  testExpression(
      "ARRAY [(1234 + 0), (a + 1), cast(null AS BIGINT)]",
      "array_constructor(1234, (a + 1), cast(null AS BIGINT))",
      type);
}

TEST_F(ExprOptimizerTest, between) {
  testExpression("3 between 2 and 4", "true");
  testExpression("2 between 3 and 4", "false");
  testExpression(
      "cast(null AS BIGINT) between 2 and 4", "cast(null AS BOOLEAN)");
  testExpression(
      "3 between cast(null AS BIGINT) and 4", "cast(null AS BOOLEAN)");
  testExpression(
      "3 between 2 and cast(null AS BIGINT)", "cast(null AS BOOLEAN)");

  testExpression("'cc' between 'b' and 'd'", "true");
  testExpression("'b' between 'cc' and 'd'", "false");
  testExpression(
      "cast(null AS VARCHAR) between 'b' and 'd'", "cast(null AS BOOLEAN)");
  testExpression(
      "'cc' between cast(null AS VARCHAR) and 'd'", "cast(null AS BOOLEAN)");
  testExpression(
      "'cc' between 'b' and cast(null AS VARCHAR)", "cast(null AS BOOLEAN)");

  testExpression("1234 between 1000 and 2000", "true");
  testExpression("1234 between 3 and 4", "false");
  testExpression("1234 between 1000 and 2000", "true");
  testExpression("1234 between 3 and 4", "false");
  testExpression("1234 between 1234 and (1234 + 1)", "true");
  testExpression("'hello' between 'e' and 'i'", "true");
  testExpression("'hello' between 'a' and 'b'", "false");

  testExpression(
      "1234 between a and 2000 + 1",
      "1234 between a and 2001",
      ROW({"a"}, {BIGINT()}));
  testExpression(
      "CAST('hello' AS VARCHAR) between a and 'bar'",
      "'hello' between a and 'bar'",
      ROW({"a"}, {VARCHAR()}));

  testExpression(
      "1.15 between cast(1.1 AS DECIMAL(3, 2)) and cast(1.2 AS DECIMAL(3, 2))",
      "true");
  testExpression(
      "9876543210.98745612035 between cast(9876543210.9874561203 AS DECIMAL(21,11)) and cast(9876543210.9874561204 AS DECIMAL(21,11))",
      "true");
  testExpression(
      "123.455 between cast(123.45 AS DECIMAL(6,3)) and cast(123.46 AS DECIMAL(6,3))",
      "true");
  testExpression(
      "cast(123.455 AS DECIMAL(6, 3)) between cast(123.45 AS DECIMAL(6, 3)) and cast(123.46 AS DECIMAL(6, 3))",
      "true");
}

TEST_F(ExprOptimizerTest, castToBigint) {
  // bigint
  testExpression("cast(0 as BIGINT)", "0");
  testExpression("cast(123 as BIGINT)", "123");
  testExpression("cast(-123 as BIGINT)", "-123");
  testExpression("cast(BIGINT '0' as BIGINT)", "0");
  testExpression("cast(BIGINT '123' as BIGINT)", "123");
  testExpression("cast(BIGINT '-123' as BIGINT)", "-123");

  // double
  testExpression("cast(123.0E0 as BIGINT)", "123");
  testExpression("cast(-123.0E0 as BIGINT)", "-123");
  testExpression("cast(123.456E0 as BIGINT)", "123");
  testExpression("cast(-123.456E0 as BIGINT)", "-123");

  // boolean
  testExpression("cast(true as BIGINT)", "1");
  testExpression("cast(false as BIGINT)", "0");

  // string
  testExpression("cast('123' as BIGINT)", "123");
  testExpression("cast('-123' as BIGINT)", "-123");

  // null
  testExpression("cast(null as BIGINT)", "cast(null AS BIGINT)");

  // decimal
  testExpression("cast(DECIMAL '1.01' as BIGINT)", "1");
  testExpression("cast(DECIMAL '7.8' as BIGINT)", "8");
  testExpression("cast(DECIMAL '1234567890.123' as BIGINT)", "1234567890");
  testExpression("cast(DECIMAL '00000000000000000000.000' as BIGINT)", "0");
}

TEST_F(ExprOptimizerTest, castToBoolean) {
  // bigint
  testExpression("cast(123 as BOOLEAN)", "true");
  testExpression("cast(-123 as BOOLEAN)", "true");
  testExpression("cast(0 as BOOLEAN)", "false");
  testExpression("cast(12300000000 as BOOLEAN)", "true");
  testExpression("cast(-12300000000 as BOOLEAN)", "true");
  testExpression("cast(BIGINT '0' as BOOLEAN)", "false");

  // boolean
  testExpression("cast(true as BOOLEAN)", "true");
  testExpression("cast(false as BOOLEAN)", "false");

  // string
  testExpression("cast('true' as BOOLEAN)", "true");
  testExpression("cast('false' as BOOLEAN)", "false");
  testExpression("cast('t' as BOOLEAN)", "true");
  testExpression("cast('f' as BOOLEAN)", "false");
  testExpression("cast('1' as BOOLEAN)", "true");
  testExpression("cast('0' as BOOLEAN)", "false");

  // null
  testExpression("cast(null as BOOLEAN)", "cast(null AS BOOLEAN)");

  // double
  testExpression("cast(123.45E0 as BOOLEAN)", "true");
  testExpression("cast(-123.45E0 as BOOLEAN)", "true");
  testExpression("cast(0.0E0 as BOOLEAN)", "false");

  // decimal
  testExpression("cast(0.00 as BOOLEAN)", "false");
  testExpression("cast(7.8 as BOOLEAN)", "true");
  testExpression("cast(12345678901234567890.123 as BOOLEAN)", "true");
  testExpression("cast(00000000000000000000.000 as BOOLEAN)", "false");
}

TEST_F(ExprOptimizerTest, castToDouble) {
  // bigint
  testExpression("cast(0 as DOUBLE)", "0.0E0");
  testExpression("cast(123 as DOUBLE)", "123.0E0");
  testExpression("cast(-123 as DOUBLE)", "-123.0E0");
  testExpression("cast(BIGINT '0' as DOUBLE)", "0.0E0");
  testExpression("cast(12300000000 as DOUBLE)", "12300000000.0E0");
  testExpression("cast(-12300000000 as DOUBLE)", "-12300000000.0E0");

  // double
  testExpression("cast(123.0E0 as DOUBLE)", "123.0E0");
  testExpression("cast(-123.0E0 as DOUBLE)", "-123.0E0");
  testExpression("cast(123.456E0 as DOUBLE)", "123.456E0");
  testExpression("cast(-123.456E0 as DOUBLE)", "-123.456E0");

  // string
  testExpression("cast('0' as DOUBLE)", "0.0E0");
  testExpression("cast('123' as DOUBLE)", "123.0E0");
  testExpression("cast('-123' as DOUBLE)", "-123.0E0");
  testExpression("cast('123.0E0' as DOUBLE)", "123.0E0");
  testExpression("cast('-123.0E0' as DOUBLE)", "-123.0E0");
  testExpression("cast('123.456E0' as DOUBLE)", "123.456E0");
  testExpression("cast('-123.456E0' as DOUBLE)", "-123.456E0");

  // null
  testExpression("cast(null as DOUBLE)", "cast(null as DOUBLE)");

  // boolean
  testExpression("cast(true as DOUBLE)", "1.0E0");
  testExpression("cast(false as DOUBLE)", "0.0E0");
}

TEST_F(ExprOptimizerTest, castToVarchar) {
  // bigint
  testExpression("cast(123 as VARCHAR)", "'123'");
  testExpression("cast(-123 as VARCHAR)", "'-123'");
  testExpression("cast(BIGINT '123' as VARCHAR)", "'123'");
  testExpression("cast(12300000000 as VARCHAR)", "'12300000000'");
  testExpression("cast(-12300000000 as VARCHAR)", "'-12300000000'");

  // double
  testExpression("cast(123.0E0 as VARCHAR)", "'123.0'");
  testExpression("cast(-123.0E0 as VARCHAR)", "'-123.0'");
  testExpression("cast(123.456E0 as VARCHAR)", "'123.456'");
  testExpression("cast(-123.456E0 as VARCHAR)", "'-123.456'");

  // boolean
  testExpression("cast(true as VARCHAR)", "'true'");
  testExpression("cast(false as VARCHAR)", "'false'");

  // string
  testExpression("cast('xyz' as VARCHAR)", "'xyz'");
  testExpression("cast(cast('abcxyz' as VARCHAR) as VARCHAR)", "'abcxyz'");

  // null
  testExpression("cast(null as VARCHAR)", "cast(null AS VARCHAR)");

  // decimal
  testExpression("cast(1.1 as VARCHAR)", "'1.1'");
}

TEST_F(ExprOptimizerTest, castOptimization) {
  testExpression("cast(a as VARCHAR)", "a", ROW({"a"}, {VARCHAR()}));
  testExpression("cast(a as INTEGER)", "a", ROW({"a"}, {INTEGER()}));
  testExpression("cast(a as BIGINT)", "a", ROW({"a"}, {BIGINT()}));
  testExpression("cast(a as DOUBLE)", "a", ROW({"a"}, {DOUBLE()}));
}

TEST_F(ExprOptimizerTest, conjunctOr) {
  testExpression("true or true", "true");
  testExpression("true or false", "true");
  testExpression("false or true", "true");
  testExpression("false or false", "false");

  testExpression("true or cast(null AS BOOLEAN)", "true");
  testExpression("cast(null AS BOOLEAN) or true", "true");
  testExpression(
      "cast(null AS BOOLEAN) or cast(null AS BOOLEAN)",
      "cast(null AS BOOLEAN)");
  testExpression("false or cast(null AS BOOLEAN)", "cast(null AS BOOLEAN)");
  testExpression("cast(null AS BOOLEAN) or false", "cast(null AS BOOLEAN)");

  testExpression(
      "a='z' or (3-1)=(4-2)", "a='z' or true", ROW({"a"}, {VARCHAR()}));
  testExpression(
      "a='z' or (3-1)=(5-4)", "a='z' or false", ROW({"a"}, {VARCHAR()}));
  testExpression(
      "(4-2)=(8/4) or a='z'", "true or a='z'", ROW({"a"}, {VARCHAR()}));
  testExpression(
      "(8/2)=(2*3) or a='z'", "false or a='z'", ROW({"a"}, {VARCHAR()}));
  testExpression(
      "a='z' or b=1+1", "a='z' or b=2", ROW({"a", "b"}, {VARCHAR(), BIGINT()}));
}

TEST_F(ExprOptimizerTest, comparison) {
  testExpression("null = null", "cast(null AS BOOLEAN)");
  testExpression("'a' = 'b'", "false");
  testExpression("'a' = 'a'", "true");
  testExpression("'a' = cast(null as varchar)", "cast(null AS BOOLEAN)");
  testExpression("cast(null as varchar) = 'a'", "cast(null AS BOOLEAN)");
  testExpression("X'a b' = X'a b'", "true");
  testExpression("X'a b' = X'a d'", "false");

  testExpression("1234 = 1234", "true");
  testExpression("1234 = 12340000000", "false");
  testExpression("1234 = BIGINT '1234'", "true");
  testExpression("1234 = 1234", "true");
  testExpression("12.34 = 12.34", "true");
  testExpression("'hello' = 'hello'", "true");
  testExpression("(123 * 10) + 4 = a", "1234 = a", ROW({"a"}, {BIGINT()}));
  testExpression("10151082135029368 = 10151082135029369", "false");

  testExpression("1.1 = 1.1", "true");
  testExpression("9876543210.9874561203 = 9876543210.9874561203", "true");
  testExpression(
      "cast((126.50 - 3.05) AS DECIMAL(5, 2)) = cast(123.45 AS DECIMAL(5, 2))",
      "true");
  testExpression(
      "cast((12345678901234567890.000 + 0.123) AS DECIMAL(23, 3)) = cast(12345678901234567890.123 AS DECIMAL(23, 3))",
      "true");
}

TEST_F(ExprOptimizerTest, dereference) {
  const auto type = ROW({"a"}, {BIGINT()});
  testExpression("ARRAY []", "ARRAY []");
  testExpression(
      "ARRAY [(a + 0), (a + 1), (a + 2)]",
      "array_constructor((a + 0), (a + 1), (a + 2))",
      type);
  testExpression(
      "ARRAY [(1234 + 0), (a + 1), (1234 + 2)]",
      "array_constructor(1234, (a + 1), 1236)",
      type);
  testExpression(
      "ARRAY [(1234 + 0), (a + 1), cast(null AS BIGINT)]",
      "array_constructor(1234, (a + 1), cast(null AS BIGINT))",
      type);
}

TEST_F(ExprOptimizerTest, failedExpressionOptimization) {
  assertFailCall("0 / 0", "cast(fail(division by zero) as BIGINT)");
  assertFailCall(
      "CAST(4000000000 as INTEGER)",
      "cast(fail(Cannot cast BIGINT '4000000000' to INTEGER. Overflow during arithmetic conversion: ) as INTEGER)");
  assertFailCall(
      "CAST(-4000000000 as INTEGER)",
      "cast(fail(Cannot cast BIGINT '-4000000000' to INTEGER. Negative overflow during arithmetic conversion: ) as INTEGER)");
  assertFailCall(
      "(INTEGER '400000' * INTEGER '200000')",
      "cast(fail(integer overflow: 400000 * 200000) as INTEGER)");
  assertFailCall(
      "case 1 when 0 / 0 then 1 when 0 / 0 then 2 else 1 end",
      "switch(eq(1,cast(fail(division by zero) as BIGINT)),1,eq(1,cast(fail(division by zero) as BIGINT)),2,1)");

  auto type = ROW({"a"}, {BIGINT()});
  assertFailCall(
      "a + (0 / 0)",
      "plus(ROW[\"a\"],cast(fail(division by zero) as BIGINT))",
      type);
  assertFailCall(
      "if(false, 1, 0 / 0)",
      "if(false,1,cast(fail(division by zero) as BIGINT))",
      type);
  assertFailCall(
      "CASE a WHEN 1 THEN 1 WHEN 0 / 0 THEN 2 END",
      "switch(eq(ROW[\"a\"],1),1,eq(ROW[\"a\"],cast(fail(division by zero) as BIGINT)),2)",
      type);
  assertFailCall(
      "case true when a = 1 then 1 when 0 / 0 = 0 then 2 else 33 end",
      "switch(eq(true,eq(ROW[\"a\"],1)),1,eq(true,eq(cast(fail(division by zero) as BIGINT),0)),2,33)",
      type);
  assertFailCall(
      "CASE 1234 WHEN a THEN 1 WHEN 0 / 0 THEN 2 ELSE 1 END",
      "switch(eq(1234,ROW[\"a\"]),1,eq(1234,cast(fail(division by zero) as BIGINT)),2,1)",
      type);

  type = ROW({"a"}, {BOOLEAN()});
  assertFailCall(
      "coalesce(0 / 0 > 1, a, 0 / 0 = 0)",
      "coalesce(gt(cast(fail(division by zero) as BIGINT),1),ROW[\"a\"],eq(cast(fail(division by zero) as BIGINT),0))",
      type);
  assertFailCall(
      "case when a then 1 when 0 / 0 = 0 then 2 end",
      "switch(ROW[\"a\"],1,eq(cast(fail(division by zero) as BIGINT),0),2)",
      type);
  assertFailCall(
      "case when a then 1 else 0 / 0  end",
      "if(ROW[\"a\"],1,cast(fail(division by zero) as BIGINT))",
      type);
  assertFailCall(
      "case when a then 0 / 0 else 1 end",
      "if(ROW[\"a\"],cast(fail(division by zero) as BIGINT),1)",
      type);
  assertFailCall(
      "CASE a WHEN true THEN 1 ELSE 0 / 0 END",
      "if(eq(ROW[\"a\"],true),1,cast(fail(division by zero) as BIGINT))",
      type);
}

TEST_F(ExprOptimizerTest, ifConditional) {
  testExpression("IF(2 = 2, 3, 4)", "3");
  testExpression("IF(1 = 2, 3, 4)", "4");
  testExpression("IF(1 = 2, BIGINT '3', 4)", "4");
  testExpression("IF(1 = 2, 3000000000, 4)", "4");

  testExpression("IF(true, 3, 4)", "3");
  testExpression("IF(false, 3, 4)", "4");
  testExpression("IF(cast(null AS BOOLEAN), 3, 4)", "4");

  testExpression("IF(true, 3, cast(null AS BIGINT))", "3");
  testExpression("IF(false, 3, cast(null AS BIGINT))", "cast(null AS BIGINT)");
  testExpression("IF(true, cast(null AS BIGINT), 4)", "cast(null AS BIGINT)");
  testExpression("IF(false, cast(null AS BIGINT), 4)", "4");
  testExpression(
      "IF(true, cast(null AS BIGINT), cast(null AS BIGINT))",
      "cast(null AS BIGINT)");
  testExpression(
      "IF(false, cast(null AS BIGINT), cast(null AS BIGINT))",
      "cast(null AS BIGINT)");

  testExpression("IF(true, 3.5E0, 4.2E0)", "3.5E0");
  testExpression("IF(false, 3.5E0, 4.2E0)", "4.2E0");

  testExpression("IF(true, 'foo', 'bar')", "'foo'");
  testExpression("IF(false, 'foo', 'bar')", "'bar'");

  testExpression("IF(true, 1.01, 1.02)", "1.01");
  testExpression("IF(false, 1.01, 1.02)", "1.02");
  testExpression(
      "IF(true, 1234567890.123, cast(1.02 AS DECIMAL(13,3)))",
      "1234567890.123");
  testExpression(
      "IF(false, cast(1.01 AS DECIMAL(13,3)), 1234567890.123)",
      "1234567890.123");
}

TEST_F(ExprOptimizerTest, isDistinctFrom) {
  testExpression(
      "cast(null AS BIGINT) is distinct from cast(null AS BIGINT)", "false");
  testExpression("3 is distinct from 4", "true");
  testExpression("3 is distinct from BIGINT '4'", "true");
  testExpression("3 is distinct from 4000000000", "true");
  testExpression("3 is distinct from 3", "false");
  testExpression("3 is distinct from cast(null AS BIGINT)", "true");
  testExpression("cast(null AS BIGINT) is distinct from 3", "true");
  testExpression(
      "10151082135029368 is distinct from 10151082135029369", "true");

  testExpression("1.1 is distinct from 1.1", "false");
  testExpression(
      "9876543210.9874561203 is distinct from cast(NULL AS DECIMAL(20, 10))",
      "true");
  testExpression(
      "cast(123.45 AS DECIMAL(5, 2)) is distinct from cast(NULL AS DECIMAL(5, 2))",
      "true");
  testExpression(
      "cast(12345678901234567890.123 AS DECIMAL(23, 3)) is distinct from cast(12345678901234567890.123 AS DECIMAL(23, 3))",
      "false");
}

TEST_F(ExprOptimizerTest, isNull) {
  testExpression("cast(null AS BIGINT) is null", "true");
  testExpression("1 is null", "false");
  testExpression("10000000000 is null", "false");
  testExpression("BIGINT '1' is null", "false");
  testExpression("1.0 is null", "false");
  testExpression("'a' is null", "false");
  testExpression("true is null", "false");
  testExpression("cast(null AS BIGINT) + 1 is null", "true");
  testExpression("a is null", "a is null", ROW({"a"}, {BIGINT()}));
  testExpression(
      "a + (1 + 1) is null", "a + 2 is null", ROW({"a"}, {BIGINT()}));
  testExpression("1.1 is null", "false");
  testExpression("9876543210.9874561203 is null", "false");
  testExpression("cast(123.45 AS DECIMAL(5, 2)) is null", "false");
  testExpression(
      "cast(12345678901234567890.123 AS DECIMAL(23, 3)) is null", "false");
}

TEST_F(ExprOptimizerTest, isNotNull) {
  testExpression("cast(null AS BIGINT) is not null", "false");
  testExpression("1 is not null", "true");
  testExpression("10000000000 is not null", "true");
  testExpression("BIGINT '1' is not null", "true");
  testExpression("1.0 is not null", "true");
  testExpression("'a' is not null", "true");
  testExpression("true is not null", "true");
  testExpression("cast(null AS BIGINT) + 1 is not null", "false");
  testExpression("a is not null", "a is not null", ROW({"a"}, {BIGINT()}));
  testExpression(
      "a + (1 + 1) is not null", "a + 2 is not null", ROW({"a"}, {BIGINT()}));
  testExpression("1.1 is not null", "true");
  testExpression("9876543210.9874561203 is not null", "true");
  testExpression("cast(123.45 AS DECIMAL(5, 2)) is not null", "true");
  testExpression(
      "cast(12345678901234567890.123 AS DECIMAL(23, 3)) is not null", "true");
}

TEST_F(ExprOptimizerTest, lambda) {
  testExpression("transform(ARRAY[1, 5], x -> x + x)");
}

TEST_F(ExprOptimizerTest, like) {
  testExpression("'a' LIKE 'a'", "true");
  testExpression("'' LIKE 'a'", "false");
  testExpression("'abc' LIKE 'a'", "false");
  testExpression("'a' LIKE '_'", "true");
  testExpression("'' LIKE '_'", "false");
  testExpression("'abc' LIKE '_'", "false");

  testExpression("'a' LIKE '%'", "true");
  testExpression("'' LIKE '%'", "true");
  testExpression("'abc' LIKE '%'", "true");
  testExpression("'abc' LIKE '___'", "true");
  testExpression("'ab' LIKE '___'", "false");
  testExpression("'abcd' LIKE '___'", "false");

  testExpression("'abc' LIKE 'abc'", "true");
  testExpression("'xyz' LIKE 'abc'", "false");
  testExpression("'abc0' LIKE 'abc'", "false");
  testExpression("'0abc' LIKE 'abc'", "false");
  testExpression("'abc' LIKE 'abc%'", "true");
  testExpression("'abc0' LIKE 'abc%'", "true");
  testExpression("'0abc' LIKE 'abc%'", "false");

  testExpression("'abc' LIKE '%abc'", "true");
  testExpression("'0abc' LIKE '%abc'", "true");
  testExpression("'abc0' LIKE '%abc'", "false");
  testExpression("'abc' LIKE '%abc%'", "true");
  testExpression("'0abc' LIKE '%abc%'", "true");
  testExpression("'abc0' LIKE '%abc%'", "true");
  testExpression("'0abc0' LIKE '%abc%'", "true");
  testExpression("'xyzw' LIKE '%abc%'", "false");

  testExpression("'abc' LIKE '%ab%c%'", "true");
  testExpression("'0abc' LIKE '%ab%c%'", "true");
  testExpression("'abc0' LIKE '%ab%c%'", "true");
  testExpression("'0abc0' LIKE '%ab%c%'", "true");
  testExpression("'ab01c' LIKE '%ab%c%'", "true");
  testExpression("'0ab01c' LIKE '%ab%c%'", "true");
  testExpression("'ab01c0' LIKE '%ab%c%'", "true");
  testExpression("'0ab01c0' LIKE '%ab%c%'", "true");
  testExpression("'xyzw' LIKE '%ab%c%'", "false");
  testExpression("cast(null AS VARCHAR) LIKE '%'", "cast(null AS BOOLEAN)");
  testExpression("'a' LIKE cast(null AS VARCHAR)", "cast(null AS BOOLEAN)");
  testExpression(
      "'a' LIKE '%' ESCAPE cast(null AS VARCHAR)", "cast(null AS BOOLEAN)");
  testExpression("'%' LIKE 'z%' ESCAPE 'z'", "true");

  // ensure regex chars are escaped
  testExpression("'' LIKE ''", "true");
  testExpression("'.*' LIKE '.*'", "true");
  testExpression("'[' LIKE '['", "true");
  testExpression("']' LIKE ']'", "true");
  testExpression("'{' LIKE '{'", "true");
  testExpression("'}' LIKE '}'", "true");
  testExpression("'?' LIKE '?'", "true");
  testExpression("'+' LIKE '+'", "true");
  testExpression("'(' LIKE '('", "true");
  testExpression("')' LIKE ')'", "true");
  testExpression("'|' LIKE '|'", "true");
  testExpression("'^' LIKE '^'", "true");
  testExpression("'$' LIKE '$'", "true");

  const auto type = ROW({"a"}, {VARCHAR()});
  testExpression("a LIKE '' ESCAPE '#'", type);
  testExpression("a LIKE 'a#__b' ESCAPE '#'", type);
  testExpression("a LIKE 'a##%b' ESCAPE '#'", type);
  testExpression("a LIKE 'abc%'", type);
  testExpression("a LIKE a ESCAPE a", type);
}

TEST_F(ExprOptimizerTest, negation) {
  testExpression("not true", "false");
  testExpression("not false", "true");
  testExpression("not cast(null AS BOOLEAN)", "cast(null AS BOOLEAN)");
  testExpression("not 1 = 1", "false");
  testExpression("not 1 = BIGINT '1'", "false");
  testExpression("not 1 != 1", "true");
  testExpression("not a = 1", "not a = 1", ROW({"a"}, {BIGINT()}));
  testExpression("not a = (1 + 1)", "not a = 2", ROW({"a"}, {BIGINT()}));
}

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

  // Ensure session timezone from queryConfig is used.
  setQueryTimeZone("Pacific/Apia");
  testExpression("hour(from_unixtime(9.98489045321E8))", "3");

  setQueryTimeZone("America/Los_Angeles");
  testExpression("hour(from_unixtime(9.98489045321E8))", "7");
}

TEST_F(ExprOptimizerTest, tryCast) {
  testExpression("try_cast(null as BIGINT)", "cast(null AS BIGINT)");
  testExpression("try_cast(123 as BIGINT)", "123");
  testExpression("try_cast(null as INTEGER)", "cast(null AS INTEGER)");
  testExpression("try_cast('foo' as VARCHAR)", "'foo'");
  testExpression("try_cast('foo' as BIGINT)", "cast(null AS BIGINT)");
  testExpression(
      "try_cast(a as BIGINT)",
      "try_cast(a as BIGINT)",
      ROW({"a"}, {DECIMAL(5, 2)}));
  testExpression(
      "try_cast('foo' as DECIMAL(2,1))", "cast(null AS DECIMAL(2,1))");
}

TEST_F(ExprOptimizerTest, subscript) {
  testExpression(
      "MAP(ARRAY [BIGINT '1', 2], ARRAY [BIGINT '3', 4])[BIGINT '1']", "3");
  testExpression(
      "MAP(ARRAY [BIGINT '1', 2], ARRAY [BIGINT '3', 4])[BIGINT '1']", "3");
  testExpression(
      "MAP(ARRAY [2, 4, BIGINT '6'], ARRAY [3, BIGINT '6', 9])[BIGINT '6']",
      "9");
  testExpression(
      "MAP(ARRAY [ARRAY[BIGINT '1', 1]], ARRAY['a'])[ARRAY[BIGINT '1', 1]]",
      "'a'");
  testExpression(
      "MAP(ARRAY [BIGINT '1', 2], ARRAY [BIGINT '3', 4])[-1]",
      "cast(null AS BIGINT)");
}

} // namespace
} // namespace facebook::velox::expression
