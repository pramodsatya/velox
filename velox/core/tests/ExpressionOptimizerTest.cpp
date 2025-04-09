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
#include "velox/core/ExpressionOptimizer.h"
#include "velox/core/Expressions.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

namespace facebook::velox::core::test {

class ExpressionOptimizerTest : public testing::Test,
                           public velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  ExpressionOptimizerTest() {}

  core::TypedExprPtr testExpression(const std::string& input,
                                    const RowTypePtr& inputType,
                                    const std::string& expected,
                                    const RowTypePtr& expectedType) {
    auto makeTypedExpr = [&](const std::string& expression,
                             const RowTypePtr& type) {
      auto untyped = parse::parseExpr(expression, {});
      return core::Expressions::inferTypes(untyped, type, execCtx_->pool());
    };

    auto optimizedInput = optimizeExpression(makeTypedExpr(input, inputType), queryCtx_, pool());
    auto optimizedExpected = optimizeExpression(makeTypedExpr(expected, expectedType), queryCtx_, pool());
    ASSERT(optimizedExpected->equals(*optimizedInput));
  }

  std::shared_ptr<core::QueryCtx> queryCtx_{velox::core::QueryCtx::create()};
  std::unique_ptr<core::ExecCtx> execCtx_{
      std::make_unique<core::ExecCtx>(pool_.get(), queryCtx_.get())};
};

TEST_F(ExpressionOptimizerTest, conditional) {

}

TEST_F(ExpressionOptimizerTest, conjunct) {

}

TEST_F(ExpressionOptimizerTest, constantFolding) {

}

} // namespace facebook::velox::core::test
