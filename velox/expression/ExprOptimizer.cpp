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
#include "velox/expression/ExprOptimizer.h"
#include "velox/expression/Expr.h"
#include "velox/expression/ExprConstants.h"
#include "velox/expression/ExprOptimizer.h"
#include "velox/expression/ExprRewriteRegistry.h"
#include "velox/expression/ExprUtils.h"

namespace facebook::velox::expression {

namespace {

const std::vector<core::TypedExprPtr> getFoldedInputs(
    const core::TypedExprPtr& expr,
    core::QueryCtx* queryCtx,
    memory::MemoryPool* pool,
    bool replaceEvalErrorWithFailExpr) {
  std::vector<core::TypedExprPtr> foldedInputs;
  for (const auto& input : expr->inputs()) {
    foldedInputs.push_back(
        optimize(input, queryCtx, pool, replaceEvalErrorWithFailExpr));
  }
  return foldedInputs;
}

// Helper function to constant fold the expression and return a fail expression
// in case constant folding throws an exception.
core::TypedExprPtr tryConstantFold(
    const core::TypedExprPtr& expr,
    core::QueryCtx* queryCtx,
    memory::MemoryPool* pool,
    bool replaceEvalErrorWithFailExpr) {
  try {
    if (auto results =
            exec::tryEvaluateConstantExpression(expr, pool, queryCtx, false)) {
      return std::make_shared<core::ConstantTypedExpr>(results);
    }
    // Return the expression unevaluated.
    return expr;
  } catch (VeloxUserError& e) {
    if (replaceEvalErrorWithFailExpr) {
      return std::make_shared<core::CallTypedExpr>(
          UNKNOWN(),
          kFail,
          std::make_shared<core::ConstantTypedExpr>(VARCHAR(), e.what()));
    }
    return expr;
  }
}
} // namespace

core::TypedExprPtr optimize(
    const core::TypedExprPtr& expr,
    core::QueryCtx* queryCtx,
    memory::MemoryPool* pool,
    bool replaceEvalErrorWithFailExpr) {
  LOG(ERROR) << expr->toString() << " kind " << expr->kind();
  if (expr->isConstantKind()) {
    return expr;
  } else if (expr->isFieldAccessKind()) {
    return expr;
  }

  core::TypedExprPtr result;
  if (utils::allConstantExpr(expr->inputs())) {
    LOG(ERROR) << expr->toString() << " kind " << expr->kind();
    result =
        tryConstantFold(expr, queryCtx, pool, replaceEvalErrorWithFailExpr);
    LOG(ERROR) << result->toString() << " kind " << result->kind();
  } else if (
      const auto rewritten =
          ExprRewriteRegistry::instance().rewrite(expr, true)) {
    result = rewritten;
  } else {
    if (expr->isCallKind()) {
      const auto inputs =
          getFoldedInputs(expr, queryCtx, pool, replaceEvalErrorWithFailExpr);
      if (utils::allConstantExpr(inputs)) {
        result =
            tryConstantFold(expr, queryCtx, pool, replaceEvalErrorWithFailExpr);
      } else {
        const auto* callExpr = expr->asUnchecked<core::CallTypedExpr>();
        result = std::make_shared<core::CallTypedExpr>(
            callExpr->type(), inputs, callExpr->name());
      }
    } else if (expr->isCastKind()) {
      const auto inputs =
          getFoldedInputs(expr, queryCtx, pool, replaceEvalErrorWithFailExpr);
      if (utils::allConstantExpr(inputs)) {
        result =
            tryConstantFold(expr, queryCtx, pool, replaceEvalErrorWithFailExpr);
      } else {
        const auto* castExpr = expr->asUnchecked<core::CastTypedExpr>();
        result = std::make_shared<core::CastTypedExpr>(
            expr->type(), inputs, castExpr->isTryCast());
      }
    } else if (expr->isLambdaKind()) {
      const auto* lambdaExpr = expr->asUnchecked<core::LambdaTypedExpr>();
      const auto foldedBody = optimize(
          lambdaExpr->body(), queryCtx, pool, replaceEvalErrorWithFailExpr);
      result = std::make_shared<core::LambdaTypedExpr>(
          lambdaExpr->signature(), foldedBody);
    } else {
      result = expr;
    }
  }

  LOG(ERROR) << result->toString() << " kind " << result->kind();
  return result;
}
} // namespace facebook::velox::expression
