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
#include "velox/expression/ExprRewriteRegistry.h"

namespace facebook::velox::expression {

namespace {

// Tries constant folding expression with tryEvaluateConstantExpression API.
// If constant folding throws VeloxUserError, returns original expression when
// failResult is false, otherwise returns a fail expression.
core::TypedExprPtr tryConstantFold(
    const core::TypedExprPtr& expr,
    core::QueryCtx* queryCtx,
    memory::MemoryPool* pool,
    bool failResult) {
  try {
    if (auto results =
            exec::tryEvaluateConstantExpression(expr, pool, queryCtx, false)) {
      return std::make_shared<core::ConstantTypedExpr>(results);
    }
  } catch (VeloxUserError& e) {
    if (failResult) {
      const auto failExpr = std::make_shared<core::CallTypedExpr>(
          UNKNOWN(),
          expression::kFail,
          std::make_shared<core::ConstantTypedExpr>(VARCHAR(), e.message()));
      return std::make_shared<core::CastTypedExpr>(
          expr->type(), failExpr, false);
    }
  }
  // Return the expression unmodified.
  return expr;
}

// Optimizes all inputs to expr and returns an expression that is of the same
// kind as expr but with optimized inputs.
core::TypedExprPtr optimizeInputs(
    const core::TypedExprPtr& expr,
    core::QueryCtx* queryCtx,
    memory::MemoryPool* pool,
    bool failResult) {
  if (expr->isCallKind()) {
    std::vector<core::TypedExprPtr> optimizedInputs;
    optimizedInputs.reserve(expr->inputs().size());
    for (const auto& input : expr->inputs()) {
      optimizedInputs.push_back(optimize(input, queryCtx, pool, failResult));
    }
    const auto* callExpr = expr->asUnchecked<core::CallTypedExpr>();

    return std::make_shared<core::CallTypedExpr>(
        callExpr->type(), optimizedInputs, callExpr->name());
  }

  if (expr->isCastKind()) {
    const auto optimizedInput =
        optimize(expr->inputs().at(0), queryCtx, pool, failResult);
    const auto* castExpr = expr->asUnchecked<core::CastTypedExpr>();
    return std::make_shared<core::CastTypedExpr>(
        expr->type(), optimizedInput, castExpr->isTryCast());
  }

  if (expr->isLambdaKind()) {
    const auto* lambdaExpr = expr->asUnchecked<core::LambdaTypedExpr>();
    const auto foldedBody =
        optimize(lambdaExpr->body(), queryCtx, pool, failResult);
    return std::make_shared<core::LambdaTypedExpr>(
        lambdaExpr->signature(), foldedBody);
  }

  return expr;
}
} // namespace

core::TypedExprPtr optimize(
    const core::TypedExprPtr& expr,
    core::QueryCtx* queryCtx,
    memory::MemoryPool* pool,
    bool failResult) {
  auto result = expr;
  // cast(1 AS BIGINT) -> 1.
  // cast(a AS BIGINT) -> a ; when type(a) == BIGINT.
  // cast(concat(a, 'test') AS VARCHAR) -> concat(a, 'test') ; when type(a) ==
  //  VARCHAR.
  if (result->isCastKind() &&
      *result->type() == *result->inputs().at(0)->type()) {
    result = result->inputs().at(0);
  }
  // 1 -> 1, a -> a.
  if (result->isConstantKind() || result->isFieldAccessKind()) {
    return result;
  }

  result = optimizeInputs(result, queryCtx, pool, failResult);
  // Expressions without any arguments, such as random(), should not be constant
  // folded as they could be non-deterministic.
  bool allInputsConstant = !result->inputs().empty();
  for (const auto& input : result->inputs()) {
    if (!input->isConstantKind()) {
      allInputsConstant = false;
      break;
    }
  }

  if (allInputsConstant) {
    return tryConstantFold(result, queryCtx, pool, failResult);
  }
  return ExprRewriteRegistry::instance().rewrite(result);
}

} // namespace facebook::velox::expression
