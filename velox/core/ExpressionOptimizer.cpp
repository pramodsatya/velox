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
#include "velox/expression/Expr.h"

namespace facebook::velox::core {
namespace {
/// Comparator for core::TypedExprPtr; used to deduplicate arguments to
/// COALESCE special form expression.
struct TypedExprComparator {
  bool operator()(const core::TypedExprPtr& a, const core::TypedExprPtr& b)
      const {
    return a->hash() < b->hash();
  }
};

core::TypedExprPtr tryConstantFold(
    const core::TypedExprPtr& expr,
    const std::shared_ptr<core::QueryCtx>& queryCtx,
    memory::MemoryPool* pool) {
  try {
    auto result = exec::evaluateConstantExpression(expr, pool, queryCtx);
    return std::make_shared<core::ConstantTypedExpr>(result);
  } catch (VeloxUserError& e) {
    const auto error = std::string(e.what());
    // References to variables will not be resolved so this error is expected.
    if (error.find("Field not found") != std::string::npos) {
      return expr;
    } else {
      return std::make_shared<core::CallTypedExpr>(
          VARCHAR(),
          std::vector<core::TypedExprPtr>(
              {std::make_shared<core::ConstantTypedExpr>(VARCHAR(), e.what())}),
          "fail");
    }
  }
}

core::TypedExprPtr constantFold(
    const core::TypedExprPtr& expr,
    const std::shared_ptr<core::QueryCtx>& queryCtx,
    memory::MemoryPool* pool) {
  core::TypedExprPtr result;
  std::vector<core::TypedExprPtr> foldedInputs;
  for (auto& input : expr->inputs()) {
    foldedInputs.push_back(constantFold(input, queryCtx, pool));
  }

  bool isField = false;
  if (auto callExpr =
          std::dynamic_pointer_cast<const core::CallTypedExpr>(expr)) {
    result = std::make_shared<core::CallTypedExpr>(
        callExpr->type(), foldedInputs, callExpr->name());
  } else if (
      auto castExpr =
          std::dynamic_pointer_cast<const core::CastTypedExpr>(expr)) {
    VELOX_CHECK(!foldedInputs.empty());
    if (foldedInputs.at(0)->type() == expr->type()) {
      result = foldedInputs.at(0);
    } else {
      result = std::make_shared<core::CastTypedExpr>(
          expr->type(), foldedInputs, castExpr->nullOnFailure());
    }
  } else if (
      auto constantExpr =
          std::dynamic_pointer_cast<const core::ConstantTypedExpr>(expr)) {
    return constantExpr;
  } else if (
      auto field =
          std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(expr)) {
    isField = true;
    result = field;
  } else if (
      auto concatExpr =
          std::dynamic_pointer_cast<const core::ConcatTypedExpr>(expr)) {
    result = concatExpr;
  } else if (
      auto derefExpr =
          std::dynamic_pointer_cast<const core::DereferenceTypedExpr>(expr)) {
    auto inputExpr = derefExpr->inputs().at(0);
    auto idx = derefExpr->index();
    if (auto callExprInput =
            std::dynamic_pointer_cast<const core::CallTypedExpr>(inputExpr)) {
      VELOX_CHECK_EQ(callExprInput->name(), "row_constructor");
      result = inputExpr->inputs().at(idx);
    } else {
      result = inputExpr;
    }
  } else {
    result = expr;
  }

  auto folded = !isField ? tryConstantFold(result, queryCtx, pool) : result;
  return folded;
}

core::TypedExprPtr optimizeIfExpression(
    const core::TypedExprPtr& input,
    const std::shared_ptr<core::QueryCtx>& queryCtx,
    memory::MemoryPool* pool) {
  auto expr = std::dynamic_pointer_cast<const core::CallTypedExpr>(input);
  if (expr == nullptr || expr->name() != "if" || expr->inputs().size() != 3) {
    return nullptr;
  }

  auto condition = expr->inputs().at(0);
  auto folded = constantFold(condition, queryCtx, pool);

  if (auto constantExpr =
          std::dynamic_pointer_cast<const core::ConstantTypedExpr>(folded)) {
    if (auto constVector = constantExpr->toConstantVector(pool)) {
      if (constVector->isNullAt(0) ||
          constVector->as<ConstantVector<bool>>()->valueAt(0)) {
        return expr->inputs().at(1);
      }
      return expr->inputs().at(2);
    }
  }
  return expr;
}

template <bool isAnd>
core::TypedExprPtr optimizeConjunctExpression(
    const core::TypedExprPtr& input,
    const std::shared_ptr<core::QueryCtx>& queryCtx,
    memory::MemoryPool* pool) {
  auto expr = std::dynamic_pointer_cast<const core::CallTypedExpr>(input);
  constexpr auto expectedName = isAnd ? "and" : "or";
  if (expr == nullptr || expr->name() != expectedName) {
    return nullptr;
  }

  bool allInputsConstant = true;
  bool hasNullInput = false;
  std::vector<core::TypedExprPtr> optimizedInputs;
  core::TypedExprPtr nullInput = nullptr;
  for (const auto& inputExpr : expr->inputs()) {
    auto folded = constantFold(inputExpr, queryCtx, pool);
    if (auto constantExpr =
            std::dynamic_pointer_cast<const core::ConstantTypedExpr>(folded)) {
      auto constantVector = constantExpr->toConstantVector(pool);
      if (!constantVector->isNullAt(0)) {
        if constexpr (isAnd) {
          // AND (.., false, ..) -> false
          if (!constantVector->as<ConstantVector<bool>>()->valueAt(0)) {
            return constantExpr;
          }
        } else {
          // OR (.., true, ..) -> true
          if (constantVector->as<ConstantVector<bool>>()->valueAt(0)) {
            return constantExpr;
          }
        }
      } else if (!hasNullInput) {
        hasNullInput = true;
        nullInput = inputExpr;
      }
    } else {
      allInputsConstant = false;
      optimizedInputs.push_back(inputExpr);
    }
  }

  if (allInputsConstant && hasNullInput) {
    return nullInput;
  } else if (optimizedInputs.empty()) {
    return expr->inputs().front();
  } else if (optimizedInputs.size() == 1) {
    return optimizedInputs.front();
  }
  return std::make_shared<core::CallTypedExpr>(
      expr->type(), optimizedInputs, expr->name());
}

core::TypedExprPtr addCoalesceArgument(
    const core::TypedExprPtr& input,
    std::set<core::TypedExprPtr, TypedExprComparator>& optimizedTypedExprs,
    std::vector<core::TypedExprPtr>& deduplicatedInputs,
    const std::shared_ptr<core::QueryCtx>& queryCtx,
    memory::MemoryPool* pool) {
  auto folded = constantFold(input, queryCtx, pool);
  // First non-NULL constant input to COALESCE returns non-NULL value.
  if (auto constantExpr =
          std::dynamic_pointer_cast<const core::ConstantTypedExpr>(folded)) {
    auto constantVector = constantExpr->toConstantVector(pool);
    if (!constantVector->isNullAt(0)) {
      if (optimizedTypedExprs.find(folded) == optimizedTypedExprs.end()) {
        optimizedTypedExprs.insert(folded);
        deduplicatedInputs.push_back(input);
      }
      return input;
    }
  } else if (optimizedTypedExprs.find(folded) == optimizedTypedExprs.end()) {
    optimizedTypedExprs.insert(folded);
    deduplicatedInputs.push_back(input);
  }

  return nullptr;
}

core::TypedExprPtr optimizeCoalesceSpecialFormImpl(
    const core::CallTypedExprPtr& expr,
    std::set<core::TypedExprPtr, TypedExprComparator>& inputTypedExprSet,
    std::vector<core::TypedExprPtr>& deduplicatedInputs,
    const std::shared_ptr<core::QueryCtx>& queryCtx,
    memory::MemoryPool* pool) {
  // Once a constant input is seen, subsequent inputs to the COALESCE expression
  // can be ignored.
  for (const auto& input : expr->inputs()) {
    if (const auto call =
            std::dynamic_pointer_cast<const core::CallTypedExpr>(input)) {
      if (call->name() == "coalesce") {
        // If the argument is a COALESCE expression, the arguments of inner
        // COALESCE can be combined with the arguments of outer COALESCE
        // expression. If the inner COALESCE has a constant expression, return.
        if (auto optimizedCoalesceSubExpr = optimizeCoalesceSpecialFormImpl(
                call, inputTypedExprSet, deduplicatedInputs, queryCtx, pool)) {
          return optimizedCoalesceSubExpr;
        }
      } else if (
          auto optimized = addCoalesceArgument(
              input, inputTypedExprSet, deduplicatedInputs, queryCtx, pool)) {
        return optimized;
      }
    } else if (
        auto optimized = addCoalesceArgument(
            input, inputTypedExprSet, deduplicatedInputs, queryCtx, pool)) {
      return optimized;
    }
  }
  // Return null if COALESCE has no constant input.
  return nullptr;
}

core::TypedExprPtr optimizeCoalesceExpression(
    const core::TypedExprPtr& expr,
    const std::shared_ptr<core::QueryCtx>& queryCtx,
    memory::MemoryPool* pool) {
  auto call = std::dynamic_pointer_cast<const core::CallTypedExpr>(expr);
  if (call == nullptr || call->name() != "coalesce") {
    return nullptr;
  }
  // Deduplicate inputs to COALESCE and remove NULL inputs, returning a list of
  // optimized inputs to COALESCE.
  std::set<core::TypedExprPtr, TypedExprComparator> inputTypedExprSet;
  std::vector<core::TypedExprPtr> deduplicatedInputs;
  optimizeCoalesceSpecialFormImpl(
      call, inputTypedExprSet, deduplicatedInputs, queryCtx, pool);

  // Return NULL if all inputs to COALESCE are NULL. If there is a single input
  // to COALESCE after optimization, return this expression. Otherwise, return
  // COALESCE expression with optimized inputs.
  if (deduplicatedInputs.empty()) {
    return call->inputs().front();
  } else if (deduplicatedInputs.size() == 1) {
    return deduplicatedInputs.front();
  }
  return std::make_shared<core::CallTypedExpr>(
      call->type(), deduplicatedInputs, call->name());
}
} // namespace

std::vector<ExpressionOptimization>& getExpressionOptimizations() {
  static std::vector<ExpressionOptimization> rewrites;
  return rewrites;
}

void registerExpressionOptimizations(
    const std::vector<ExpressionOptimization>& customOptimizations) {
  auto& expressionOptimizations = getExpressionOptimizations();
  expressionOptimizations.emplace_back(
      [&](const core::TypedExprPtr& expr,
          const std::shared_ptr<core::QueryCtx>& queryCtx,
          memory::MemoryPool* pool) {
        return optimizeCoalesceExpression(expr, queryCtx, pool);
      });
  expressionOptimizations.emplace_back(
      [&](const core::TypedExprPtr& expr,
          const std::shared_ptr<core::QueryCtx>& queryCtx,
          memory::MemoryPool* pool) {
        return optimizeIfExpression(expr, queryCtx, pool);
      });
  expressionOptimizations.emplace_back(
      [&](const core::TypedExprPtr& expr,
          const std::shared_ptr<core::QueryCtx>& queryCtx,
          memory::MemoryPool* pool) {
        return optimizeConjunctExpression<true>(expr, queryCtx, pool);
      });
  expressionOptimizations.emplace_back(
      [&](const core::TypedExprPtr& expr,
          const std::shared_ptr<core::QueryCtx>& queryCtx,
          memory::MemoryPool* pool) {
        return optimizeConjunctExpression<false>(expr, queryCtx, pool);
      });

  // Register custom expression optimizations.
  for (const auto& optimization : customOptimizations) {
    expressionOptimizations.emplace_back(optimization);
  }
}

TypedExprPtr optimizeExpression(
    const core::TypedExprPtr& expr,
    const std::shared_ptr<core::QueryCtx>& queryCtx,
    memory::MemoryPool* pool) {
  auto result = expr;
  auto optimizations = getExpressionOptimizations();
  for (auto& optimize : optimizations) {
    if (auto optimized = optimize(result, queryCtx, pool)) {
      result = optimized;
    }
  }

  return constantFold(result, queryCtx, pool);
}
} // namespace facebook::velox::core
