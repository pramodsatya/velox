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

#include "velox/experimental/cudf/expression/CudfExpressionCompiler.h"
#include "velox/experimental/cudf/expression/ExpressionEvaluatorRegistry.h"

#include "velox/expression/ExprOptimizer.h"

namespace facebook::velox::cudf_velox {

CudfExpressionCompiler::CudfExpressionCompiler(
    RowTypePtr inputRowSchema,
    core::QueryCtx* queryCtx,
    memory::MemoryPool* pool)
    : schema_(std::move(inputRowSchema)),
      queryCtx_(queryCtx),
      pool_(pool) {}

std::shared_ptr<CudfExpression> CudfExpressionCompiler::compile(
    const core::TypedExprPtr& expr) {
  if (pool_) {
    // Fold constants (e.g. cast-of-literal) at the TypedExpr level so that
    // evaluator constructors see simple ConstantTypedExpr nodes.  This mirrors
    // how the CPU expression compilation path optimizes during
    // ExprCompiler::compile().  queryCtx_ may be nullptr (e.g. connector
    // context) — expression::optimize() handles that gracefully.
    optimizedExpr_ = expression::optimize(expr, queryCtx_, pool_);
  } else {
    optimizedExpr_ = expr;
  }

  // Select the best evaluator and create the expression.  Each evaluator
  // handles its own sub-tree compilation internally.
  const auto* best = findBestEvaluator(optimizedExpr_);
  VELOX_CHECK_NOT_NULL(
      best,
      "No cuDF expression evaluator can handle: {}",
      optimizedExpr_->toString());
  return best->create(optimizedExpr_, schema_);
}

} // namespace facebook::velox::cudf_velox
