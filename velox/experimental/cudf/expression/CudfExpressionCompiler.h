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

#pragma once

#include "velox/experimental/cudf/expression/ExpressionEvaluator.h"

namespace facebook::velox::core {
class QueryCtx;
} // namespace facebook::velox::core

namespace facebook::velox::cudf_velox {

/// Compiler that transforms TypedExpr trees into CudfExpressions by
/// selecting the best evaluator for each sub-tree.
///
/// A single compiler instance is constructed per compilation scope (e.g. one
/// operator initialization) and may be used to compile multiple expressions.
///
/// The compile() method:
///   1. Optimizes the expression (constant folding / rewrites) when QueryCtx
///      and MemoryPool are available.
///   2. Selects the best evaluator for the root expression.
///   3. Creates the evaluator.  Each evaluator handles its own sub-tree
///      compilation internally (e.g. FunctionExpression recursively calls
///      createCudfExpression for children, AST calls compileSubExpression).
///
/// Usage:
///   CudfExpressionCompiler compiler(schema, queryCtx, pool);
///   auto filter = compiler.compile(filterExpr);
///   auto proj   = compiler.compile(projectExpr);
class CudfExpressionCompiler {
 public:
  /// Construct a compiler for the given input schema.
  ///
  /// @param inputRowSchema  The schema of the input row.
  /// @param queryCtx        Query context for optimization. May be nullptr to
  ///                        skip optimization (e.g. test utilities).
  /// @param pool            Memory pool for optimization. May be nullptr.
  CudfExpressionCompiler(
      RowTypePtr inputRowSchema,
      core::QueryCtx* queryCtx = nullptr,
      memory::MemoryPool* pool = nullptr);

  /// Compile a single expression.
  ///
  /// If queryCtx and pool were provided at construction, the expression is
  /// optimized (constant folding / rewrites) before compilation.  The
  /// optimized expression is retained and accessible via optimizedExpr().
  std::shared_ptr<CudfExpression> compile(const core::TypedExprPtr& expr);

  /// The optimized expression from the most recent compile() call.
  /// Callers that need to walk the expression tree after compilation (e.g.
  /// CudfHashJoin building a two-table AST) should use this rather than the
  /// original expression to benefit from constant folding.
  const core::TypedExprPtr& optimizedExpr() const {
    return optimizedExpr_;
  }

  const RowTypePtr& inputRowSchema() const {
    return schema_;
  }

 private:
  RowTypePtr schema_;
  core::QueryCtx* queryCtx_;
  memory::MemoryPool* pool_;

  /// The optimized expression from the most recent compile() call.
  core::TypedExprPtr optimizedExpr_;
};

} // namespace facebook::velox::cudf_velox
