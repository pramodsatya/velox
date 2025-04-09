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

#include <set>

#include "velox/core/Expressions.h"
#include "velox/core/QueryCtx.h"

namespace facebook::velox::core {

using ExpressionOptimization = std::function<core::TypedExprPtr(
    const core::TypedExprPtr,
    const std::shared_ptr<core::QueryCtx>,
    memory::MemoryPool*)>;

/// Returns all registered expression optimizations.
std::vector<ExpressionOptimization>& getExpressionOptimizations();

/// Register expression optimizations for AND, OR, IF, COALESCE. Register
/// additional expression optimizations from arguments if specified.
void registerExpressionOptimizations(
    const std::vector<ExpressionOptimization>& customOptimizations = {});

/// Apply registered expression optimizations and constant fold subtrees of the
/// input expression. Return the optimized expression.
TypedExprPtr optimizeExpression(
    const core::TypedExprPtr& expr,
    const std::shared_ptr<core::QueryCtx>& queryCtx,
    memory::MemoryPool* pool);
} // namespace facebook::velox::core
