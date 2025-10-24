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
#include "velox/core/Expressions.h"
#include "velox/core/QueryCtx.h"

namespace facebook::velox::expression {

/// Optimizes expression through a combination of constant folding and rewrites;
/// all possible subtrees of the expression are constant folded first in a
/// bottom up manner, then the folded expression is rewritten. If an exception
/// (i.e VeloxUserError) is encountered during optimization of any subtree of
/// the expression and:
///  1. If 'failResult' is 'true', the failing subexpression is replaced with a
///     fail expression of UNKNOWN type and containing the error message from
///     VeloxUserError, so evaluation of this expression will fail.
///  2. If 'failResult' is 'false', the exception is ignored and the failing
///     subexpression is left unchanged in the expression tree.
core::TypedExprPtr optimize(
    const core::TypedExprPtr& expr,
    core::QueryCtx* queryCtx,
    memory::MemoryPool* pool,
    bool failResult);
} // namespace facebook::velox::expression
