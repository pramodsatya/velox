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

#include "velox/core/Expressions.h"

#include <cudf/ast/jit/udf.hpp>
#include <cudf/types.hpp>

#include <functional>
#include <optional>
#include <string>
#include <vector>

namespace facebook::velox::cudf_velox {

/// A call of Velox device code that the JIT evaluator makes from the kernel of
/// the expression around it (CudfConfig::jitCustomOpsEnabled), through
/// cudf::ast::jit::call.
struct JitCustomCall {
  /// The device function.
  cudf::ast::jit::device_binary function;

  /// The device function's arguments, in order: inputs of the call, or
  /// constants computed while lowering it.
  std::vector<core::TypedExprPtr> arguments;

  /// Type of the value the device function writes.
  cudf::data_type outputType;
};

/// Lowers a call of the function the op is registered for, or returns nullopt
/// for a call the op does not implement, which the other evaluators then
/// handle.
using JitCustomOpLowering =
    std::function<std::optional<JitCustomCall>(const core::CallTypedExpr&)>;

/// Registers a JIT custom op for the function `name`, prefix included.
void registerJitCustomOp(const std::string& name, JitCustomOpLowering lowering);

/// Lowers `expr` to a JIT custom op call, or returns nullopt when it is not a
/// call that a registered op implements.
std::optional<JitCustomCall> lowerJitCustomOp(const core::TypedExprPtr& expr);

void unregisterJitCustomOps();

} // namespace facebook::velox::cudf_velox
