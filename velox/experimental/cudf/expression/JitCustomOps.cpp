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

#include "velox/experimental/cudf/expression/JitCustomOps.h"

#include <unordered_map>

namespace facebook::velox::cudf_velox {

// Generated from JitCustomOpsDevice.cu by EmbedBinary.cmake.
extern const uint8_t kJitCustomOpsFragment[];
extern const size_t kJitCustomOpsFragmentSize;

namespace {

std::unordered_map<std::string, JitCustomOpLowering>& jitCustomOps() {
  static std::unordered_map<std::string, JitCustomOpLowering> ops;
  return ops;
}

} // namespace

std::span<const uint8_t> jitCustomOpsFragment() {
  return {kJitCustomOpsFragment, kJitCustomOpsFragmentSize};
}

void registerJitCustomOp(
    const std::string& name,
    JitCustomOpLowering lowering) {
  jitCustomOps()[name] = std::move(lowering);
}

std::optional<JitCustomCall> lowerJitCustomOp(const core::TypedExprPtr& expr) {
  if (!expr->isCallKind()) {
    return std::nullopt;
  }
  const auto* call = expr->asUnchecked<core::CallTypedExpr>();
  const auto it = jitCustomOps().find(call->name());
  if (it == jitCustomOps().end()) {
    return std::nullopt;
  }
  return it->second(*call);
}

void unregisterJitCustomOps() {
  jitCustomOps().clear();
}

} // namespace facebook::velox::cudf_velox
