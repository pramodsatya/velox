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

#include "velox/experimental/cudf/expression/AstUtils.h"
#include "velox/experimental/cudf/expression/JitCustomOps.h"

#include "velox/functions/lib/TimeUtils.h"

#include <cmath>
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

// A call of the device function `symbol` in JitCustomOpsDevice.cu.
JitCustomCall makeCall(
    const char* symbol,
    std::vector<core::TypedExprPtr> arguments,
    cudf::type_id outputType) {
  return JitCustomCall{
      cudf::ast::jit::device_binary{
          jitCustomOpsFragment(), cudf::lto_binary_type::FATBIN, symbol},
      std::move(arguments),
      cudf::data_type{outputType}};
}

std::optional<int32_t> constantIntegerValue(const core::TypedExprPtr& expr) {
  if (!expr->isConstantKind() || expr->type()->kind() != TypeKind::INTEGER) {
    return std::nullopt;
  }
  const auto* constant = expr->asUnchecked<core::ConstantTypedExpr>();
  if (constant->isNull()) {
    return std::nullopt;
  }
  if (constant->hasValueVector()) {
    return constant->valueVector()->as<SimpleVector<int32_t>>()->valueAt(0);
  }
  return constant->value().value<TypeKind::INTEGER>();
}

// year, month, quarter or week of a DATE, which return BIGINT.
JitCustomOpLowering lowerDateField(const char* symbol) {
  return [symbol](
             const core::CallTypedExpr& call) -> std::optional<JitCustomCall> {
    const auto& inputs = call.inputs();
    if (inputs.size() != 1 || !inputs[0]->type()->isDate() ||
        call.type()->kind() != TypeKind::BIGINT) {
      return std::nullopt;
    }
    return makeCall(symbol, {inputs[0]}, cudf::type_id::INT64);
  };
}

// date_trunc of a DATE to a week, month, quarter or year. Truncating to a day
// is the identity, which DateTruncFunction handles.
std::optional<JitCustomCall> lowerDateTrunc(const core::CallTypedExpr& call) {
  const auto& inputs = call.inputs();
  if (inputs.size() != 2 || !inputs[1]->type()->isDate()) {
    return std::nullopt;
  }
  const auto unitString = constantVarcharValue(inputs[0]);
  if (!unitString.has_value()) {
    return std::nullopt;
  }
  const auto unit =
      functions::fromDateTimeUnitString(*unitString, /*throwIfInvalid=*/false);
  if (!unit.has_value()) {
    return std::nullopt;
  }
  const char* symbol = nullptr;
  switch (*unit) {
    case functions::DateTimeUnit::kWeek:
      symbol = "velox_date_trunc_week_date";
      break;
    case functions::DateTimeUnit::kMonth:
      symbol = "velox_date_trunc_month_date";
      break;
    case functions::DateTimeUnit::kQuarter:
      symbol = "velox_date_trunc_quarter_date";
      break;
    case functions::DateTimeUnit::kYear:
      symbol = "velox_date_trunc_year_date";
      break;
    default:
      return std::nullopt;
  }
  return makeCall(symbol, {inputs[1]}, cudf::type_id::TIMESTAMP_DAYS);
}

// round of a DOUBLE to a constant number of decimals. Like RoundFunction, it
// computes the factor 10^decimals on the host.
std::optional<JitCustomCall> lowerRound(const core::CallTypedExpr& call) {
  const auto& inputs = call.inputs();
  if (inputs.empty() || inputs.size() > 2 ||
      inputs[0]->type()->kind() != TypeKind::DOUBLE ||
      call.type()->kind() != TypeKind::DOUBLE) {
    return std::nullopt;
  }
  int32_t decimals = 0;
  if (inputs.size() == 2) {
    const auto value = constantIntegerValue(inputs[1]);
    if (!value.has_value()) {
      return std::nullopt;
    }
    decimals = *value;
  }
  return makeCall(
      "velox_round_double",
      {inputs[0],
       std::make_shared<core::ConstantTypedExpr>(INTEGER(), Variant(decimals)),
       std::make_shared<core::ConstantTypedExpr>(
           DOUBLE(), Variant(std::pow(10.0, static_cast<double>(decimals))))},
      cudf::type_id::FLOAT64);
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

void registerPrestoJitCustomOps(const std::string& prefix) {
  registerJitCustomOp(prefix + "year", lowerDateField("velox_year_date"));
  registerJitCustomOp(prefix + "month", lowerDateField("velox_month_date"));
  registerJitCustomOp(prefix + "quarter", lowerDateField("velox_quarter_date"));
  registerJitCustomOp(prefix + "week", lowerDateField("velox_week_date"));
  registerJitCustomOp(
      prefix + "week_of_year", lowerDateField("velox_week_date"));
  registerJitCustomOp(prefix + "date_trunc", lowerDateTrunc);
  registerJitCustomOp(prefix + "round", lowerRound);
}

} // namespace facebook::velox::cudf_velox
