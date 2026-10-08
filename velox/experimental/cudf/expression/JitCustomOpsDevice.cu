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

// Device code of the JIT custom ops (JitCustomOps.h). nvcc compiles it ahead of
// time into a fatbin carrying LTO-IR (see CMakeLists.txt), which a JIT kernel
// that calls the ops is linked with, so their bodies are never compiled at
// runtime. Each op is
//
//   extern "C" __device__ cudf::errc velox_<op>(R* out, A... in);
//
// as cudf::ast::jit::device_binary requires: the result through the first
// argument, the inputs by value, and an error code.

#include <cudf/errc.hpp>
#include <cudf/wrappers/timestamps.hpp>

#include <cstdint>

namespace {

__device__ int64_t floorDiv(int64_t a, int64_t b) {
  const int64_t quotient = a / b;
  return (a % b != 0 && (a < 0) != (b < 0)) ? quotient - 1 : quotient;
}

struct CivilDate {
  int64_t year;
  uint32_t month;
  uint32_t day;
};

// Days since 1970-01-01 to a proleptic Gregorian date and back, after Howard
// Hinnant's chrono-compatible date algorithms.
__device__ CivilDate civilFromDays(int64_t days) {
  days += 719468;
  const int64_t era = (days >= 0 ? days : days - 146096) / 146097;
  const auto dayOfEra = static_cast<uint32_t>(days - era * 146097);
  const uint32_t yearOfEra =
      (dayOfEra - dayOfEra / 1460 + dayOfEra / 36524 - dayOfEra / 146096) / 365;
  const uint32_t dayOfYear =
      dayOfEra - (365 * yearOfEra + yearOfEra / 4 - yearOfEra / 100);
  const uint32_t shiftedMonth = (5 * dayOfYear + 2) / 153;
  const uint32_t month =
      shiftedMonth < 10 ? shiftedMonth + 3 : shiftedMonth - 9;
  return {
      static_cast<int64_t>(yearOfEra) + era * 400 + (month <= 2 ? 1 : 0),
      month,
      dayOfYear - (153 * shiftedMonth + 2) / 5 + 1};
}

__device__ int64_t daysFromCivil(int64_t year, uint32_t month, uint32_t day) {
  year -= month <= 2 ? 1 : 0;
  const int64_t era = (year >= 0 ? year : year - 399) / 400;
  const auto yearOfEra = static_cast<uint32_t>(year - era * 400);
  const uint32_t dayOfYear =
      (153 * (month > 2 ? month - 3 : month + 9) + 2) / 5 + day - 1;
  const uint32_t dayOfEra =
      yearOfEra * 365 + yearOfEra / 4 - yearOfEra / 100 + dayOfYear;
  return era * 146097 + static_cast<int64_t>(dayOfEra) - 719468;
}

__device__ int64_t daysOf(cudf::timestamp_D date) {
  return date.time_since_epoch().count();
}

__device__ cudf::timestamp_D dateOf(int64_t days) {
  return cudf::timestamp_D{cudf::duration_D{static_cast<int32_t>(days)}};
}

// 0 for a Monday to 6 for a Sunday; 1970-01-01 was a Thursday.
__device__ int64_t daysSinceMonday(int64_t days) {
  return days + 3 - floorDiv(days + 3, 7) * 7;
}

} // namespace

// year, month, quarter and week of a DATE, as BIGINT.

extern "C" __device__ cudf::errc velox_year_date(
    int64_t* out,
    cudf::timestamp_D in) {
  *out = civilFromDays(daysOf(in)).year;
  return cudf::errc::SUCCESS;
}

extern "C" __device__ cudf::errc velox_month_date(
    int64_t* out,
    cudf::timestamp_D in) {
  *out = civilFromDays(daysOf(in)).month;
  return cudf::errc::SUCCESS;
}

extern "C" __device__ cudf::errc velox_quarter_date(
    int64_t* out,
    cudf::timestamp_D in) {
  *out = (civilFromDays(daysOf(in)).month - 1) / 3 + 1;
  return cudf::errc::SUCCESS;
}

// The ISO 8601 week: weeks start on Monday, and a week belongs to the year of
// its Thursday.
extern "C" __device__ cudf::errc velox_week_date(
    int64_t* out,
    cudf::timestamp_D in) {
  const int64_t days = daysOf(in);
  const int64_t thursday = days - daysSinceMonday(days) + 3;
  const int64_t yearStart = daysFromCivil(civilFromDays(thursday).year, 1, 1);
  *out = (thursday - yearStart) / 7 + 1;
  return cudf::errc::SUCCESS;
}

// date_trunc of a DATE: the first day of its week, which starts on Monday, its
// month, quarter or year.

extern "C" __device__ cudf::errc velox_date_trunc_week_date(
    cudf::timestamp_D* out,
    cudf::timestamp_D in) {
  const int64_t days = daysOf(in);
  *out = dateOf(days - daysSinceMonday(days));
  return cudf::errc::SUCCESS;
}

extern "C" __device__ cudf::errc velox_date_trunc_month_date(
    cudf::timestamp_D* out,
    cudf::timestamp_D in) {
  const int64_t days = daysOf(in);
  *out = dateOf(days - (civilFromDays(days).day - 1));
  return cudf::errc::SUCCESS;
}

extern "C" __device__ cudf::errc velox_date_trunc_quarter_date(
    cudf::timestamp_D* out,
    cudf::timestamp_D in) {
  const auto date = civilFromDays(daysOf(in));
  *out = dateOf(daysFromCivil(date.year, (date.month - 1) / 3 * 3 + 1, 1));
  return cudf::errc::SUCCESS;
}

extern "C" __device__ cudf::errc velox_date_trunc_year_date(
    cudf::timestamp_D* out,
    cudf::timestamp_D in) {
  *out = dateOf(daysFromCivil(civilFromDays(daysOf(in)).year, 1, 1));
  return cudf::errc::SUCCESS;
}

// Presto round(DOUBLE, INTEGER), as functions::round
// (velox/functions/prestosql/ArithmeticImpl.h), with factor = 10^decimals
// computed on the host, as RoundFunction does.
extern "C" __device__ cudf::errc velox_round_double(
    double* out,
    double number,
    int32_t decimals,
    double factor) {
  if (!isfinite(number)) {
    *out = number;
  } else if (decimals == 0) {
    *out = round(number);
  } else if (decimals < 0) {
    *out = round(number * factor) / factor;
  } else {
    const double truncated = trunc(number);
    const double fraction = number - truncated;
    if (fraction == 0.0) {
      *out = number;
    } else if (fabs(number) < 17592186044416.0) {
      // The CPU compares with the float 17592186044415.F, which is 2^44.
      *out = round(number * factor) / factor;
    } else {
      *out = truncated + round(fraction * factor) / factor;
    }
  }
  return cudf::errc::SUCCESS;
}
