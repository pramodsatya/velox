# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_library(velox_cudf_expression_registry ExpressionEvaluatorRegistry.cpp)

target_link_libraries(
  velox_cudf_expression_registry
  PUBLIC cudf::cudf
  PRIVATE velox_exception velox_common_base
)

add_library(
  velox_cudf_expression
  ArrayAccessFunctions.cpp
  AstExpression.cpp
  CommonFunctions.cpp
  DateTruncFunction.cpp
  DecimalExpressionKernels.cpp
  DecimalExpressionKernelsGpu.cu
  ExpressionEvaluator.cpp
  JitCustomOps.cpp
  JitExpression.cpp
  NullMask.cpp
  PrestoFunctions.cpp
  prestosql/DateAddFunction.cpp
  prestosql/DatePlusIntervalFunction.cpp
  SparkFunctions.cpp
  sparksql/DateAddFunction.cpp
  sparksql/HashFunction.cpp
  sparksql/SubStringFunction.cpp
  SubfieldFiltersToAst.cpp
)

target_link_libraries(
  velox_cudf_expression
  PUBLIC cudf::cudf
  PRIVATE
    arrow
    velox_common_base
    velox_cudf_expression_registry
    velox_cudf_vector
    velox_exception
    velox_expression
)

target_compile_options(velox_cudf_expression PRIVATE -Wno-missing-field-initializers)

# The JIT custom ops' device code, compiled ahead of time into a fatbin carrying
# LTO-IR and embedded in velox_cudf_expression (jitCustomOpsFragment()). It
# targets the lowest configured architecture; nvJitLink generates the code for
# the device the kernel runs on.
set(_velox_cudf_archs ${CMAKE_CUDA_ARCHITECTURES})
if(_velox_cudf_archs STREQUAL "native")
  set(_velox_cudf_archs ${CMAKE_CUDA_ARCHITECTURES_NATIVE})
elseif(_velox_cudf_archs STREQUAL "all")
  set(_velox_cudf_archs ${CMAKE_CUDA_ARCHITECTURES_ALL})
elseif(_velox_cudf_archs STREQUAL "all-major")
  set(_velox_cudf_archs ${CMAKE_CUDA_ARCHITECTURES_ALL_MAJOR})
endif()
set(_velox_cudf_lto_arch)
foreach(_velox_cudf_arch IN LISTS _velox_cudf_archs)
  string(REGEX MATCH "^[0-9]+" _velox_cudf_arch_number "${_velox_cudf_arch}")
  if(
    _velox_cudf_arch_number
    AND (NOT _velox_cudf_lto_arch OR _velox_cudf_arch_number LESS _velox_cudf_lto_arch)
  )
    set(_velox_cudf_lto_arch ${_velox_cudf_arch_number})
  endif()
endforeach()
if(NOT _velox_cudf_lto_arch)
  message(
    FATAL_ERROR
    "Cannot pick an LTO-IR architecture for the JIT custom ops from "
    "CMAKE_CUDA_ARCHITECTURES=${CMAKE_CUDA_ARCHITECTURES}"
  )
endif()
add_library(velox_cudf_jit_custom_ops_device OBJECT JitCustomOpsDevice.cu)
set_target_properties(
  velox_cudf_jit_custom_ops_device
  PROPERTIES CUDA_FATBIN_COMPILATION ON CUDA_ARCHITECTURES OFF
)
target_compile_options(
  velox_cudf_jit_custom_ops_device
  PRIVATE -dlto -arch=compute_${_velox_cudf_lto_arch} -code=lto_${_velox_cudf_lto_arch}
)
target_link_libraries(velox_cudf_jit_custom_ops_device PRIVATE cudf::cudf)
set(_velox_cudf_jit_custom_ops_fragment ${CMAKE_CURRENT_BINARY_DIR}/JitCustomOpsFragment.cpp)
add_custom_command(
  OUTPUT ${_velox_cudf_jit_custom_ops_fragment}
  COMMAND
    ${CMAKE_COMMAND} -DINPUT=$<TARGET_OBJECTS:velox_cudf_jit_custom_ops_device>
    -DOUTPUT=${_velox_cudf_jit_custom_ops_fragment} -DSYMBOL=kJitCustomOpsFragment -P
    ${CMAKE_CURRENT_SOURCE_DIR}/EmbedBinary.cmake
  DEPENDS
    velox_cudf_jit_custom_ops_device
    $<TARGET_OBJECTS:velox_cudf_jit_custom_ops_device>
    ${CMAKE_CURRENT_SOURCE_DIR}/EmbedBinary.cmake
  VERBATIM
)
target_sources(velox_cudf_expression PRIVATE ${_velox_cudf_jit_custom_ops_fragment})
