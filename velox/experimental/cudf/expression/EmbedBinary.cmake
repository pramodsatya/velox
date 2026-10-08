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

# Writes the bytes of INPUT to OUTPUT as a C++ array named SYMBOL in namespace
# facebook::velox::cudf_velox, with its size as SYMBOL##Size. Run as a script:
#   cmake -DINPUT=... -DOUTPUT=... -DSYMBOL=... -P EmbedBinary.cmake
file(READ "${INPUT}" content HEX)
string(LENGTH "${content}" hexLength)
math(EXPR size "${hexLength} / 2")
string(REGEX REPLACE "([0-9a-f][0-9a-f])" "0x\\1," bytes "${content}")
file(
  WRITE
  "${OUTPUT}"
  "// Generated from ${INPUT} by EmbedBinary.cmake.\n"
  "#include <cstddef>\n#include <cstdint>\n\n"
  "namespace facebook::velox::cudf_velox {\n"
  "extern const uint8_t ${SYMBOL}[] = {${bytes}};\n"
  "extern const size_t ${SYMBOL}Size = ${size};\n"
  "} // namespace facebook::velox::cudf_velox\n"
)
