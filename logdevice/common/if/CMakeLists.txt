# Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

include_directories(${CMAKE_BINARY_DIR})
link_directories("${CMAKE_BINARY_DIR}/staging/usr/local/lib")

set(
  _common_if_include_prefix
  "logdevice/common/if"
 )

file(
  MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/${_common_if_include_prefix}
 )

ld_thrift_py3_library(
  "common"
  ""
  "json"
  "${CMAKE_CURRENT_SOURCE_DIR}"
  "${CMAKE_BINARY_DIR}/${_common_if_include_prefix}"
  "${_common_if_include_prefix}"
  PY3_NAMESPACE "logdevice"
 )

add_dependencies(common-cpp2-target fbthrift)

set_target_properties(
        common-cpp2-obj
        PROPERTIES POSITION_INDEPENDENT_CODE True
)

target_link_libraries(
  common-cpp2
  ${THRIFT_DEPS}
)
