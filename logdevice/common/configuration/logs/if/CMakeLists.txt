# Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

include_directories(${CMAKE_BINARY_DIR})
link_directories("${CMAKE_BINARY_DIR}/staging/usr/local/lib")

set(
  _logsconfig_if_include_prefix
  "logdevice/logsconfig"
 )

file(
  MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/${_logsconfig_if_include_prefix}
 )

ld_thrift_py3_library(
  "logsconfig"
  ""
  "json"
  "${CMAKE_CURRENT_SOURCE_DIR}"
  "${CMAKE_BINARY_DIR}/${_logsconfig_if_include_prefix}"
  "${_logsconfig_if_include_prefix}"
  PY3_NAMESPACE "logdevice"
  CYTHON_INCLUDES
    "${CMAKE_BINARY_DIR}/logdevice/common/gen-py3/"
 )

add_dependencies(logsconfig-cpp2-target fbthrift)

set_target_properties(
        logsconfig-cpp2-obj
        PROPERTIES POSITION_INDEPENDENT_CODE True
)

target_link_libraries(
  logsconfig-cpp2
  common-cpp2
  ${THRIFT_DEPS}
)
