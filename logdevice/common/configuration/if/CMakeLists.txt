# Copyright (c) 2018-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

include_directories(${CMAKE_BINARY_DIR})
link_directories("${CMAKE_BINARY_DIR}/staging/usr/local/lib")

set(
  _configuration_if_include_prefix
  "logdevice/common/configuration/if"
 )

file(
  MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/${_configuration_if_include_prefix}
 )

ld_thrift_py3_library(
  "AllReadStreamsDebugConfig"
  ""
  "json"
  "${CMAKE_CURRENT_SOURCE_DIR}"
  "${CMAKE_BINARY_DIR}/${_configuration_if_include_prefix}"
  "${_configuration_if_include_prefix}"
  PY3_NAMESPACE "logdevice/all_read_streams_debug_config"
 )

add_dependencies(AllReadStreamsDebugConfig-cpp2-target fbthrift)

set_target_properties(
  AllReadStreamsDebugConfig-cpp2-obj
  PROPERTIES POSITION_INDEPENDENT_CODE True
)

target_link_libraries(
  AllReadStreamsDebugConfig-cpp2
  ${THRIFT_DEPS}
)
