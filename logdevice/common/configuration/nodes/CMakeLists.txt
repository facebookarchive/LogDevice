# Copyright (c) 2018-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

include_directories(${CMAKE_BINARY_DIR})

set(
  _nodesconfiguration_if_include_prefix
  "logdevice/common/configuration/nodes"
 )

file(
  MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/${_nodesconfiguration_if_include_prefix}
 )

thrift_library(
  "NodesConfiguration"
  ""
  "cpp2"
  "json"
  "${CMAKE_CURRENT_SOURCE_DIR}"
  "${CMAKE_BINARY_DIR}/${_nodesconfiguration_if_include_prefix}"
  "${_nodesconfiguration_if_include_prefix}"
  THRIFT_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/.."
 )

add_dependencies(NodesConfiguration-cpp2-target fbthrift)
add_dependencies(NodesConfiguration-cpp2-target Membership-cpp2-target)

set_target_properties(NodesConfiguration-cpp2-obj
  PROPERTIES
    POSITION_INDEPENDENT_CODE True
)

target_link_libraries(
  NodesConfiguration-cpp2
  Membership-cpp2
  ${THRIFT_DEPS}
)
