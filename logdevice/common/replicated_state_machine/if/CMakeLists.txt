# Copyright (c) 2019-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

include_directories(${CMAKE_BINARY_DIR})
link_directories("${CMAKE_BINARY_DIR}/staging/usr/local/lib")

set(
  _rsm_if_include_prefix
  "logdevice/common/replicated_state_machine/if"
 )

file(
  MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/${_rsm_if_include_prefix}
 )

thrift_library(
   "KeyValueStore"
   ""
   "cpp2"
   "json"
   "${CMAKE_CURRENT_SOURCE_DIR}"
   "${CMAKE_BINARY_DIR}/${_rsm_if_include_prefix}"
   "${_rsm_if_include_prefix}"
   THRIFT_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/.."
  )

add_dependencies(KeyValueStore-cpp2-target fbthrift)

set_target_properties(
        KeyValueStore-cpp2-obj
        PROPERTIES POSITION_INDEPENDENT_CODE True
)

target_link_libraries(
  KeyValueStore-cpp2
  ${THRIFT_DEPS}
)
