# Copyright (c) 2019-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

include_directories(${CMAKE_BINARY_DIR})
link_directories("${CMAKE_BINARY_DIR}/staging/usr/local/lib")

set(
  _checkpointing_if_include_prefix
  "logdevice/lib/checkpointing/if"
 )

file(
  MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/${_checkpointing_if_include_prefix}
 )

 ld_thrift_py3_library(
   "Checkpoint"
   ""
   "json"
   "${CMAKE_CURRENT_SOURCE_DIR}"
   "${CMAKE_BINARY_DIR}/${_checkpointing_if_include_prefix}"
   "${_checkpointing_if_include_prefix}"
   PY3_NAMESPACE "logdevice/checkpointing"
  )

add_dependencies(Checkpoint-cpp2-target fbthrift)

set_target_properties(
        Checkpoint-cpp2-obj
        PROPERTIES POSITION_INDEPENDENT_CODE True
)

target_link_libraries(
  Checkpoint-cpp2
  ${THRIFT_DEPS}
)
