# Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

include_directories(${CMAKE_BINARY_DIR})
link_directories("${CMAKE_BINARY_DIR}/staging/usr/local/lib")

set(
  _epochstore_if_include_prefix
  "logdevice/server/epoch_store/if"
 )

file(
  MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/${_epochstore_if_include_prefix}
 )

ld_thrift_py3_library(
  "EpochStore"
  ""
  "json"
  "${CMAKE_CURRENT_SOURCE_DIR}"
  "${CMAKE_BINARY_DIR}/${_epochstore_if_include_prefix}"
  "${_epochstore_if_include_prefix}"
 )

add_dependencies(EpochStore-cpp2-target fbthrift common-cpp2)

set_target_properties(
        EpochStore-cpp2-obj
        PROPERTIES POSITION_INDEPENDENT_CODE True
)

target_link_libraries(
  EpochStore-cpp2
  common-cpp2
  ${THRIFT_DEPS}
)
