# Copyright (c) 2018-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

include_directories(${CMAKE_BINARY_DIR})

set(
  _membership_if_include_prefix
  "logdevice/common/membership"
 )

file(
  MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/${_membership_if_include_prefix}
 )

thrift_library(
  "Membership"
  ""
  "cpp2"
  ""
  "${CMAKE_CURRENT_SOURCE_DIR}"
  "${CMAKE_BINARY_DIR}/${_membership_if_include_prefix}"
  "${_membership_if_include_prefix}"
  THRIFT_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/.."
 )

add_dependencies(Membership-cpp2-target fbthrift)

set_target_properties(
        Membership-cpp2-obj
        PROPERTIES POSITION_INDEPENDENT_CODE True
)

target_link_libraries(
  Membership-cpp2
  ${FBTHRIFT_LIBRARIES}
  ${GLOG_LIBRARIES}
  ${LIBGFLAGS_LIBRARY}
)
