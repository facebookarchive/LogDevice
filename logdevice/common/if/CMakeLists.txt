# Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

include_directories(${CMAKE_BINARY_DIR})
link_directories("${CMAKE_BINARY_DIR}/staging/usr/local/lib")

set(_common_if_inc_prefix "logdevice/common/if")
set(_common_binary_dir "${CMAKE_BINARY_DIR}/${_common_if_inc_prefix}")
add_subdirectory(for_open_source/common/fb303/if)

file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/${_common_if_inc_prefix})

# Common lib
ld_thrift_py3_library(
  "common"
  ""
  "json"
  "${CMAKE_CURRENT_SOURCE_DIR}"
  "${_common_binary_dir}"
  "${_common_if_inc_prefix}"
  PY3_NAMESPACE "logdevice"
 )

add_dependencies(common-cpp2-target fbthrift)
set_target_properties(common-cpp2-obj PROPERTIES POSITION_INDEPENDENT_CODE True)
target_link_libraries(common-cpp2 ${THRIFT_DEPS})

thrift_library(
  "payload"
  ""
  "cpp2"
  ""
  "${CMAKE_CURRENT_SOURCE_DIR}"
  "${_common_binary_dir}"
  "${_common_if_inc_prefix}"
  THRIFT_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/.."
 )

 add_dependencies(payload-cpp2-target fbthrift)

 set_target_properties(
         payload-cpp2-obj
         PROPERTIES POSITION_INDEPENDENT_CODE True
 )

# API Model thrift
thrift_library(
  "ApiModel"
  ""
  "cpp2"
  "json"
  "${CMAKE_CURRENT_SOURCE_DIR}"
  "${_common_binary_dir}"
  "${_common_if_inc_prefix}"
  THRIFT_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/.."
 )
add_dependencies(ApiModel-cpp2-target fbthrift)
set_target_properties(
         ApiModel-cpp2-obj
         PROPERTIES POSITION_INDEPENDENT_CODE True
)
target_link_libraries(ApiModel-cpp2 ${THRIFT_DEPS})

# API Service thrift
thrift_library(
  "ApiService"
  "LogDeviceAPI"
  "cpp2"
  "json"
  "${CMAKE_CURRENT_SOURCE_DIR}"
  "${_common_binary_dir}"
  "${_common_if_inc_prefix}"
  THRIFT_INCLUDE_DIRECTORIES
    "${CMAKE_SOURCE_DIR}/.."
    "${FB303_INCLUDE_DIR}"
 )
add_dependencies(ApiService-cpp2-target fbthrift)
set_target_properties(
         ApiService-cpp2-obj
         PROPERTIES POSITION_INDEPENDENT_CODE True
)
target_link_libraries(
  ApiService-cpp2
  ApiModel-cpp2
  ${THRIFT_DEPS}
  "fb303-cpp2"
)

# Setup language specific dependencies
add_dependencies(ApiService-cpp2-target fb303-cpp2-target)

target_link_libraries(
  common-cpp2
  payload-cpp2
  ${THRIFT_DEPS}
)

foreach(_lang ${_languages})
  add_dependencies(common-${_lang}-target fb303-${_lang}-target)
endforeach()
