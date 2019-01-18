# Copyright (c) 2018-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

include_directories(${CMAKE_BINARY_DIR})

set(_adminapi_if_include_prefix "logdevice/admin/if")

add_subdirectory(common/fb303/if)

file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/${_adminapi_if_include_prefix})

foreach(THRIFT_SOURCE
        common
        exceptions
        settings
        logtree
        nodes
        safety
        maintenance
)
  thrift_generate(
    ${THRIFT_SOURCE}
    ""
    "cpp2"
    ""
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${CMAKE_BINARY_DIR}/${_adminapi_if_include_prefix}"
    "${_adminapi_if_include_prefix}"
   )
   add_dependencies(${THRIFT_SOURCE}-cpp2-target fbthrift)
endforeach()

thrift_library(
  "admin"
  "AdminAPI"
  "cpp2"
  ""
  "${CMAKE_CURRENT_SOURCE_DIR}"
  "${CMAKE_BINARY_DIR}/${_adminapi_if_include_prefix}"
  "${_adminapi_if_include_prefix}"
 )
add_dependencies(admin-cpp2-target fbthrift)

add_dependencies(admin-cpp2-target
                 common-cpp2-target
                 nodes-cpp2-target
                 fb303-cpp2-target
                 exceptions-cpp2-target
                 logtree-cpp2-target
                 safety-cpp2-target
                 maintenance-cpp2-target
                 settings-cpp2-target)
