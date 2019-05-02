# Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

include_directories(${CMAKE_BINARY_DIR}})

set(_fb303_if_include_prefix "common/fb303/if")

file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/${_fb303_if_include_prefix})

ld_thrift_py3_library(
  "fb303"
  "FacebookService"
  ""
  "${CMAKE_CURRENT_SOURCE_DIR}"
  "${CMAKE_BINARY_DIR}/${_fb303_if_include_prefix}"
  "${_fb303_if_include_prefix}"
)

add_dependencies(fb303-cpp2-target fbthrift)
