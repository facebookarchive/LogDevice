# Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

set(FOLLY_ROOT_DIR ${LOGDEVICE_DIR}/external/folly)

include(ExternalProject)

if(thriftpy3)
  set(_folly_cmake_extra_opts "-DPYTHON_EXTENSIONS=True")
endif()


ExternalProject_Add(folly
    SOURCE_DIR "${FOLLY_ROOT_DIR}"
    DOWNLOAD_COMMAND ""
    CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE=True -DCXX_STD=gnu++17
       -DCMAKE_CXX_STANDARD=17
       ${_folly_cmake_extra_opts}
    INSTALL_COMMAND make install DESTDIR=${LOGDEVICE_STAGING_DIR}
    )

ExternalProject_Get_Property(folly SOURCE_DIR)
ExternalProject_Get_Property(folly BINARY_DIR)

set(FOLLY_LIBRARIES
    ${BINARY_DIR}/libfolly.a)
set(FOLLY_BENCHMARK_LIBRARIES
    ${BINARY_DIR}/folly/libfollybenchmark.a)
set(FOLLY_TEST_UTIL_LIBRARIES
    ${BINARY_DIR}/libfolly_test_util.a)

set(FOLLY_INCLUDE_DIR ${SOURCE_DIR})
message(STATUS "Folly Library: ${FOLLY_LIBRARIES}")
message(STATUS "Folly Benchmark: ${FOLLY_BENCHMARK_LIBRARIES}")
message(STATUS "Folly Includes: ${FOLLY_INCLUDE_DIR}")

mark_as_advanced(
    FOLLY_ROOT_DIR
    FOLLY_LIBRARIES
    FOLLY_BENCHMARK_LIBRARIES
    FOLLY_INCLUDE_DIR
)
