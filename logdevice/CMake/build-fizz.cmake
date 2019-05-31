# Copyright (c) 2018-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

set(FIZZ_ROOT_DIR ${LOGDEVICE_DIR}/external/fizz/fizz)

include(ExternalProject)

ExternalProject_Add(fizz
    SOURCE_DIR "${FIZZ_ROOT_DIR}"
    DOWNLOAD_COMMAND ""
    CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE=True
        -DCMAKE_PREFIX_PATH=${LOGDEVICE_STAGING_DIR}/usr/local
        -DBUILD_TESTS=OFF
    INSTALL_COMMAND make install DESTDIR=${LOGDEVICE_STAGING_DIR}
    )

ExternalProject_Get_Property(fizz BINARY_DIR)

ExternalProject_Add_StepDependencies(fizz configure folly)

set(FIZZ_LIBRARIES
    ${BINARY_DIR}/lib/libfizz.a)

message(STATUS "Fizz Library: ${FIZZ_LIBRARIES}")
message(STATUS "Fizz Benchmark: ${FIZZ_BENCHMARK_LIBRARIES}")
message(STATUS "Fizz Includes: ${FIZZ_INCLUDE_DIR}")

mark_as_advanced(
    FIZZ_ROOT_DIR
    FIZZ_LIBRARIES
    FIZZ_INCLUDE_DIR
)
