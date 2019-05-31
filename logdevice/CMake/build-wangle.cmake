# Copyright (c) 2018-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

set(WANGLE_ROOT_DIR ${LOGDEVICE_DIR}/external/wangle/wangle)

include(ExternalProject)

ExternalProject_Add(wangle
    SOURCE_DIR "${WANGLE_ROOT_DIR}"
    DOWNLOAD_COMMAND ""
    CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE=True
        -DCMAKE_PREFIX_PATH=${LOGDEVICE_STAGING_DIR}/usr/local
        -DBUILD_TESTS=OFF
    INSTALL_COMMAND make install DESTDIR=${LOGDEVICE_STAGING_DIR}
)

ExternalProject_Get_Property(wangle BINARY_DIR)

ExternalProject_Add_StepDependencies(wangle configure fizz folly)

set(WANGLE_LIBRARIES
    ${BINARY_DIR}/lib/libwangle.a)

message(STATUS "Wangle Library: ${WANGLE_LIBRARIES}")

mark_as_advanced(
    WANGLE_ROOT_DIR
    WANGLE_LIBRARIES
    WANGLE_BENCHMARK_LIBRARIES
    WANGLE_INCLUDE_DIR
)
