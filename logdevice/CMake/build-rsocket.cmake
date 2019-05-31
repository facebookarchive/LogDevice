# Copyright (c) 2017-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

include(ExternalProject)

ExternalProject_Add(rsocket
    GIT_REPOSITORY https://github.com/rsocket/rsocket-cpp.git
    GIT_TAG master
    # Disable configure, build and install steps: we just want the source
    PREFIX "${CMAKE_CURRENT_BINARY_DIR}"
    SOURCE_DIR "${CMAKE_CURRENT_BINARY_DIR}/external/rsocket-cpp"
    CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE=True
        -DCMAKE_PREFIX_PATH=${LOGDEVICE_STAGING_DIR}/usr/local
        -DBUILD_TESTS=OFF
        -DBUILD_BENCHMARKS=OFF
        -DBUILD_EXAMPLES=OFF
        -DCMAKE_CXX_STANDARD=17
        -DCXX_STD=gnu++17
    INSTALL_COMMAND make install DESTDIR=${LOGDEVICE_STAGING_DIR}
    )

# Specify include dir
ExternalProject_Get_Property(rsocket SOURCE_DIR)
ExternalProject_Get_Property(rsocket BINARY_DIR)
ExternalProject_Add_StepDependencies(rsocket configure fizz folly fmt)

set(RSOCKET_LIBRARIES
  ${BINARY_DIR}/libReactiveSocket.a
  ${BINARY_DIR}/yarpl/libyarpl.a
)

message(STATUS "Rsocket Library: ${RSOCKET_LIBRARIES}")

mark_as_advanced(
  RSOCKET_LIBRARIES
)
