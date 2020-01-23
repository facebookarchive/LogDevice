# Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

set(PROMETHEUS_ROOT_DIR ${LOGDEVICE_DIR}/external/prometheus-cpp)

include(ExternalProject)

ExternalProject_Add(prometheus
    SOURCE_DIR "${PROMETHEUS_ROOT_DIR}"
    DOWNLOAD_COMMAND ""
    CMAKE_ARGS
        -DBUILD_SHARED_LIBS=OFF
        -DENABLE_TESTING=OFF
        -DENABLE_PUSH=OFF
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DCMAKE_PREFIX_PATH=${LOGDEVICE_STAGING_DIR}/usr/local	
    INSTALL_COMMAND make install DESTDIR=${LOGDEVICE_STAGING_DIR}
    )

ExternalProject_Get_Property(prometheus SOURCE_DIR)
ExternalProject_Get_Property(prometheus BINARY_DIR)

set(PROMETHEUS_LIBRARIES
    ${LOGDEVICE_STAGING_DIR}/usr/local/lib/libprometheus-cpp-core.a
    ${LOGDEVICE_STAGING_DIR}/usr/local/lib/libprometheus-cpp-pull.a)

set(PROMETHEUS_INCLUDE_DIR ${LOGDEVICE_STAGING_DIR}/usr/local/include)

message(STATUS "Prometheus Library: ${PROMETHEUS_LIBRARIES}")
message(STATUS "Prometheus Includes: ${PROMETHEUS_INCLUDE_DIR}")

mark_as_advanced(
  PROMETHEUS_ROOT_DIR
  PROMETHEUS_LIBRARIES
  PROMETHEUS_INCLUDE_DIR
)
