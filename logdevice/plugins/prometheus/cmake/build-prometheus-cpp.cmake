# Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

include(ExternalProject)

ExternalProject_Add(prometheus
    GIT_REPOSITORY https://github.com/jupp0r/prometheus-cpp
    GIT_TAG master
    PREFIX "${CMAKE_CURRENT_BINARY_DIR}"
    SOURCE_DIR "${CMAKE_CURRENT_BINARY_DIR}/external/prometheus-cpp"
    CMAKE_ARGS -DBUILD_SHARED_LIBS=OFF
               -DENABLE_TESTING=OFF
               -DENABLE_PUSH=OFF
               -DCMAKE_POSITION_INDEPENDENT_CODE=ON
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
  PROMETHEUS_LIBRARIES
  PROMETHEUS_INCLUDE_DIR
)
