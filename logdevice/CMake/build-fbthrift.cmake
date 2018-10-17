# Copyright (c) 2018-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

set(FBTHRIFT_ROOT_DIR ${LOGDEVICE_DIR}/external/fbthrift)

include(ExternalProject)

ExternalProject_Add(fbthrift
    SOURCE_DIR "${FBTHRIFT_ROOT_DIR}"
    DOWNLOAD_COMMAND ""
    CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE=True
        -DCMAKE_PREFIX_PATH=${LOGDEVICE_STAGING_DIR}/usr/local
    INSTALL_COMMAND make install DESTDIR=${LOGDEVICE_STAGING_DIR}
    )

ExternalProject_Get_Property(fbthrift SOURCE_DIR)
ExternalProject_Get_Property(fbthrift BINARY_DIR)

ExternalProject_Add_StepDependencies(fbthrift configure mstch folly wangle)

set(FBTHRIFT_LIBRARIES
    ${BINARY_DIR}/libfbthrift.a)

message(STATUS "FBThrift Library: ${FBTHRIFT_LIBRARIES}")
message(STATUS "FBThrift Includes: ${FBTHRIFT_INCLUDE_DIR}")

mark_as_advanced(
    FBTHRIFT_ROOT_DIR
    FBTHRIFT_LIBRARIES
    FBTHRIFT_INCLUDE_DIR
)
