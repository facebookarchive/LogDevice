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
    CMAKE_ARGS -Dthriftpy3=${thriftpy3} -DCXX_STD=gnu++14
        -DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX}
        -DCMAKE_PREFIX_PATH=${LOGDEVICE_STAGING_DIR}/usr/local
    INSTALL_COMMAND make install DESTDIR=${LOGDEVICE_STAGING_DIR}
    )

ExternalProject_Get_Property(fbthrift SOURCE_DIR)
ExternalProject_Get_Property(fbthrift BINARY_DIR)

ExternalProject_Add_StepDependencies(fbthrift configure folly wangle)

# The following settings are required by ThriftLibrary.cmake; to create rules
# for thrift compilation:
set(THRIFT1 ${BINARY_DIR}/bin/thrift1)
set(THRIFT_TEMPLATES ${LOGDEVICE_DIR}/external/fbthrift/thrift/compiler/generate/templates)
set(THRIFTCPP2 ${BINARY_DIR}/lib/libthriftcpp2.a)

set(FBTHRIFT_LIBRARIES
    ${BINARY_DIR}/libfbthrift.a)
set(FBTHRIFT_INCLUDE_DIR
    ${SOURCE_DIR})
message(STATUS "FBThrift Library: ${FBTHRIFT_LIBRARIES}")
message(STATUS "FBThrift Includes: ${FBTHRIFT_INCLUDE_DIR}")
message("FBThrift Compiler: ${THRIFT1}")


mark_as_advanced(
    FBTHRIFT_ROOT_DIR
    FBTHRIFT_LIBRARIES
    FBTHRIFT_INCLUDE_DIR
)
