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
    CMAKE_ARGS
        -Dthriftpy3=${thriftpy3}
        -DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX}
        -DCMAKE_PREFIX_PATH=${LOGDEVICE_STAGING_DIR}/usr/local
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DCMAKE_POSITION_INDEPENDENT_CODE=True
        -DBUILD_SHARED_LIBS=ON
        -Denable_tests=OFF
        -DCXX_STD=gnu++17
        -DCMAKE_CXX_STANDARD=17
    INSTALL_COMMAND make install DESTDIR=${LOGDEVICE_STAGING_DIR}
    )

ExternalProject_Get_Property(fbthrift SOURCE_DIR)
ExternalProject_Get_Property(fbthrift BINARY_DIR)

ExternalProject_Add_StepDependencies(fbthrift configure folly wangle
  rsocket fmt)

# The following settings are required by ThriftLibrary.cmake; to create rules
# for thrift compilation:
set(THRIFT1 ${BINARY_DIR}/bin/thrift1)
# We don't set THRIFTCPP2 because we manage the full thrift dependency
# externally via LOGDEVICE_EXTERNAL_DEPS
#set(THRIFTCPP2 ${BINARY_DIR}/lib/libthriftcpp2.a)

set(FBTHRIFT_LIBRARIES
    ${BINARY_DIR}/lib/libprotocol.so
    ${BINARY_DIR}/lib/libthriftcpp2.so
    ${BINARY_DIR}/lib/libcompiler_ast.so
    ${BINARY_DIR}/lib/libtransport.so
    ${BINARY_DIR}/lib/libthriftfrozen2.so
    ${BINARY_DIR}/lib/libcompiler_generators.so
    ${BINARY_DIR}/lib/libcompiler_lib.so
    ${BINARY_DIR}/lib/libmustache_lib.so
    ${BINARY_DIR}/lib/libasync.so
    ${BINARY_DIR}/lib/libthrift-core.so
    ${BINARY_DIR}/lib/libcompiler_base.so
    ${BINARY_DIR}/lib/libthriftprotocol.so
    ${BINARY_DIR}/lib/libconcurrency.so
)

set(FBTHRIFT_INCLUDE_DIR
    ${SOURCE_DIR} ${BINARY_DIR})
message(STATUS "FBThrift Library: ${FBTHRIFT_LIBRARIES}")
message(STATUS "FBThrift Includes: ${FBTHRIFT_INCLUDE_DIR}")
message("FBThrift Compiler: ${THRIFT1}")


mark_as_advanced(
    FBTHRIFT_ROOT_DIR
    FBTHRIFT_LIBRARIES
    FBTHRIFT_INCLUDE_DIR
)
