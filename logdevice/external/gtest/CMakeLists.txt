# Copyright (c) 2017-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

cmake_minimum_required(VERSION 3.4.0 FATAL_ERROR)
project(gtest_builder C CXX)
include(ExternalProject)

set(GTEST_FORCE_SHARED_CRT ON)
set(GTEST_DISABLE_PTHREADS OFF)

ExternalProject_Add(googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG release-1.8.1
    CMAKE_ARGS -DCMAKE_ARCHIVE_OUTPUT_DIRECTORY_DEBUG:PATH=DebugLibs
    -DCMAKE_ARCHIVE_OUTPUT_DIRECTORY_RELEASE:PATH=ReleaseLibs
    -DCMAKE_CXX_FLAGS=${MSVC_COMPILER_DEFS}
    -Dgtest_force_shared_crt=${GTEST_FORCE_SHARED_CRT}
    -Dgtest_disable_pthreads=${GTEST_DISABLE_PTHREADS}
    PREFIX "${CMAKE_CURRENT_BINARY_DIR}"
    # Disable install step
    INSTALL_COMMAND ""
    )

# Specify include dir
ExternalProject_Get_Property(googletest source_dir)
set(GTEST_INCLUDE_DIRS ${source_dir}/googletest/include PARENT_SCOPE)
set(GMOCK_INCLUDE_DIRS ${source_dir}/googlemock/include PARENT_SCOPE)

# Specify MainTest's link libraries
ExternalProject_Get_Property(googletest binary_dir)
set(GTEST_LIBS_DIR ${binary_dir}/googlemock/gtest)
set(GMOCK_LIBS_DIR ${binary_dir}/googlemock)

SET(GTEST_LIBRARY ${GTEST_LIBS_DIR}/libgtest_main.a ${GTEST_LIBS_DIR}/libgtest.a PARENT_SCOPE)
SET(GMOCK_LIBRARY ${GMOCK_LIBS_DIR}/libgmock_main.a ${GMOCK_LIBS_DIR}/libgmock.a PARENT_SCOPE)

message(STATUS "GTest: ${GTEST_INCLUDE_DIRS} ${GMOCK_INCLUDE_DIRS}")
