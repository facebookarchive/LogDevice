# Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

cmake_minimum_required(VERSION 3.4.0 FATAL_ERROR)

# includes
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/CMake" ${CMAKE_MODULE_PATH})
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories("${LOGDEVICE_STAGING_DIR}/usr/local/include/")

set(PACKAGE_NAME "logdevice")
set(PACKAGE_VERSION "1.0-dev")
set(PACKAGE_STRING "${PACKAGE_NAME}-${PACKAGE_VERSION}")
set(PACKAGE_BUGREPORT "https://github.com/facebookincubator/LogDevice/issues")

set_property(GLOBAL PROPERTY USE_FOLDERS ON)

project(${PACKAGE_NAME} CXX)

enable_testing()
# gtest_discover_tests was introduced in cmake 3.10
if(${CMAKE_VERSION} VERSION_EQUAL "3.10" OR ${CMAKE_VERSION} VERSION_GREATER "3.10")
  include(GoogleTest OPTIONAL RESULT_VARIABLE HAVE_CMAKE_GTEST)
endif()

include (logdevice-functions)

include(build-config)

include(build-folly)
include(build-fizz)
include(build-wangle)
include(build-fbthrift)

include(${LOGDEVICE_DIR}/external/fbthrift/ThriftLibrary.cmake)

include(build-rocksdb)
include(build-docs)
include(logdevice-deps)

# GTest Project
add_subdirectory(${LOGDEVICE_DIR}/external/gtest)
include_directories(${GTEST_INCLUDE_DIRS} ${GMOCK_INCLUDE_DIRS})

# OpenTracing Project

add_subdirectory(${LOGDEVICE_DIR}/external/opentracing)
include_directories(${OPENTRACING_INCLUDE_DIRS})

# Flatbuffers Project
add_subdirectory(${LOGDEVICE_DIR}/external/flatbuffers)
include_directories(${FLATBUFFERS_INCLUDE_DIRS})

# Gason
add_subdirectory(${LOGDEVICE_DIR}/external/gason)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/external/gason)

include_directories(${LOGDEVICE_DIR}/..)

auto_sources(hfiles "*.h" RECURSE "${LOGDEVICE_CLIENT_HEADER_DIR}")

set(LOGDEVICE_EXTERNAL_DEPS
  ${OPENTRACING_MOCKTRACER_STATIC_LIB}
  ${OPENTRACING_STATIC_LIB}
  ${LIBUNWIND_LIBRARIES}
  ${FOLLY_BENCHMARK_LIBRARIES}
  ${FOLLY_LIBRARIES}
  ${ROCKSDB_LIBRARIES}
  ${Boost_LIBRARIES}
  ${OPENSSL_LIBRARIES}
  ${ZSTD_LIBRARY}
  ${LIBEVENT_LIB}
  ${LIBEVENT_LIB_SSL}
  ${LIBDL_LIBRARIES}
  ${DOUBLE_CONVERSION_LIBRARY}
  ${Zookeeper_LIBRARY}
  ${LIBGLOG_LIBRARY}
  ${LZ4_LIBRARY}
  ${ZLIB_LIBRARIES}
  ${LIBLZMA_LIBRARIES}
  ${IBERTY_LIBRARIES}
  ${BZIP2_LIBRARIES}
  ${ZLIB_LIBRARIES}
  ${JEMALLOC_LIBRARIES}
  ${IBERTY_LIBRARIES}
  ${SNAPPY_LIBRARY}
  Threads::Threads)

add_subdirectory(common)
add_subdirectory(lib)
add_subdirectory(server)
add_subdirectory(replication_checker)
add_subdirectory(admin)
add_subdirectory(test)
add_subdirectory(examples)
add_subdirectory(clients/python)
add_subdirectory(ops)
