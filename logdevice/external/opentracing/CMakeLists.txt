# Copyright (c) 2017-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

cmake_minimum_required(VERSION 3.4.0 FATAL_ERROR)
project(opentracing_builder C CXX)
include(ExternalProject)

ExternalProject_Add(OpenTracing
    GIT_REPOSITORY https://github.com/opentracing/opentracing-cpp.git
    GIT_TAG v1.5.0
    CMAKE_ARGS -DCMAKE_ARCHIVE_OUTPUT_DIRECTORY_DEBUG:PATH=DebugLibs
    -DCMAKE_ARCHIVE_OUTPUT_DIRECTORY_RELEASE:PATH=ReleaseLibs
    -DCMAKE_POSITION_INDEPENDENT_CODE=True
    -DCMAKE_CXX_FLAGS=${MSVC_COMPILER_DEFS}
    PREFIX "${CMAKE_CURRENT_BINARY_DIR}"
    # Disable install step
    INSTALL_COMMAND ""
    )

ExternalProject_Get_Property(OpenTracing binary_dir)
ExternalProject_Get_Property(OpenTracing source_dir)
set(OPENTRACING_INCLUDE_DIRS ${binary_dir}/include ${source_dir}/include ${source_dir}/3rd_party/include ${source_dir}/mocktracer/include PARENT_SCOPE)

MESSAGE("OpenTracing includes: " ${binary_dir}/include)

set(OPENTRACING_STATIC_LIB ${binary_dir}/output/libopentracing.a PARENT_SCOPE)
set(OPENTRACING_MOCKTRACER_STATIC_LIB ${binary_dir}/output/libopentracing_mocktracer.a PARENT_SCOPE)
