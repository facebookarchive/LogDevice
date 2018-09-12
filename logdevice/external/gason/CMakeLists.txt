# Copyright (c) 2017-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

cmake_minimum_required(VERSION 3.4.0 FATAL_ERROR)
project(gason_builder C CXX)
include(ExternalProject)

ExternalProject_Add(gason_sources
    GIT_REPOSITORY https://github.com/vivkin/gason.git
    GIT_TAG ede29fc5f0de8e47fd82c09f2f98123d2c867f28
    # Disable configure, build and install steps: we just want the source
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND ""
    PREFIX "${CMAKE_CURRENT_BINARY_DIR}"
    SOURCE_DIR "${CMAKE_CURRENT_BINARY_DIR}/external/gason"
    )

# Specify include dir
ExternalProject_Get_Property(gason_sources source_dir)

set_source_files_properties(${source_dir}/gason.cpp
                            ${source_dir}/gason.h
                            PROPERTIES GENERATED TRUE)

add_library(gason_static STATIC
            ${source_dir}/gason.cpp
            ${source_dir}/gason.h)

add_dependencies(gason_static gason_sources)

set_target_properties(gason_static
  PROPERTIES POSITION_INDEPENDENT_CODE ON)


set(GASON_INCLUDE_DIRS ${source_dir} PARENT_SCOPE)
