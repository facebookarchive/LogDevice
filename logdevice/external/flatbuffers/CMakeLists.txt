# Copyright (c) 2017-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

cmake_minimum_required(VERSION 3.4.0 FATAL_ERROR)
project(flatbuffers_builder C CXX)
include(ExternalProject)

# We currently depend on a specific version of flatbuffers since the generated
# files are checked in and depend on <flatbuffers/flatbuffers.h> being the
# same version as the flatc compiler used to generate them.  We should move to
# a setup where those files are generated at build time, allowing users to
# upgrade flatbuffers as they wish.

ExternalProject_Add(flatbuffers
    GIT_REPOSITORY https://github.com/google/flatbuffers.git
    GIT_TAG v1.5.0
    # Disable configure, build and install steps: we just want the headers
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND ""
    )

# Specify include dir
ExternalProject_Get_Property(flatbuffers source_dir)
set(FLATBUFFERS_INCLUDE_DIRS ${source_dir}/include PARENT_SCOPE)
