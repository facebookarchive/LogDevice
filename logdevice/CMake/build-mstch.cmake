# Copyright (c) 2018-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

cmake_minimum_required(VERSION 3.4.0 FATAL_ERROR)
project(mstch C CXX)
include(ExternalProject)

ExternalProject_Add(mstch
    GIT_REPOSITORY https://github.com/no1msd/mstch.git
    GIT_TAG 1.0.2
    INSTALL_COMMAND make install DESTDIR=${LOGDEVICE_STAGING_DIR}
    )
