# Copyright (c) 2018-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

cmake_minimum_required(VERSION 3.4.0 FATAL_ERROR)
project(zstd C CXX)
include(ExternalProject)

ExternalProject_Add(zstd
    GIT_REPOSITORY https://github.com/facebook/zstd.git
    GIT_TAG v1.3.3
    CONFIGURE_COMMAND cmake -DCMAKE_POSITION_INDEPENDENT_CODE=True ../zstd/build/cmake
    INSTALL_COMMAND make install DESTDIR=${LOGDEVICE_STAGING_DIR}
    )

set(ZSTD_LIBRARY ${LOGDEVICE_STAGING_DIR}/usr/local/lib/libzstd.a PARENT_SCOPE)
