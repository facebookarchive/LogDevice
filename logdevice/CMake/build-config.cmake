# Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS}\
  -Wno-nullability-completeness\
  -Wno-deprecated-declarations\
  -Wno-inconsistent-missing-override\
  -Wno-defaulted-function-deleted")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--export-dynamic")
set(CMAKE_CXX_LINK_FLAGS "${CMAKE_CXX_LINK_FLAGS} -latomic")
set(LOGDEVICE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
set(LOGDEVICE_ADMIN_DIR "${LOGDEVICE_DIR}/admin")
set(LOGDEVICE_ADMIN_SERVER_DIR "${LOGDEVICE_DIR}/ops/admin_server")
set(LOGDEVICE_CLIENT_HEADER_DIR "${LOGDEVICE_DIR}/include")
set(LOGDEVICE_CLIENT_SRC_DIR "${LOGDEVICE_DIR}/lib")
set(LOGDEVICE_COMMON_DIR "${LOGDEVICE_DIR}/common")
set(LOGDEVICE_EXAMPLES_DIR "${LOGDEVICE_DIR}/examples")
set(LOGDEVICE_SERVER_DIR "${LOGDEVICE_DIR}/server")
set(LOGDEVICE_TEST_DIR "${LOGDEVICE_DIR}/test")
set(LOGDEVICE_PHONY_MAIN "${LOGDEVICE_DIR}/test/phony_main.cpp")
set(FB303_INCLUDE_DIR "${LOGDEVICE_COMMON_DIR}/if/for_open_source")

set(LOGDEVICE_STAGING_DIR "${CMAKE_BINARY_DIR}/staging")
set(LOGDEVICE_PYTHON_CLIENT_DIR "${LOGDEVICE_DIR}/clients/python")
set(THREADS_PREFER_PTHREAD_FLAG ON)


# Setting Output
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(UNIT_TEST_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/test)
set(LOGDEVICE_PY_OUT ${CMAKE_BINARY_DIR}/python-out)
