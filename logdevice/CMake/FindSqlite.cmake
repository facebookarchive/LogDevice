# Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

find_path(SQLITE_INCLUDE_DIR NAMES sqlite3.h)
find_library(SQLITE_LIBRARY NAMES sqlite3)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(
    SQLITE DEFAULT_MSG
    SQLITE_LIBRARY SQLITE_INCLUDE_DIR)

if (NOT SQLITE_FOUND)
  message(STATUS "Using third-party bundled SQLite")
else()
  message(STATUS "Found SQLite: ${SQLITE_LIBRARY}")
endif (NOT SQLITE_FOUND)

mark_as_advanced(SQLITE_INCLUDE_DIR SQLITE_LIBRARY)
