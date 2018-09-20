# Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

find_path(SNAPPY_INCLUDE_DIR NAMES snappy.h)
find_library(SNAPPY_LIBRARY NAMES snappy)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(
    SNAPPY DEFAULT_MSG
    SNAPPY_LIBRARY SNAPPY_INCLUDE_DIR)

if (NOT SNAPPY_FOUND)
  message(STATUS "Using third-party bundled Snappy")
else()
  message(STATUS "Found Snappy: ${SNAPPY_LIBRARY}")
endif (NOT SNAPPY_FOUND)

mark_as_advanced(SNAPPY_INCLUDE_DIR SNAPPY_LIBRARY)
