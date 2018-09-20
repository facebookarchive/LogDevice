# Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

auto_sources(locallogstore_hfiles "*.h" RECURSE 
  "${LOGDEVICE_SERVER_DIR}/locallogstore/test"
)
auto_sources(locallogstore_files "*.cpp" RECURSE 
  "${LOGDEVICE_SERVER_DIR}/locallogstore/test"
)

add_library(locallogstore_test_util STATIC 
  ${locallogstore_hfiles}
  ${locallogstore_files}
)

target_link_libraries(locallogstore_test_util
  common
  common_test_util
  ${GMOCK_LIBRARY}
  ${GTEST_LIBRARY}
)
