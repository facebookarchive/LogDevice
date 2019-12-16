# Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
auto_sources(hfiles "*.h" RECURSE "${LOGDEVICE_C_CLIENT_DIR}")
auto_sources(files "*.cpp" RECURSE "${LOGDEVICE_C_CLIENT_DIR}")

add_library(logdevice_cclient SHARED ${hfiles} ${files})

target_link_libraries(logdevice_cclient
  ldquery
  ldclient
  ${Boost_LIBRARIES})
