# Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

add_library(admin_command_client STATIC AdminCommandClient.h AdminCommandClient.cpp)
add_dependencies(admin_command_client common folly)

target_link_libraries(admin_command_client
   ${LOGDEVICE_EXTERNAL_DEPS}
)

set_target_properties(admin_command_client
  PROPERTIES POSITION_INDEPENDENT_CODE ON)

add_subdirectory(py)
