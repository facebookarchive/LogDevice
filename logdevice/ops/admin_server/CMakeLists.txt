# Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

auto_sources(hfiles "*.h" RECURSE "${LOGDEVICE_ADMIN_SERVER_DIR}")
auto_sources(files "*.cpp" RECURSE "${LOGDEVICE_ADMIN_SERVER_DIR}")

add_executable(ld-admin-server ${files} ${hfiles})
target_link_libraries(ld-admin-server
  logdevice_server
  ${LOGDEVICE_EXTERNAL_DEPS}
)

install(TARGETS ld-admin-server
  COMPONENT runtime
  DESTINATION bin
)
