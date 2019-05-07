# Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


auto_sources(safety_hfiles "*.h" RECURSE
  "${LOGDEVICE_ADMIN_DIR}/safety")
auto_sources(safety_files "*.cpp" RECURSE "${LOGDEVICE_ADMIN_DIR}/safety")

auto_sources(admin_settings_hfiles "*.h" RECURSE
  "${LOGDEVICE_ADMIN_DIR}/settings")
auto_sources(admin_settings_files
  "*.cpp" RECURSE "${LOGDEVICE_ADMIN_DIR}/settings")

add_library(logdevice_safety_checker STATIC ${safety_hfiles} ${safety_files})

add_library(logdevice_admin_settings STATIC ${admin_settings_hfiles}
  ${admin_settings_files})


add_subdirectory(if)

add_dependencies(logdevice_safety_checker common logdevice_server)
add_dependencies(logdevice_admin_settings common)
