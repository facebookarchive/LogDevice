# Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


auto_sources(safety_hfiles "*.h" RECURSE
  "${LOGDEVICE_ADMIN_DIR}/safety")
auto_sources(safety_files "*.cpp" RECURSE "${LOGDEVICE_ADMIN_DIR}/safety")

add_library(logdevice_safety_checker STATIC ${safety_hfiles} ${safety_files})


add_subdirectory(if)

# TODO: Remove ldclient when we decouple safety_checker from
# logdevice client library.
add_dependencies(logdevice_safety_checker common ldclient logdevice_server)
