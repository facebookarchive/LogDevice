# Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

auto_sources(hfiles "*.h" RECURSE "${LOGDEVICE_PYTHON_CLIENT_DIR}/util")
auto_sources(files "*.cpp" RECURSE "${LOGDEVICE_PYTHON_CLIENT_DIR}/util")

message(STATUS "Building ${hfiles} ${files}")
add_library(logdevice_python_util SHARED ${hfiles} ${files})

include_directories(${PYTHON_INCLUDE_DIRS})

add_dependencies(logdevice_python_util folly)

target_link_libraries(logdevice_python_util
  ${FOLLY_LIBRARIES}
  ${Boost_LIBRARIES}
  ${PYTHON_LIBRARIES}
)

install(TARGETS logdevice_python_util
  COMPONENT runtime
  DESTINATION lib
)
