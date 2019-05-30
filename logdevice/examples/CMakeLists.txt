# Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

add_executable(ldcat
  ${LOGDEVICE_EXAMPLES_DIR}/cat.cpp
  ${LOGDEVICE_EXAMPLES_DIR}/parse_target_log.h
  ${LOGDEVICE_EXAMPLES_DIR}/parse_target_log.cpp)

add_executable(ldtail
  ${LOGDEVICE_EXAMPLES_DIR}/tail.cpp
  ${LOGDEVICE_EXAMPLES_DIR}/parse_target_log.h
  ${LOGDEVICE_EXAMPLES_DIR}/parse_target_log.cpp)

add_executable(ldtrim
  ${LOGDEVICE_EXAMPLES_DIR}/trim.cpp
  ${LOGDEVICE_EXAMPLES_DIR}/parse_target_log.h
  ${LOGDEVICE_EXAMPLES_DIR}/parse_target_log.cpp)

add_executable(ldwrite
  ${LOGDEVICE_EXAMPLES_DIR}/write.cpp
  ${LOGDEVICE_EXAMPLES_DIR}/parse_target_log.h
  ${LOGDEVICE_EXAMPLES_DIR}/parse_target_log.cpp)

add_executable(ldbuffwrite
  ${LOGDEVICE_EXAMPLES_DIR}/buffered_writer.cpp
  ${LOGDEVICE_EXAMPLES_DIR}/parse_target_log.h
  ${LOGDEVICE_EXAMPLES_DIR}/parse_target_log.cpp)

auto_sources(cluster_hfiles "*.h" RECURSE "${LOGDEVICE_DIR}/test/utils")
auto_sources(cluster_files "*.cpp" RECURSE "${LOGDEVICE_DIR}/test/utils")
auto_sources(cluster_testutil_files "*TestUtil.cpp" RECURSE
  "${LOGDEVICE_COMMON_DIR}/test")
auto_sources(cluster_testutil_hfiles "*TestUtil.h" RECURSE
  "${LOGDEVICE_COMMON_DIR}/test")
auto_sources(cluster_storeutil_files "*StoreUtil.cpp" RECURSE
  "${LOGDEVICE_SERVER_DIR}/locallogstore/test")

REMOVE_MATCHES_FROM_LISTS(cluster_files
  MATCHES
    "sizes.cpp"
)

add_executable(ld-dev-cluster
  ${cluster_hfiles}
  ${cluster_files}
  ${cluster_testutil_files}
  ${cluster_testutil_hfiles}
  ${cluster_storeutil_files}
)

target_link_libraries(ld-dev-cluster
  logdevice_server
  ${LOGDEVICE_EXTERNAL_DEPS}
)

target_link_libraries(ldcat ldclient)
target_link_libraries(ldtail ldclient)
target_link_libraries(ldtrim ldclient)
target_link_libraries(ldwrite ldclient)
target_link_libraries(ldbuffwrite ldclient)

add_custom_target(examples)
add_dependencies(examples ldcat ldtail ldtrim ldwrite ldbuffwrite)
