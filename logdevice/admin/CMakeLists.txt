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

auto_sources(admin_hfiles "*.h" "${LOGDEVICE_ADMIN_DIR}/")
auto_sources(admin_files "*.cpp""${LOGDEVICE_ADMIN_DIR}/")

auto_sources(admin_membership_hfiles "*.h"
  "${LOGDEVICE_ADMIN_DIR}/cluster_membership/")
auto_sources(admin_membership_files "*.cpp"
  "${LOGDEVICE_ADMIN_DIR}/cluster_membership/")

auto_sources(maintenance_hfiles "*.h" "${LOGDEVICE_ADMIN_DIR}/maintenance")
auto_sources(maintenance_files "*.cpp" "${LOGDEVICE_ADMIN_DIR}/maintenance")

# TODO: Add tests for Maintenance and Admin API
REMOVE_MATCHES_FROM_LISTS(admin_files admin_hfiles
  MATCHES
    "/test/"
    "/maintenance/test/"
    "AdminAPIUtils.cpp"
    "Conv.cpp"
    "AdminAPIUtils.h"
    "Conv.h"
)

add_library(logdevice_admin_util STATIC
  "${LOGDEVICE_ADMIN_DIR}/AdminAPIUtils.cpp"
  "${LOGDEVICE_ADMIN_DIR}/AdminAPIUtils.h"
  "${LOGDEVICE_ADMIN_DIR}/Conv.cpp"
  "${LOGDEVICE_ADMIN_DIR}/Conv.h")

add_dependencies(logdevice_admin_util admin-cpp2)

target_link_libraries(logdevice_admin_util
  logdevice_safety_checker
  admin-cpp2
  common)

set_target_properties(logdevice_admin_util
  PROPERTIES
    POSITION_INDEPENDENT_CODE True
)

add_library(logdevice_safety_checker STATIC ${safety_hfiles} ${safety_files})
target_link_libraries(logdevice_safety_checker common logdevice_server)

set_target_properties(logdevice_safety_checker
  PROPERTIES
    POSITION_INDEPENDENT_CODE True
)

add_library(logdevice_admin_settings STATIC ${admin_settings_hfiles}
  ${admin_settings_files})
target_link_libraries(logdevice_admin_settings common)

set_target_properties(logdevice_admin_settings
  PROPERTIES
    POSITION_INDEPENDENT_CODE True
)

add_library(logdevice_admin_maintenance STATIC ${maintenance_hfiles}
  ${maintenance_files})

target_link_libraries(logdevice_admin_maintenance
  common
  logdevice_safety_checker
  logdevice_admin_util
  admin-cpp2
  MaintenanceDelta-cpp2
  ${FBTHRIFT_LIBRARIES}
  ${GLOG_LIBRARIES}
  ${LIBGFLAGS_LIBRARY}
)

set_target_properties(logdevice_admin_maintenance
  PROPERTIES
    POSITION_INDEPENDENT_CODE True
)

add_subdirectory(if)


add_library(logdevice_admin STATIC
  ${admin_hfiles}
  ${admin_membership_hfiles}
  ${admin_membership_files}
  ${admin_files})

target_link_libraries(logdevice_admin
  common
  logdevice_admin_settings
  logdevice_safety_checker
  admin-cpp2
  MaintenanceDelta-cpp2
  logdevice_server_core
  ${FBTHRIFT_LIBRARIES}
  ${GLOG_LIBRARIES}
  ${LIBGFLAGS_LIBRARY}
)

set_target_properties(logdevice_admin
  PROPERTIES
    POSITION_INDEPENDENT_CODE True
)

set(_maintenance_delta_if_include_prefix "logdevice/admin/maintenance")
set(_maintenance_delta_binary_dir
  "${CMAKE_BINARY_DIR}/${_maintenance_delta_if_include_prefix}")

file(MAKE_DIRECTORY "${_maintenance_delta_binary_dir}")

thrift_library(
  "MaintenanceDelta"
  ""
  "cpp2"
  "json"
  "${CMAKE_CURRENT_SOURCE_DIR}/maintenance"
  "${_maintenance_delta_binary_dir}"
  "${_maintenance_delta_if_include_prefix}"
  THRIFT_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/.."
)

add_dependencies(MaintenanceDelta-cpp2-target
  maintenance-cpp2-target
  fbthrift
)

set_target_properties(MaintenanceDelta-cpp2-obj
        PROPERTIES POSITION_INDEPENDENT_CODE True
)

target_link_libraries(MaintenanceDelta-cpp2
  Membership-cpp2
  admin-cpp2
  ${FBTHRIFT_LIBRARIES}
  ${GLOG_LIBRARIES}
  ${LIBGFLAGS_LIBRARY}
)
