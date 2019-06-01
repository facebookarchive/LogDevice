# Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

auto_sources(hfiles "*.h" RECURSE "${LOGDEVICE_TEST_DIR}")
auto_sources(files "*.cpp" RECURSE "${LOGDEVICE_TEST_DIR}")

REMOVE_MATCHES_FROM_LISTS(files hfiles
  MATCHES
    "/utils/"
    "/ldbench/"
    "/journal/"
    "phony_main.cpp"
)

auto_sources(test_util_hfiles "*.h" RECURSE "${LOGDEVICE_TEST_DIR}/utils")
auto_sources(test_util_files "*.cpp" RECURSE "${LOGDEVICE_TEST_DIR}/utils")

REMOVE_MATCHES_FROM_LISTS(test_util_files test_util_hfiles
  MATCHES
    "sizes.cpp"
    "localhost-cluster.cpp"
)

add_library(test_util STATIC ${test_util_hfiles} ${test_util_files})

add_dependencies(test_util googletest)
target_link_libraries(test_util
  common
  common_test_util
  ldclient_static
  ${GTEST_LIBRARY}
  ${LOGDEVICE_EXTERNAL_DEPS})

add_executable(integration_test phony_main.cpp ${hfiles} ${files})

enable_testing()
if (HAVE_CMAKE_GTEST)
gtest_discover_tests(integration_test
                     WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
else()
add_test(
  NAME IntegrationTest
  COMMAND ${UNIT_TEST_OUTPUT_DIRECTORY}/integration_test
  WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
endif()

set_property(DIRECTORY APPEND PROPERTY TEST_INCLUDE_FILES
  ${LOGDEVICE_TEST_DIR}/flakey_tests.cmake)

target_link_libraries(integration_test
  common
  common_test_util
  ldclient_static
  test_util
  locallogstore_test_util
  logdevice_server
  logdevice_safety_checker
  ${LOGDEVICE_EXTERNAL_DEPS}
  ${GTEST_LIBRARY}
  ${GMOCK_LIBRARY}
  ${LIBGFLAGS_LIBRARY})

set_target_properties(integration_test
  PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${UNIT_TEST_OUTPUT_DIRECTORY})

target_compile_definitions(integration_test
  PRIVATE
  GTEST_USE_OWN_TR1_TUPLE=0
)
