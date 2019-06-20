# Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

auto_sources(hfiles "*.h" RECURSE "${LOGDEVICE_COMMON_DIR}")
auto_sources(files "*.cpp" RECURSE "${LOGDEVICE_COMMON_DIR}")

include_directories(${CMAKE_BINARY_DIR})

REMOVE_MATCHES_FROM_LISTS(files hfiles
  MATCHES
    "/test/"
)

add_subdirectory(configuration/nodes)
add_subdirectory(membership)

add_library(common STATIC ${hfiles} ${files})
add_dependencies(common folly flatbuffers OpenTracing)
add_dependencies(common NodesConfiguration-cpp2-target)

set_target_properties(common
  PROPERTIES POSITION_INDEPENDENT_CODE ON)

target_link_libraries(common
  Membership-cpp2
  NodesConfiguration-cpp2
  ${LOGDEVICE_EXTERNAL_DEPS}
)

if(${BUILD_TESTS})
  # Tests
  file(GLOB test_hfiles "${LOGDEVICE_COMMON_DIR}/test/*.h")
  file(GLOB test_files "${LOGDEVICE_COMMON_DIR}/test/*.cpp")

  add_library(common_test_util STATIC
    "test/TestUtil.h"
    "test/TestUtil.cpp"
    "test/NodeSetTestUtil.h"
    "test/NodeSetTestUtil.cpp"
    "test/NodesConfigurationTestUtil.h"
    "test/NodesConfigurationTestUtil.cpp"
    "test/ZookeeperClientInMemory.cpp"
    "test/ZookeeperClientInMemory.h"
  )

  target_link_libraries(common_test_util
    common
    ${LOGDEVICE_EXTERNAL_DEPS}
    ${GTEST_LIBRARY}
    ${GMOCK_LIBRARY}
  )

  add_dependencies(common_test_util googletest folly rocksdb OpenTracing)

  add_executable(common_test ${test_hfiles} ${test_files})
  add_dependencies(common_test googletest folly)
  target_link_libraries(common_test
    common
    common_test_util
    ${LOGDEVICE_EXTERNAL_DEPS}
    ${FOLLY_TEST_UTIL_LIBRARIES}
    ${GTEST_LIBRARY}
    ${GMOCK_LIBRARY}
    ${LIBGFLAGS_LIBRARY})

  if(HAVE_CMAKE_GTEST)
    gtest_discover_tests(common_test
                       WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
  else()
    add_test(
      NAME CommonTest
      COMMAND ${UNIT_TEST_OUTPUT_DIRECTORY}/common_test
      WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
  endif()

  set_target_properties(common_test
    PROPERTIES
      RUNTIME_OUTPUT_DIRECTORY ${UNIT_TEST_OUTPUT_DIRECTORY})

  target_compile_definitions(common_test
    PRIVATE
    GTEST_USE_OWN_TR1_TUPLE=0
  )

  file(COPY ${LOGDEVICE_COMMON_DIR}/test/configs DESTINATION
    ${CMAKE_BINARY_DIR}/logdevice/common/test/)

  file(COPY ${LOGDEVICE_COMMON_DIR}/test/ssl_certs DESTINATION
    ${CMAKE_BINARY_DIR}/logdevice/common/test/)
endif()
