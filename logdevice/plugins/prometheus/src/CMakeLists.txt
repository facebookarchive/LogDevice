# Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# The plugin's main shared library
add_library(ldprometheus
    SHARED
    PrometheusPublisherFactory
    PrometheusStatsPublisher
    PrometheusSettings
    main
)

add_dependencies(ldprometheus prometheus folly)

target_include_directories(
    ldprometheus
    PUBLIC
        ${LOGDEVICE_INCLUDE_DIRS}
        ${PROMETHEUS_INCLUDE_DIR}
)

# Build our shared library with -fPIC
set_target_properties(
  ldprometheus
  PROPERTIES
  POSITION_INDEPENDENT_CODE True
)

# Link the prometheus library statically
target_link_libraries(
    ldprometheus
    "-Wl,--whole-archive"
    ${PROMETHEUS_LIBRARIES}
    "-Wl,--no-whole-archive"
)


# Building the tests
if(${BUILD_TESTS})
  add_executable(prometheus_test tests/PublisherTest.cpp)
  add_dependencies(prometheus_test ldprometheus)
  target_include_directories(
    prometheus_test
    PUBLIC
    ${GTEST_INCLUDE_DIRS})
  target_link_libraries(
    prometheus_test
    ldprometheus
    common
    logdevice_server  
    ${GTEST_LIBRARY}
    ${PROMETHEUS_LIBRARIES}
    ${LOGDEVICE_EXTERNAL_DEPS}
    glog
    Threads::Threads)

  if (HAVE_CMAKE_GTEST)
    gtest_discover_tests(prometheus_test
      WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
  else()
    add_test(
      NAME PrometheusTest
      COMMAND ${UNIT_TEST_OUTPUT_DIRECTORY}/prometheus_test
      WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
  endif()

  add_test(NAME prometheus_test COMMAND prometheus_test)

  set_target_properties(prometheus_test
    PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${UNIT_TEST_OUTPUT_DIRECTORY})

  target_compile_definitions(prometheus_test
    PRIVATE
    GTEST_USE_OWN_TR1_TUPLE=0
  )
endif(${BUILD_TESTS})
