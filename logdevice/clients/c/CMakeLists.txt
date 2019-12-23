# Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
auto_sources(hfiles "*.h" RECURSE "${LOGDEVICE_C_CLIENT_DIR}")
auto_sources(files "*.cpp" RECURSE "${LOGDEVICE_C_CLIENT_DIR}")

add_library(ldcclient SHARED ${hfiles} ${files})

target_link_libraries(ldcclient
  ldquery
  ldclient
  ${Boost_LIBRARIES})


# Tests
if(${BUILD_TESTS})
  file(GLOB test_hfiles "${LOGDEVICE_C_CLIENT_DIR}/test/*.h")
  file(GLOB test_files  "${LOGDEVICE_C_CLIENT_DIR}/test/*.cpp")

  add_executable(ldcclient_test
    ${LOGDEVICE_PHONY_MAIN}
    ${test_hfiles}
    ${test_files}
  )

  enable_testing()
  if(HAVE_CMAKE_GTEST)
    gtest_discover_tests(ldcclient_test
                     WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
  else()
    add_test(
      NAME LDCClientTest
      COMMAND ${UNIT_TEST_OUTPUT_DIRECTORY}/ldcclient_test
      WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
  endif()

  set_property(DIRECTORY APPEND PROPERTY TEST_INCLUDE_FILES
    ${LOGDEVICE_C_CLIENT_DIR}/flaky_tests.cmake)

  target_link_libraries(ldcclient_test
    ldcclient
    common
    common_test_util
    test_util
    logdevice_server
    logdevice_admin_settings
    locallogstore_test_util
    ${LOGDEVICE_EXTERNAL_DEPS}
    ${GMOCK_LIBRARY}
    ${GTEST_LIBRARY}
    ${LIBGFLAGS_LIBRARY})

  set_target_properties(ldcclient_test
    PROPERTIES
      RUNTIME_OUTPUT_DIRECTORY ${UNIT_TEST_OUTPUT_DIRECTORY})

  target_compile_definitions(ldcclient_test
    PRIVATE
    GTEST_USE_OWN_TR1_TUPLE=0
  )

  # file(COPY ${LOGDEVICE_C_CLIENT_DIR}/test/configs DESTINATION
  #   ${CMAKE_BINARY_DIR}/logdevice/clients/c/test/)
endif()
