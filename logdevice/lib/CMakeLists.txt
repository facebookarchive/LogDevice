# Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

auto_sources(hfiles "*.h" RECURSE "${LOGDEVICE_CLIENT_SRC_DIR}")
auto_sources(files "*.cpp" RECURSE "${LOGDEVICE_CLIENT_SRC_DIR}")

REMOVE_MATCHES_FROM_LISTS(files hfiles includes
  MATCHES
    "/test/"
)

add_library(ldclient SHARED ${hfiles} ${files})
add_library(ldclient_static STATIC ${hfiles} ${files})

set_target_properties(ldclient ldclient_static
  PROPERTIES
  OUTPUT_NAME logdevice)

target_link_libraries(ldclient
  common
  ${LOGDEVICE_EXTERNAL_DEPS})

target_link_libraries(ldclient_static
  common
  ${LOGDEVICE_EXTERNAL_DEPS})

set_target_properties(ldclient_static
  PROPERTIES POSITION_INDEPENDENT_CODE ON)

add_dependencies(ldclient rocksdb folly)

# Tests
if (${BUILD_TESTS})
  file(GLOB test_hfiles "${LOGDEVICE_CLIENT_SRC_DIR}/test/*.h")
  file(GLOB test_files  "${LOGDEVICE_CLIENT_SRC_DIR}/test/*.cpp")

  add_executable(ldclient_test ${test_hfiles} ${test_files})

  target_link_libraries(ldclient_test
    ldclient_static
    common
    common_test_util
    ${LOGDEVICE_EXTERNAL_DEPS}
    ${GMOCK_LIBRARY}
    ${GTEST_LIBRARY}
    ${LIBGFLAGS_LIBRARY})

  add_dependencies(ldclient_test rocksdb)

  set_target_properties(ldclient_test
    PROPERTIES
      RUNTIME_OUTPUT_DIRECTORY ${UNIT_TEST_OUTPUT_DIRECTORY}
      OUTPUT_NAME logdevice_client_test)

  enable_testing()
  if(HAVE_CMAKE_GTEST)
  gtest_discover_tests(ldclient_test
                       WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
  else()
  add_test(
    NAME LdClientTest
    COMMAND ${UNIT_TEST_OUTPUT_DIRECTORY}/logdevice_client_test
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
  endif()
endif() # ${BUILD_TESTS}

install(DIRECTORY ${LOGDEVICE_CLIENT_HEADER_DIR}/
  COMPONENT devel
  DESTINATION
  include/logdevice
  FILES_MATCHING PATTERN "*.h")

install(TARGETS ldclient
  COMPONENT runtime
  DESTINATION lib)

# Ensure dynamic lib is in search path, configure ld and apply it
# when installing to /usr/lib, it will already be there, so no specific
# config required. But for /usr/local or a custom location we would
# need to add this to the library search paths
if(NOT CMAKE_INSTALL_PREFIX STREQUAL "/usr")
    install(CODE "
    exec_program(\"id -u\" OUTPUT_VARIABLE _current_uid)
    if (_current_uid EQUAL 0)
      file(WRITE \"/etc/ld.so.conf.d/logdevice.conf\"
        \"${CMAKE_INSTALL_PREFIX}/lib\")
      execute_process(COMMAND \"ldconfig\")
    else()
      message(\"Configuration of library path skipped, as root permission\"
      \" required. Please configure manually.\")
    endif()")
endif()
