# Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

file(GLOB files
     *.cpp tables/*.cpp)

file(GLOB hfiles
     *.h tables/*.h)
# markdown-ld-query, a util for producing documentation, defines main()
# hence it is excluded from the ldquery library.
REMOVE_MATCHES_FROM_LISTS(files
  MATCHES
    "markdown-ld-query.cpp"
)

add_library(ldquery STATIC ${hfiles} ${files})
add_dependencies(ldquery common gason_sources admin_command_client)
target_link_libraries(ldquery
  common
  logdevice_server
  logdevice_python
  ldclient
  logdevice_safety_checker
  admin_command_client
  gason_static
  ${SQLITE_LIBRARY}
  ${Boost_LIBRARIES}
  ${LIBGFLAGS_LIBRARY}
)


add_executable(markdown-ldquery "markdown-ldquery.cpp")
target_link_libraries(markdown-ldquery ldquery admin_command_client)

add_subdirectory(py)

set_target_properties(ldquery
  PROPERTIES POSITION_INDEPENDENT_CODE ON)
