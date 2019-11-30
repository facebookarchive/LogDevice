# Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
auto_sources(hfiles "*.h" RECURSE "${LOGDEVICE_PYTHON_CLIENT_DIR}")
auto_sources(files "*.cpp" RECURSE "${LOGDEVICE_PYTHON_CLIENT_DIR}")

REMOVE_MATCHES_FROM_LISTS(files hfiles
  MATCHES
    "test/*"
    "logdevice_settings.cpp"  # Settings depends on server at the moment >.<
)

add_library(logdevice_python SHARED ${hfiles} ${files})
include_directories(${PYTHON_INCLUDE_DIRS})

message(STATUS "Linking Python bindings with ${PYTHON_LIBRARIES}")
target_link_libraries(logdevice_python
  ldclient
  ${Boost_LIBRARIES}
  ${PYTHON_LIBRARIES}
)

# Since CMake prepends 'lib' to each library
# which would break the 'import logdevice' statements in Python
# In the future, we'll have to revisit this part, since
# we split our 'logdevice' namespace into multiple .so binaries (e.g. test)
file(MAKE_DIRECTORY "${LOGDEVICE_PY_OUT}/logdevice")
set_target_properties(logdevice_python
  PROPERTIES
  LIBRARY_OUTPUT_DIRECTORY "${LOGDEVICE_PY_OUT}/logdevice"
    OUTPUT_NAME "client"
    PREFIX ""
)
