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
  ${PYTHON_LIBRARIES})

# Since CMake prepends 'lib' to each library
# which would break the 'import logdevice' statements in Python
# In the future, we'll have to revisit this part, since
# we split our 'logdevice' namespace into multiple .so binaries (e.g. test)
set_target_properties(logdevice_python PROPERTIES PREFIX "")
set_target_properties(logdevice_python PROPERTIES OUTPUT_NAME "client")

message(STATUS "Python library will be installed to ${_python_dist_path}")

# We expect both the client.so and __init__.py file to be in the logdevice
# folder
install(TARGETS logdevice_python
  COMPONENT runtime
  DESTINATION "${PYTHON_MODULE_INSTALL_PREFIX}/logdevice/")
install(CODE
  "file(WRITE
    \"\$ENV\{DESTDIR\}${PYTHON_MODULE_INSTALL_PREFIX}/logdevice/__init__.py\"
    \"\")")
