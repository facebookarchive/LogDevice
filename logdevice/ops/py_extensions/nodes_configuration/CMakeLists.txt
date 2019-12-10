# Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

python_add_module(logdevice_nodes_configuration_manager_python
  "nodes_configuration_manager.cpp"
)

include_directories(${PYTHON_INCLUDE_DIRS})

add_dependencies(logdevice_nodes_configuration_manager_python fbthrift)

target_link_libraries(logdevice_nodes_configuration_manager_python
  logdevice_python
  ldclient
  ${Boost_LIBRARIES}
  ${THRIFT_DEPS}
)

# Since CMake prepends 'lib' to each library
# which would break the 'import logdevice' statements in Python
# In the future, we'll have to revisit this part, since
# we split our 'logdevice' namespace into multiple .so binaries (e.g. test)
file(MAKE_DIRECTORY "${LOGDEVICE_PY_OUT}/logdevice/ops")
set_target_properties(logdevice_nodes_configuration_manager_python
  PROPERTIES
  LIBRARY_OUTPUT_DIRECTORY "${LOGDEVICE_PY_OUT}/logdevice/ops"
    OUTPUT_NAME "nodes_configuration_manager"
    PREFIX ""
)
