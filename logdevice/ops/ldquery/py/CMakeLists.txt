# Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

add_library(logdevice_ldquery_python SHARED "LDQueryModule.cpp")

include_directories(${PYTHON_INCLUDE_DIRS})

target_link_libraries(logdevice_ldquery_python
  ldquery
)

# Since CMake prepends 'lib' to each library
# which would break the 'import logdevice' statements in Python
# In the future, we'll have to revisit this part, since
# we split our 'logdevice' namespace into multiple .so binaries (e.g. test)
file(MAKE_DIRECTORY "${LOGDEVICE_PY_OUT}/logdevice/ldquery/internal")
file(WRITE "${LOGDEVICE_PY_OUT}/logdevice/__init__.py")
set_target_properties(logdevice_ldquery_python
  PROPERTIES
  LIBRARY_OUTPUT_DIRECTORY "${LOGDEVICE_PY_OUT}/logdevice/ldquery/internal"
    OUTPUT_NAME "ext"
    PREFIX ""
)

file(GLOB ldquery_py_files
  "${CMAKE_CURRENT_SOURCE_DIR}/*.py"
)

# Emitting the libraries and python extensions into the LOGDEVICE_PY_OUT
# directory with the correct structure
foreach(_src ${ldquery_py_files})
  get_filename_component(_target "${_src}" NAME)
  add_custom_command(TARGET logdevice_ldquery_python PRE_BUILD
    COMMAND
      ${CMAKE_COMMAND} -E create_symlink
      "${_src}"
      "${LOGDEVICE_PY_OUT}/logdevice/ldquery/${_target}"
  )
endforeach()
