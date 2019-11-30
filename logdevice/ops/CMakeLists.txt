# Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

add_subdirectory(ldquery)
add_subdirectory(admin_command_client)

add_custom_target(create_ldshell ALL
  DEPENDS
    logdevice_ldquery_python
    logdevice_admin_command_client_python
  )

file(GLOB _ops_dirs
  "${LOGDEVICE_DIR}/ops/*.md"
  "${LOGDEVICE_DIR}/ops/*.py"
)

list(APPEND _ops_dirs
  "${LOGDEVICE_DIR}/ops/ldops"
  "${LOGDEVICE_DIR}/ops/ldshell"
)

# Just in case it wasn't already there.
file(MAKE_DIRECTORY "${LOGDEVICE_PY_OUT}")

foreach(_ops ${_ops_dirs})
  file(RELATIVE_PATH _target "${LOGDEVICE_DIR}/ops" "${_ops}")
  message(STATUS "Linking ${_ops} => ${LOGDEVICE_PY_OUT}/${_target}")
  add_custom_command(TARGET create_ldshell PRE_BUILD
    COMMAND
      ${CMAKE_COMMAND} -E create_symlink
      "${_ops}"
      "${LOGDEVICE_PY_OUT}/${_target}"
  )
endforeach()

add_custom_command(TARGET create_ldshell POST_BUILD
  COMMAND
    ${PYTHON_EXECUTABLE} setup.py bdist_wheel
  WORKING_DIRECTORY
    "${LOGDEVICE_PY_OUT}/"
)

install(CODE
  "execute_process(
  COMMAND ${PYTHON_EXECUTABLE} setup.py install
  WORKING_DIRECTORY \"${LOGDEVICE_PY_OUT}/\"
  )"
)
