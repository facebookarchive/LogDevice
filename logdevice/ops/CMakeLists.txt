# Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

add_subdirectory(ldquery)
add_subdirectory(admin_command_client)

if (NOT DEFINED NO_PYTHON)
  install(CODE "execute_process(COMMAND ${PYTHON_EXECUTABLE} setup.py install
                                WORKING_DIRECTORY ${LOGDEVICE_DIR}/ops/)")
endif (NOT DEFINED NO_PYTHON)
