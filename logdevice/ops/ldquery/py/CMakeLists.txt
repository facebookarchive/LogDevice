# Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

add_library(logdevice_ldquery_python SHARED "LDQueryModule.cpp")

include_directories(${PYTHON_INCLUDE_DIRS})

target_link_libraries(logdevice_ldquery_python
  ldquery)

# Since CMake prepends 'lib' to each library
# which would break the 'import logdevice' statements in Python
# In the future, we'll have to revisit this part, since
# we split our 'logdevice' namespace into multiple .so binaries (e.g. test)
set_target_properties(logdevice_ldquery_python PROPERTIES PREFIX "")
set_target_properties(logdevice_ldquery_python PROPERTIES OUTPUT_NAME "ext")

install(TARGETS logdevice_ldquery_python
  COMPONENT runtime
  DESTINATION "${PYTHON_MODULE_INSTALL_PREFIX}/logdevice/ldquery/internal")
install(CODE
  "file(WRITE
    \"\$ENV\{DESTDIR\}${PYTHON_MODULE_INSTALL_PREFIX}/logdevice/ldquery/internal/__init__.py\"
    \"\")")
install(FILES lib.py __init__.py
        DESTINATION "${PYTHON_MODULE_INSTALL_PREFIX}/logdevice/ldquery")
