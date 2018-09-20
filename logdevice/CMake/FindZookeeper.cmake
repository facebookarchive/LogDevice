# Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

find_path(Zookeeper_INCLUDE_DIR zookeeper/zookeeper.h)

find_library(Zookeeper_LIBRARY NAMES zookeeper_mt libzookeeper_mt)

set(Zookeeper_LIBRARIES ${Zookeeper_LIBRARY} )
set(Zookeeper_INCLUDE_DIRS ${Zookeeper_INCLUDE_DIR} )

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Zookeeper  DEFAULT_MSG
    Zookeeper_LIBRARY Zookeeper_INCLUDE_DIR)

mark_as_advanced(Zookeeper_INCLUDE_DIR Zookeeper_LIBRARY)
