/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/protocol/Message.h"
#include "logdevice/include/PermissionActions.h"

namespace facebook { namespace logdevice {

struct PermissionParams {
  bool requiresPermission{false};
  ACTION action{ACTION::MAX};
  logid_t log_id{LOGID_INVALID};
};

class ServerMessagePermission {
 public:
  static PermissionParams computePermissionParams(Message* msg);
};

}} // namespace facebook::logdevice
