/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/admin/if/gen-cpp2/AdminAPI.h"

namespace facebook { namespace logdevice {
class ServerProcessor;
class Server;
namespace configuration {
class Node;
}

class AdminAPIHandlerBase : public virtual thrift::AdminAPISvIf {
 protected:
  AdminAPIHandlerBase() = default;
  explicit AdminAPIHandlerBase(Server* server);

 protected:
  Server* ld_server_;
  ServerProcessor* processor_;
};
}} // namespace facebook::logdevice
