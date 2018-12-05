/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Optional.h>

#include "logdevice/admin/AdminAPIHandlerBase.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

class NodesConfigAPIHandler : public virtual AdminAPIHandlerBase {
 public:
  virtual void
  // See admin.thrift for documentation
  getNodesConfig(thrift::NodesConfigResponse&,
                 std::unique_ptr<thrift::NodesFilter> filter) override;
};
}} // namespace facebook::logdevice
