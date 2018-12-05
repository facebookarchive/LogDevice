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

class CheckImpactHandler : public virtual AdminAPIHandlerBase {
 public:
  // check admin.thrift for documentation
  virtual folly::SemiFuture<std::unique_ptr<thrift::CheckImpactResponse>>
  semifuture_checkImpact(
      std::unique_ptr<thrift::CheckImpactRequest> request) override;
};
}} // namespace facebook::logdevice
