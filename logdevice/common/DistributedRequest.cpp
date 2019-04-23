/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/DistributedRequest.h"

namespace facebook { namespace logdevice {

DistributedRequest::~DistributedRequest() {}

FailedShardsMap DistributedRequest::getFailedShards(Status status) const {
  if (status == Status::OK || !nodeset_accessor_) {
    return FailedShardsMap{};
  }

  return nodeset_accessor_->getFailedShards(
      [](Status s) -> bool { return s != Status::OK; });
}

}} // namespace facebook::logdevice
