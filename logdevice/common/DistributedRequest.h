/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <unordered_map>
#include <vector>

#include "logdevice/common/NodeSetAccessor.h"
#include "logdevice/common/Request.h"

namespace facebook { namespace logdevice {

// Request which should be processed by multiple nodes
// We need to move all logic related to request sending and nodes managing to
// this class, because inheritor request classes use many copy-pasted code
class DistributedRequest : public Request {
 public:
  explicit DistributedRequest(RequestType type = RequestType::MISC)
      : Request(type) {}

  virtual ~DistributedRequest();

  FailedShardsMap getFailedShards(Status status) const;

 protected:
  std::unique_ptr<StorageSetAccessor> nodeset_accessor_{nullptr};
};

}} // namespace facebook::logdevice
