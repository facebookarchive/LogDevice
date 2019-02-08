/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <vector>

#include "logdevice/common/configuration/FlowGroupPolicy.h"
#include "logdevice/common/configuration/NodeLocation.h"
#include "logdevice/common/configuration/TrafficClass.h"
#include "logdevice/common/debug.h"

namespace folly {
struct dynamic;
}

/**
 * @file Base class for Shaping based on FlowGroups
 *
 */

namespace facebook { namespace logdevice { namespace configuration {

class ShapingConfig {
 public:
  virtual bool configured(NodeLocationScope) const = 0;
  virtual folly::dynamic toFollyDynamic() const = 0;
  std::vector<FlowGroupPolicy> flowGroupPolicies;
  virtual ~ShapingConfig() {}
};

}}} // namespace facebook::logdevice::configuration
