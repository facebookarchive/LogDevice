/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <array>

#include "logdevice/common/configuration/FlowGroupPolicy.h"
#include "logdevice/common/configuration/NodeLocation.h"
#include "logdevice/common/configuration/TrafficClass.h"

namespace folly {
struct dynamic;
}

namespace facebook { namespace logdevice { namespace configuration {

struct TrafficShapingConfig {
  TrafficShapingConfig();

  bool configured(NodeLocationScope scope) const {
    return flowGroupPolicies[static_cast<size_t>(scope)].configured();
  }

  folly::dynamic toFollyDynamic() const;

  TrafficClass default_read_traffic_class = TrafficClass::READ_BACKLOG;
  std::array<FlowGroupPolicy, NodeLocation::NUM_ALL_SCOPES> flowGroupPolicies;
};

}}} // namespace facebook::logdevice::configuration
