/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "TrafficShapingConfig.h"

#include <folly/dynamic.h>
#include <folly/json.h>

#include "logdevice/common/PriorityMap.h"

namespace facebook { namespace logdevice { namespace configuration {

TrafficShapingConfig::TrafficShapingConfig() {
  flowGroupPolicies[static_cast<size_t>(NodeLocationScope::NODE)].setConfigured(
      true);
  flowGroupPolicies[static_cast<size_t>(NodeLocationScope::ROOT)].setConfigured(
      true);
}

folly::dynamic TrafficShapingConfig::toFollyDynamic() const {
  folly::dynamic result =
      folly::dynamic::object("default_read_traffic_class",
                             trafficClasses()[default_read_traffic_class]);

  folly::dynamic scope_list = folly::dynamic::array;
  NodeLocationScope scope = NodeLocationScope::NODE;
  for (const auto& fgp : flowGroupPolicies) {
    if (fgp.configured()) {
      scope_list.push_back(fgp.toFollyDynamic(scope));
    }
    scope = NodeLocation::nextGreaterScope(scope);
  }
  if (!scope_list.empty()) {
    result["scopes"] = scope_list;
  }

  return result;
}

}}} // namespace facebook::logdevice::configuration
