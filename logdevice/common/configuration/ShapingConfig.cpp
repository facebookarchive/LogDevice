/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/configuration/ShapingConfig.h"

#include "logdevice/common/PriorityMap.h"

namespace facebook { namespace logdevice { namespace configuration {

ShapingConfig::ShapingConfig(
    std::set<NodeLocationScope> valid_scopes,
    const std::set<NodeLocationScope>& scopes_to_configure)
    : valid_scopes_(std::move(valid_scopes)) {
  FlowGroupPolicy configured_fgp;
  configured_fgp.setConfigured(true);

  for (auto& s : scopes_to_configure) {
    if (valid_scopes_.find(s) != valid_scopes_.end()) {
      flowGroupPolicies[s] = configured_fgp;
    }
  }
}

bool ShapingConfig::configured(NodeLocationScope scope) const {
  auto it = flowGroupPolicies.find(scope);
  if (it != flowGroupPolicies.end()) {
    return it->second.configured();
  }
  return false;
}

void ShapingConfig::toFollyDynamic(folly::dynamic& result) const {
  folly::dynamic scope_list = folly::dynamic::array;

  for (const auto& fgp : flowGroupPolicies) {
    if (fgp.second.configured()) {
      scope_list.push_back(toFollyDynamic(fgp.first, fgp.second));
    }
  }

  if (!scope_list.empty()) {
    result["scopes"] = scope_list;
  }
}

folly::dynamic ShapingConfig::toFollyDynamic(const NodeLocationScope scope,
                                             const FlowGroupPolicy& fgp) {
  folly::dynamic result =
      folly::dynamic::object("name", NodeLocation::scopeNames()[scope])(
          "shaping_enabled", fgp.enabled());

  folly::dynamic meter_list = folly::dynamic::array;
  Priority p = Priority::MAX;
  for (const auto& entry : fgp.entries) {
    if (entry.guaranteed_bw != 0 || entry.capacity != 0) {
      meter_list.push_back(toFollyDynamic(p, entry));
    }
    p = priorityBelow(p);
  }
  if (!meter_list.empty()) {
    result["meters"] = meter_list;
  }

  return result;
}

folly::dynamic
ShapingConfig::toFollyDynamic(const Priority pri,
                              const FlowGroupPolicy::Entry& fgp_entry) {
  folly::dynamic result;
  result = folly::dynamic::object("name",
                                  pri == Priority::INVALID
                                      ? "PRIORITY_QUEUE"
                                      : PriorityMap::toName()[asInt(pri)])(
      "guaranteed_bytes_per_second", fgp_entry.guaranteed_bw)(
      "max_burst_bytes", fgp_entry.capacity);

  if (fgp_entry.max_bw != INT64_MAX) {
    result.insert("max_bytes_per_second", fgp_entry.max_bw);
  }

  return result;
}

}}} // namespace facebook::logdevice::configuration
