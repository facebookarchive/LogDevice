/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <set>

#include <folly/dynamic.h>
#include <folly/json.h>

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
  ShapingConfig(std::set<NodeLocationScope> valid_scopes,
                const std::set<NodeLocationScope>& scopes_to_configure);

  bool configured(NodeLocationScope scope) const;

  /* Common Serialization functions */
  static folly::dynamic toFollyDynamic(const NodeLocationScope scope,
                                       const FlowGroupPolicy& fgp);
  static folly::dynamic toFollyDynamic(const Priority pri,
                                       const FlowGroupPolicy::Entry& fgp_entry);
  void toFollyDynamic(folly::dynamic& result) const;
  virtual folly::dynamic toFollyDynamic() const {
    folly::dynamic result = folly::dynamic::object();
    ShapingConfig::toFollyDynamic(result);
    return result;
  }

  const std::set<NodeLocationScope>& getValidScopes() {
    return valid_scopes_;
  }

  std::map<NodeLocationScope, FlowGroupPolicy> flowGroupPolicies;
  virtual ~ShapingConfig() {}

 protected:
  ShapingConfig() = default;

 private:
  std::set<NodeLocationScope> valid_scopes_;
};

}}} // namespace facebook::logdevice::configuration
