/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/TrafficShapingConfig.h"

#include <folly/dynamic.h>

namespace facebook { namespace logdevice { namespace configuration {

TrafficShapingConfig::TrafficShapingConfig()
    : ShapingConfig(/* valid_scopes */
                    {NodeLocationScope::NODE,
                     NodeLocationScope::RACK,
                     NodeLocationScope::ROW,
                     NodeLocationScope::CLUSTER,
                     NodeLocationScope::DATA_CENTER,
                     NodeLocationScope::REGION,
                     NodeLocationScope::ROOT},
                    /* configured_scopes */
                    {NodeLocationScope::NODE, NodeLocationScope::ROOT}) {
  static_assert(static_cast<int>(NodeLocationScope::INVALID) == 7, "");
}

folly::dynamic TrafficShapingConfig::toFollyDynamic() const {
  folly::dynamic result =
      folly::dynamic::object("default_read_traffic_class",
                             trafficClasses()[default_read_traffic_class]);
  ShapingConfig::toFollyDynamic(result);
  return result;
}

}}} // namespace facebook::logdevice::configuration
