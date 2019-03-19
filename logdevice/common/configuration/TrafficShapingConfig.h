/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <array>

#include "logdevice/common/configuration/ShapingConfig.h"

namespace facebook { namespace logdevice { namespace configuration {

struct TrafficShapingConfig : public ShapingConfig {
  TrafficShapingConfig();

  folly::dynamic toFollyDynamic() const;

  TrafficClass default_read_traffic_class = TrafficClass::READ_BACKLOG;
};

}}} // namespace facebook::logdevice::configuration
