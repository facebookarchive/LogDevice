/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <gmock/gmock.h>

#include "logdevice/common/configuration/ShapingConfig.h"

namespace facebook { namespace logdevice { namespace configuration {

class MockShapingConfig : public configuration::ShapingConfig {
 public:
  MOCK_CONST_METHOD0(toFollyDynamic, folly::dynamic());
  MOCK_CONST_METHOD1(configured, bool(NodeLocationScope));
};

}}} // namespace facebook::logdevice::configuration
