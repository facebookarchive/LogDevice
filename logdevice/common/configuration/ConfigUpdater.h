/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

namespace facebook { namespace logdevice { namespace configuration {

class ConfigUpdater {
 public:
  virtual void invalidateConfig() = 0;
  virtual int fetchFromSource() = 0;
  virtual ~ConfigUpdater() = default;
};
}}} // namespace facebook::logdevice::configuration
