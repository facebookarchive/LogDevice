/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/plugin/Plugin.h"

namespace facebook { namespace logdevice {

/**
 * @file `HotTextOptimizer` will be used to optimize function placement of the
 * binary for improved performance. See
 * https://research.fb.com/wp-content/uploads/2017/01/cgo2017-hfsort-final1.pdf
 */

class HotTextOptimizerPlugin : public Plugin {
 public:
  PluginType type() const override {
    return PluginType::HOT_TEXT_OPTIMIZER;
  }

  /**
   * Places hot text on large pages to improve performance.
   */
  virtual void operator()() = 0;
};

}} // namespace facebook::logdevice
