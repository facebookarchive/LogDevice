/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/NodeAvailabilityChecker.h"

namespace facebook { namespace logdevice {

// Encapsulates the common dependencies of all copyset selectors.
// Can be mocked, allowing testing copyset selectors in isolation.
class CopySetSelectorDependencies {
 public:
  CopySetSelectorDependencies() {}
  virtual ~CopySetSelectorDependencies() {}

  /**
   * Provide an object that tells which nodes are ready to store records.
   */
  virtual const NodeAvailabilityChecker* getNodeAvailability() const;

  // Returns a singleton instance.
  static const CopySetSelectorDependencies* instance();
};

}} // namespace facebook::logdevice
