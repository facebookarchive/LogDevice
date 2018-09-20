/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/CopySetSelectorDependencies.h"

namespace facebook { namespace logdevice {

const NodeAvailabilityChecker*
CopySetSelectorDependencies::getNodeAvailability() const {
  return NodeAvailabilityChecker::instance();
}

const CopySetSelectorDependencies* CopySetSelectorDependencies::instance() {
  static CopySetSelectorDependencies d;
  return &d;
}

}} // namespace facebook::logdevice
