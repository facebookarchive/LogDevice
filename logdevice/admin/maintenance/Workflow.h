/**
 *  Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree.
 **/
#pragma once

#include <folly/Function.h>
#include <folly/futures/Future.h>

#include "logdevice/admin/maintenance/types.h"
#include "logdevice/common/AuthoritativeStatus.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/membership/StorageState.h"

namespace facebook { namespace logdevice { namespace maintenance {

/**
 * A workflow is a state machine that tracks state transitions of a shard or a
 * sequencer node. This object is only accessible by the `MaintenanceManager`
 * and its internal state is only manipulated through the `run()` method.
 */
class Workflow {
 public:
  Workflow() = 0;

  ~Workflow() {}

  MaintenanceStatus getStatus() const;

 protected:
  MaintenanceStatus status_;
  // Last time the state of the workflow changed
  SystemTimestamp last_updated_at_;
};

}}} // namespace facebook::logdevice::maintenance
