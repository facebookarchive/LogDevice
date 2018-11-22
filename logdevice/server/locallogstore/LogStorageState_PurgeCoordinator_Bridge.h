/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/NodeID.h"
#include "logdevice/common/OffsetMap.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

enum class ReleaseType : uint8_t;

/**
 * Temporary bridge interface for a subset of PurgeCoordinator functionality
 * needed by LogStorageState.  This allows the PurgeCoordinator implementation
 * to live in server/.  When LogStorageState is also in server/, this interface
 * can be removed.
 */
class LogStorageState_PurgeCoordinator_Bridge {
 public:
  virtual ~LogStorageState_PurgeCoordinator_Bridge() {}
  virtual void onReleaseMessage(lsn_t,
                                NodeID from,
                                ReleaseType,
                                bool do_broadcast,
                                OffsetMap epoch_offsets = OffsetMap()) = 0;
  virtual void updateLastCleanInMemory(epoch_t epoch) = 0;
};

}} // namespace facebook::logdevice
