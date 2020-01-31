/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once
#include <string>

#include "logdevice/common/debug.h"
#include "logdevice/common/replicated_state_machine/LogBasedRSMSnapshotStore.h"
#include "logdevice/common/replicated_state_machine/RSMSnapshotStore.h"

namespace facebook { namespace logdevice {

class RsmSnapshotStoreFactory {
 public:
  static std::unique_ptr<RSMSnapshotStore>
  create(Processor* processor,
         SnapshotStoreType snapshot_store_type,
         bool storage_rw,
         logid_t snapshot_log,
         logid_t delta_log) {
    ld_info("Attempting to create snapshot store (type:%d, snapshot_log:%lu, "
            "delta_log:%lu)",
            (int)snapshot_store_type,
            snapshot_log.val_,
            delta_log.val_);

    switch (snapshot_store_type) {
      case SnapshotStoreType::NONE:
        return nullptr;
        break;

      case SnapshotStoreType::LOG:
        ld_info("Creating LogBasedRSMSnapshotStore on server.");
        return std::make_unique<LogBasedRSMSnapshotStore>(
            folly::to<std::string>(delta_log.val_),
            snapshot_log,
            processor,
            true /* allow snapshotting */);
        break;

      case SnapshotStoreType::MESSAGE:
        break;

      case SnapshotStoreType::LOCAL_STORE:
        break;
    }
    return nullptr;
  }
};
}} // namespace facebook::logdevice
