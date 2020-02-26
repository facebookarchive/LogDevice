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
         std::string key) {
    ld_info("Attempting to create snapshot store (type:%d, key:%s)",
            static_cast<int>(snapshot_store_type),
            key.c_str());

    switch (snapshot_store_type) {
      case SnapshotStoreType::LEGACY:
        return nullptr;
        break;

      case SnapshotStoreType::LOG: {
        logid_t delta_log = logid_t(folly::to<logid_t::raw_type>(key));
        logid_t snapshot_log;
        // Find out snapshot log from delta log
        switch (delta_log.val_) {
          case configuration::InternalLogs::CONFIG_LOG_DELTAS.val_:
            snapshot_log = configuration::InternalLogs::CONFIG_LOG_SNAPSHOTS;
            break;
          case configuration::InternalLogs::EVENT_LOG_DELTAS.val_:
            snapshot_log = configuration::InternalLogs::EVENT_LOG_SNAPSHOTS;
            break;
          case configuration::InternalLogs::MAINTENANCE_LOG_DELTAS.val_:
            snapshot_log =
                configuration::InternalLogs::MAINTENANCE_LOG_SNAPSHOTS;
            break;
          default:
            ld_error("Invalid key:%s", key.c_str());
            return nullptr;
        };
        ld_info("Creating LogBasedRSMSnapshotStore on server.");
        return std::make_unique<LogBasedRSMSnapshotStore>(
            key, snapshot_log, processor, true /* allow snapshotting */);
      } break;

      case SnapshotStoreType::MESSAGE:
        break;

      case SnapshotStoreType::LOCAL_STORE:
        break;
    }
    return nullptr;
  }
};
}} // namespace facebook::logdevice
