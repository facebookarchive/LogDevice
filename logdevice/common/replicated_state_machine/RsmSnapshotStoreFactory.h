/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once
#include <string>

#include "logdevice/common/Processor.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/replicated_state_machine/LogBasedRSMSnapshotStore.h"
#include "logdevice/common/replicated_state_machine/MessageBasedRSMSnapshotStore.h"
#include "logdevice/common/replicated_state_machine/RSMSnapshotStore.h"

namespace facebook { namespace logdevice {

class RsmSnapshotStoreFactory {
 public:
  static std::unique_ptr<RSMSnapshotStore>
  create(Processor* processor,
         SnapshotStoreType snapshot_store_type,
         std::string key,
         bool is_server) {
    ld_info(
        "Attempting to create snapshot store (type:%d, key:%s, is_server:%d)",
        static_cast<int>(snapshot_store_type),
        key.c_str(),
        is_server);

    switch (snapshot_store_type) {
      case SnapshotStoreType::LEGACY:
        STAT_INCR(processor->stats_, rsm_legacy_snapshot_store_init);
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
        ld_info("Creating LogBasedRSMSnapshotStore");
        STAT_INCR(processor->stats_, rsm_log_snapshot_store_init);
        return std::make_unique<LogBasedRSMSnapshotStore>(
            key, snapshot_log, processor, is_server /* allow snapshotting */);
      } break;

      case SnapshotStoreType::LOCAL_STORE:
        ld_check(!is_server);
        // Intentional fall through for cases when client settings
        // doesn't have store type as Message.
      case SnapshotStoreType::MESSAGE:
        STAT_INCR(processor->stats_, rsm_msg_snapshot_store_init);
        ld_info("Creating MessageBasedRSMSnapshotStore");
        return std::make_unique<MessageBasedRSMSnapshotStore>(key);
        break;
      default:
        ld_error("Invaild SnapshotStoreType:%d",
                 static_cast<int>(snapshot_store_type));
    }
    return nullptr;
  }
};
}} // namespace facebook::logdevice
