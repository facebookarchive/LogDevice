/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/storage/LocalLogStoreUtils.h"

#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/AuditLogFile.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"

namespace facebook { namespace logdevice { namespace LocalLogStoreUtils {

int updateTrimPoints(const TrimPointUpdateMap& trim_points,
                     ServerProcessor* processor,
                     LocalLogStore& store,
                     bool sync,
                     StatsHolder* /*stats*/) {
  E error = E::OK;
  LogStorageStateMap* state_map =
      processor ? &processor->getLogStorageStateMap() : nullptr;

  for (auto& kv : trim_points) {
    TrimMetadata metadata{kv.second};
    int rv = store.updateLogMetadata(kv.first, metadata);
    if (rv != 0 && err != E::UPTODATE) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      1,
                      "Unable to update the trim point in Store metadata "
                      "for log %lu: %s",
                      kv.first.val_,
                      error_description(err));
      error = E::LOCAL_LOG_STORE_WRITE;
      continue;
    }
    auto server_processor = checked_downcast<ServerProcessor*>(processor);
    log_trim_movement(*server_processor, store, kv.first, kv.second);

    if (state_map) {
      LogStorageState* log_state =
          state_map->insertOrGet(kv.first, store.getShardIdx());
      if (log_state == nullptr) {
        RATELIMIT_ERROR(std::chrono::seconds(1),
                        1,
                        "Unable to create log_state in LogStorageStateMap "
                        "for log %lu: %s",
                        kv.first.val_,
                        error_description(err));
        error = err;
        continue;
      }

      // updateTrimPoint will only fail with E::UPTODATE
      log_state->updateTrimPoint(metadata.trim_point_);
    }
  }

  if (error != E::OK) {
    err = error;
    return -1;
  }

  if (sync) {
    return store.sync(Durability::ASYNC_WRITE);
  }

  return 0;
}

void updatePerEpochLogMetadataTrimPoints(
    shard_index_t shard_idx,
    const PerEpochLogMetadataTrimPointUpdateMap& metadata_trim_points,
    ServerProcessor* processor) {
  if (processor == nullptr) {
    return;
  }
  LogStorageStateMap* state_map = &processor->getLogStorageStateMap();

  for (const auto& kv : metadata_trim_points) {
    LogStorageState* log_state = state_map->insertOrGet(kv.first, shard_idx);
    if (log_state == nullptr) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      1,
                      "Unable to create log_state in LogStorageStateMap "
                      "for log %lu: %s",
                      kv.first.val_,
                      error_description(err));
      continue;
    }

    // updatePerEpochLogMetadataTrimPoint will only fail with E::UPTODATE
    log_state->updatePerEpochLogMetadataTrimPoint(kv.second);
  }
}

}}} // namespace facebook::logdevice::LocalLogStoreUtils
