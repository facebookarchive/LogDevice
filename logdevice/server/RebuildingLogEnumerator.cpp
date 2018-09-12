/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/RebuildingLogEnumerator.h"

#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/LegacyLogToShard.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"
#include "logdevice/server/RebuildingEnumerateMetadataLogsTask.h"

namespace facebook { namespace logdevice {

void RebuildingLogEnumerator::start() {
  auto cur_timestamp = RecordTimestamp::now();

  auto logs_config = config_->getLogsConfig();
  ld_check(logs_config->isLocal());
  ld_check(logs_config->isFullyLoaded());
  auto local_logs_config =
      checked_downcast<configuration::LocalLogsConfig*>(logs_config.get());
  for (auto it = local_logs_config->logsBegin();
       it != local_logs_config->logsEnd();
       ++it) {
    const logid_t logid(it->first);

    // Tests don't rebuild internal logs.
    if (!rebuild_internal_logs_ &&
        configuration::InternalLogs::isInternal(logid)) {
      continue;
    }

    // Let's try and approximate the next timestamp for this log. If the log has
    // no backlog configured, it is set to -inf. Otherwise, the next timestamp
    // is the current timestamp minus the backlog value.
    // Note that this value does not have to be precise. The goal here is to
    // maximize the chances that the first time we read a batch for a log we
    // will read some records instead of having the batch stop as soon as it
    // encounters the first record.
    const auto& backlog =
        it->second.log_group->attrs().backlogDuration().value();
    RecordTimestamp next_ts = RecordTimestamp::min();
    if (backlog.hasValue()) {
      next_ts = cur_timestamp - backlog.value();
    }
    // Don't start lower than the lower bound of a time-ranged rebuild.
    next_ts.storeMax(min_timestamp_);

    // TODO: T31009131 stop using the getLegacyShardIndexForLog() function
    // altogether.
    if (getLegacyShardIndexForLog(logid, num_shards_) == shard_idx_ ||
        !rebuilding_settings_->use_legacy_log_to_shard_mapping_in_rebuilding) {
      ld_assert(result_.find(logid) == result_.end());
      result_.emplace(logid, next_ts);
    }
  }
  if (rebuild_metadata_logs_) {
    putStorageTask();
  } else {
    finalize();
  }
}

void RebuildingLogEnumerator::putStorageTask() {
  auto task = std::make_unique<RebuildingEnumerateMetadataLogsTask>(
      ref_holder_.ref(), num_shards_);
  auto task_queue =
      ServerWorker::onThisThread()->getStorageTaskQueueForShard(shard_idx_);
  task_queue->putTask(std::move(task));
}

void RebuildingLogEnumerator::onMetaDataLogsStorageTaskDone(
    Status st,
    std::vector<logid_t> log_ids) {
  if (st != E::OK) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    1,
                    "Unable to enumerate metadata logs for rebuilding on shard "
                    "%u, version %s: %s. Retrying...",
                    shard_idx_,
                    lsn_to_string(version_).c_str(),
                    error_description(st));
    putStorageTask();
    return;
  }
  for (logid_t l : log_ids) {
    result_.emplace(l, min_timestamp_);
  }
  finalize();
}

void RebuildingLogEnumerator::onMetaDataLogsStorageTaskDropped() {
  // Retrying
  RATELIMIT_WARNING(std::chrono::seconds(10),
                    1,
                    "Storage task for enumerating metadata logs dropped for "
                    "rebuilding on shard %u, version %s. Retrying...",
                    shard_idx_,
                    lsn_to_string(version_).c_str());
  putStorageTask();
}

void RebuildingLogEnumerator::finalize() {
  ld_check(!finalize_called_);
  finalize_called_ = true;
  callback_->onLogsEnumerated(shard_idx_, version_, std::move(result_));
  // `this` may be destroyed here.
}

}} // namespace facebook::logdevice
