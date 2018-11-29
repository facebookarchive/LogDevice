/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/RebuildingLogEnumerator.h"

#include "logdevice/common/LegacyLogToShard.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/server/RebuildingEnumerateMetadataLogsTask.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"

namespace facebook { namespace logdevice {

void RebuildingLogEnumerator::start() {
  auto cur_timestamp = RecordTimestamp::now();

  auto logs_config = config_->getLogsConfig();
  ld_check(logs_config->isLocal());
  ld_check(logs_config->isFullyLoaded());
  auto local_logs_config =
      checked_downcast<configuration::LocalLogsConfig*>(logs_config.get());

  uint32_t internalSkipped = 0;
  uint32_t dataSkipped = 0;

  const auto use_legacy_mapping =
      rebuilding_settings_->use_legacy_log_to_shard_mapping_in_rebuilding;

  std::vector<bool> is_shard_present(max_num_shards_, false);
  for (auto& p : parameters_) {
    const auto shard_idx = p.first;
    is_shard_present[shard_idx] = true;
  }

  for (auto it = local_logs_config->logsBegin();
       it != local_logs_config->logsEnd();
       ++it) {
    const logid_t logid(it->first);

    // Tests don't rebuild internal logs.
    if (!rebuild_internal_logs_ &&
        configuration::InternalLogs::isInternal(logid)) {
      internalSkipped++;
      continue;
    }

    // Let's try and approximate the next timestamp for this log. If the log
    // has no retention/backlog configured, it is set to -inf. Otherwise, the
    // next timestamp is the current timestamp minus the backlog value. Note
    // that this value does not have to be precise. The goal here is to maximize
    // the chances that the first time we read a batch for a log we will read
    // some records instead of having the batch stop as soon as it encounters
    // the first record.
    const auto& backlog =
        it->second.log_group->attrs().backlogDuration().value();

    // FIXME: Ideally we want to delay SHARD_IS_REBUILT past the
    // maxBacklogDuration only if we have logs relevant to the failed shard. But
    // not sure if it's possible to determine that without performing copy-set
    // iteration. Simpler to just track the biggest backlog.
    if (rebuilding_settings_->disable_data_log_rebuilding &&
        !MetaDataLog::isMetaDataLog(logid) && backlog.hasValue()) {
      // We want to skip over data logs with a finite backlog but we don't
      // want to notify that the shard is rebuilt until after the contents
      // of the longest-lived log, since rebuild was requested, has expired.
      //
      // This ensures that readers will correctly account for the shard as
      // still rebuilding for the purpose of FMAJORITY calculation. To
      // accomplish this, we track the log with the max backlog and only
      // trigger SHARD_IS_REBUILT after that logs current data has expired.
      if (backlog.value() > maxBacklogDuration_) {
        maxBacklogDuration_ = backlog.value();
      }
      dataSkipped++;
      continue;
    }

    RecordTimestamp next_ts = RecordTimestamp::min();
    if (backlog.hasValue()) {
      next_ts = cur_timestamp - backlog.value();
    }

    // TODO: T31009131 stop using the getLegacyShardIndexForLog() function
    // altogether.
    int64_t dest_shard = getLegacyShardIndexForLog(logid, max_num_shards_);
    if (use_legacy_mapping && !is_shard_present[dest_shard]) {
      continue; // log does not belong to any shards being rebuilt
    }

    results_.emplace(logid, next_ts);
  }
  ld_info("Enumerator skipped %d internal and %d data logs. Queued %ld logs "
          "for rebuild.",
          internalSkipped,
          dataSkipped,
          results_.size());

  /* figure out the metadata logs for shards that have requested them */
  for (const auto& p : parameters_) {
    auto shard_idx = p.first;
    auto params = p.second;

    if (params.rebuild_metadata_logs) {
      shard_storage_tasks_remaining_.insert(shard_idx);
      putStorageTask(shard_idx);
    }
  }
  maybeFinalize();
  // `this` may be destroyed here.
}

void RebuildingLogEnumerator::putStorageTask(uint32_t shard_idx) {
  auto task = std::make_unique<RebuildingEnumerateMetadataLogsTask>(
      ref_holder_.ref(), max_num_shards_);
  auto task_queue =
      ServerWorker::onThisThread()->getStorageTaskQueueForShard(shard_idx);
  task_queue->putTask(std::move(task));
}

void RebuildingLogEnumerator::onMetaDataLogsStorageTaskDone(
    Status st,
    uint32_t shard_idx,
    std::vector<logid_t> log_ids) {
  if (!shard_storage_tasks_remaining_.count(shard_idx)) {
    return; // ignore
  } else if (st != E::OK) {
    const auto& params = parameters_.at(shard_idx);
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    1,
                    "Unable to enumerate metadata logs for rebuilding on shard "
                    "%u, version %s: %s. Retrying...",
                    shard_idx,
                    lsn_to_string(params.version).c_str(),
                    error_description(st));
    putStorageTask(shard_idx);
  } else {
    for (logid_t l : log_ids) {
      results_.emplace(l, RecordTimestamp::min());
    }
    shard_storage_tasks_remaining_.erase(shard_idx);
    maybeFinalize();
    // `this` may be destroyed here.
  }
}

void RebuildingLogEnumerator::onMetaDataLogsStorageTaskDropped(
    uint32_t shard_idx) {
  // Retrying
  const auto& params = parameters_.at(shard_idx);
  RATELIMIT_WARNING(std::chrono::seconds(10),
                    1,
                    "Storage task for enumerating metadata logs dropped for "
                    "rebuilding on shard %u, version %s. Retrying...",
                    shard_idx,
                    lsn_to_string(params.version).c_str());
  putStorageTask(shard_idx);
}

void RebuildingLogEnumerator::abortShardIdx(shard_index_t shard_idx) {
  parameters_.erase(shard_idx);
  shard_storage_tasks_remaining_.erase(shard_idx);
  maybeFinalize();
  // `this` may be destroyed here.
}

void RebuildingLogEnumerator::maybeFinalize() {
  ld_check(!finalized_);

  if (shard_storage_tasks_remaining_.size() > 0) {
    std::string shards_remaining_str = "";
    for (auto shard : shard_storage_tasks_remaining_) {
      shards_remaining_str += " " + std::to_string(shard);
    }
    RATELIMIT_INFO(std::chrono::seconds(1),
                   1,
                   "Waiting storage tasks for shards: %s",
                   shards_remaining_str.c_str());
    return;
  }

  finalized_ = true;
  callback_->onLogsEnumerated(std::move(results_), maxBacklogDuration_);
  // `this` may be destroyed here.
}

}} // namespace facebook::logdevice
