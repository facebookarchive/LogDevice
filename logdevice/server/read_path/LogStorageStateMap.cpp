/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/read_path/LogStorageStateMap.h"

#include <folly/Memory.h>
#include <folly/Optional.h>
#include <folly/ScopeGuard.h>

#include "logdevice/common/debug.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Err.h"
#include "logdevice/server/RecordCacheDisposal.h"
#include "logdevice/server/ServerProcessor.h"

namespace facebook { namespace logdevice {

LogStorageStateMap::LogStorageStateMap(
    shard_size_t num_shards,
    std::chrono::microseconds recovery_interval,
    ServerProcessor* processor)
    : cache_disposal_(processor != nullptr &&
                              processor->settings()->enable_record_cache
                          ? std::make_unique<RecordCacheDisposal>(processor)
                          : nullptr),
      num_shards_(num_shards),
      processor_(processor),
      shard_map_(makeMap(num_shards)),
      state_recovery_interval_(recovery_interval) {
  if (processor != nullptr && processor->settings()->enable_record_cache &&
      processor->runningOnStorageNode()) {
    // only starts the record cache monitor thread if record cache
    // is enabled
    record_cache_monitor_ =
        std::make_unique<RecordCacheMonitorThread>(processor);
  }
}

LogStorageState* LogStorageStateMap::insertOrGet(logid_t log_id,
                                                 shard_index_t shard_idx) {
  Map& map = *shard_map_[shard_idx];

  // First try a lookup to avoid memory allocation in the common case
  auto it = map.find(log_id.val_);
  if (it != map.cend()) {
    return it->second.get();
  }

  // No state for this log yet.
  auto insert_result =
      map.emplace(log_id.val(),
                  std::make_unique<LogStorageState>(
                      log_id, shard_idx, this, cache_disposal_.get()));

  // Whether or not we were the ones to insert or some other thread beat us
  // to it, return a pointer to whatever ended up in the map.
  return insert_result.first->second.get();
}

LogStorageState* LogStorageStateMap::find(logid_t log_id,
                                          shard_index_t shard_idx) {
  ld_check(shard_idx < shard_map_.size());
  Map& map = *shard_map_[shard_idx];
  auto it = map.find(log_id.val_);
  return it != map.cend() ? it->second.get() : nullptr;
}

LogStorageState& LogStorageStateMap::get(logid_t log_id,
                                         shard_index_t shard_idx) {
  ld_check(shard_idx < shard_map_.size());
  Map& map = *shard_map_[shard_idx];
  auto it = map.find(log_id.val_);
  ld_check(it != map.cend());
  return *(it->second);
}

void LogStorageStateMap::clear() {
  for (shard_index_t s = 0; s < num_shards_; ++s) {
    Map& map = *shard_map_[s];
    map.clear();
  }
}

int LogStorageStateMap::repopulateRecordCacheFromLinearBuffer(
    logid_t log_id,
    shard_index_t shard,
    const char* buffer,
    size_t size) {
  std::unique_ptr<RecordCache> record_cache =
      RecordCache::fromLinearBuffer(buffer, size, cache_disposal_.get(), shard);
  if (!record_cache) {
    return -1;
  }

  auto log_storage_state = insertOrGet(log_id, shard);
  if (!log_storage_state) {
    RATELIMIT_WARNING(std::chrono::seconds(30),
                      2,
                      "Failed to create or get log storage state for log %lu",
                      log_id.val());
    return -1;
  }
  log_storage_state->record_cache_ = std::move(record_cache);
  return 0;
}

LogStorageStateMap::ReleaseStates
LogStorageStateMap::getAllLastReleasedLSNs(shard_index_t shard) const {
  ReleaseStates states;

  for (auto entry = shard_map_[shard]->cbegin();
       entry != shard_map_[shard]->cend();
       ++entry) {
    LogStorageState::LastReleasedLSN last_released =
        entry->second->getLastReleasedLSN();
    if (last_released.hasValue()) {
      states.emplace_back(logid_t(entry->first), last_released.value());
    }
  }

  return states;
}

int LogStorageStateMap::recoverLogState(logid_t log_id,
                                        shard_index_t shard_idx,
                                        LogStorageState::RecoverContext ctx,
                                        bool force_ask_sequencer) {
  LogStorageState* log_state = insertOrGet(log_id, shard_idx);
  if (log_state == nullptr) {
    // Failed to insert into map, cannot make progress
    err = E::FAILED;
    return -1;
  }
  return log_state->recover(state_recovery_interval_, ctx, force_ask_sequencer);
}

void LogStorageStateMap::shutdownRecordCaches() {
  if (cache_disposal_ == nullptr) {
    return;
  }
  for (shard_index_t i = 0; i < num_shards_; ++i) {
    for (auto entry = shard_map_[i]->cbegin(); entry != shard_map_[i]->cend();
         ++entry) {
      if (entry->second->record_cache_ != nullptr) {
        entry->second->record_cache_->shutdown();
      }
    }
  }
}

void LogStorageStateMap::shutdownRecordCacheMonitor() {
  record_cache_monitor_.reset();
}

ServerProcessor* LogStorageStateMap::getProcessor() {
  return processor_;
}
StatsHolder* LogStorageStateMap::getStats() {
  return processor_ ? processor_->stats_ : nullptr;
}

std::vector<std::unique_ptr<LogStorageStateMap::Map>>
LogStorageStateMap::makeMap(shard_size_t num_shards) {
  std::vector<std::unique_ptr<Map>> ret;

  for (shard_index_t s = 0; s < num_shards; ++s) {
    ret.push_back(std::make_unique<Map>());
  }

  return ret;
}

}} // namespace facebook::logdevice
