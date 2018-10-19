/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/rebuilding/WaitForPurgesRequest.h"

#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/RELEASE_Message.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/read_path/LogStorageState.h"
#include "logdevice/server/storage/PurgeCoordinator.h"

namespace facebook { namespace logdevice {

WaitForPurgesRequest::WaitForPurgesRequest(
    logid_t logid,
    NodeID seq,
    lsn_t up_to,
    folly::small_vector<shard_index_t> shards,
    Callback cb,
    UpdateableSettings<RebuildingSettings> rebuilding_settings)
    : FireAndForgetRequest(RequestType::WAIT_FOR_PURGES),
      logid_(logid),
      seq_(seq),
      upTo_(up_to),
      shards_(std::move(shards)),
      cb_(std::move(cb)),
      rebuildingSettings_(std::move(rebuilding_settings)) {}

void WaitForPurgesRequest::executionBody() {
  retryTimer_ = std::make_unique<ExponentialBackoffTimer>(

      [this]() { tryAgain(); }, rebuildingSettings_->wait_purges_backoff_time);

  tryAgain();
  // `this` may be destroyed.
}

void WaitForPurgesRequest::tryAgain() {
  const auto logcfg = Worker::getConfig()->getLogGroupByIDShared(
      MetaDataLog::dataLogID(logid_));
  if (!logcfg) {
    // The log was removed from the config.
    onComplete(E::NOTFOUND);
    return;
  }

  if (check()) {
    onComplete(E::OK);
  } else {
    retryTimer_->activate();
  }
}

void WaitForPurgesRequest::onComplete(Status status) {
  ld_check(cb_);
  cb_(status);
  destroy();
  // `this` is destroyed.
}

int WaitForPurgesRequest::getThreadAffinity(int nthreads) {
  return folly::hash::twang_mix64(logid_.val_) % nthreads;
}

bool WaitForPurgesRequest::check() {
  ld_check(!shards_.empty());

  auto it = shards_.begin();
  while (it != shards_.end()) {
    if (checkShardHasPurged(*it)) {
      it = shards_.erase(it);
    } else {
      ++it;
    }
  }

  return shards_.empty();
}

bool WaitForPurgesRequest::checkShardHasPurged(shard_index_t shard_idx) const {
  auto processor = ServerWorker::onThisThread()->processor_;
  auto cfg = Worker::getConfig();

  LogStorageState* log_state =
      processor->getLogStorageStateMap().insertOrGet(logid_, shard_idx);

  if (log_state == nullptr) {
    RATELIMIT_CRITICAL(
        std::chrono::seconds(10),
        10,
        "Failed to insert LogStorageState for log %lu, shard %u: %s",
        logid_.val_,
        shard_idx,
        error_description(err));
    return false;
  }

  const auto last_released = log_state->getLastReleasedLSN();

  if (last_released.hasValue() && last_released.value() >= upTo_) {
    return true;
  }

  // Make sure purging is triggered by simulating the sequencer sending a
  // RELEASE.
  log_state->purge_coordinator_->onReleaseMessage(
      upTo_, seq_, ReleaseType::GLOBAL, true /* do_release */);
  return false;
}

}} // namespace facebook::logdevice
