/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#define __STDC_FORMAT_MACROS
#include "logdevice/server/storage_tasks/RecoverLogStateTask.h"

#include <stdlib.h>

#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/RELEASE_Message.h"
#include "logdevice/server/ReleaseRequest.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/storage_tasks/StorageThread.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

namespace facebook { namespace logdevice {

void RecoverLogStateTask::execute() {
  static_assert(LSN_INVALID < LSN_OLDEST,
                "LSN_INVALID has to be the smaller than any valid LSN");

  LocalLogStore& store = storageThreadPool_->getLocalLogStore();
  readTrimPoint(store);
  readLastReleased(store);
  readLastClean(store);
}

void RecoverLogStateTask::readTrimPoint(LocalLogStore& store) {
  TrimMetadata trim_metadata{LSN_INVALID};
  int rv = store.readLogMetadata(log_id_, &trim_metadata);
  if (rv == 0 || err == E::NOTFOUND) {
    trim_metadata_ = trim_metadata;
    if (rv != 0) {
      ld_debug("Trim point not found for log %" PRIu64, log_id_.val_);
    }
  } else {
    failed_ = true;
    RATELIMIT_WARNING(std::chrono::seconds(10),
                      1,
                      "Could not read trim point from local log store "
                      "for log %" PRIu64 ": %s",
                      log_id_.val_,
                      error_description(err));
  }
}

void RecoverLogStateTask::readLastReleased(LocalLogStore& store) {
  LastReleasedMetadata last_released_metadata{LSN_INVALID};
  int rv = store.readLogMetadata(log_id_, &last_released_metadata);
  if (rv == 0 || err == E::NOTFOUND) {
    last_released_metadata_ = last_released_metadata;
    if (rv != 0) {
      ld_debug("Last released LSN not found for log %" PRIu64, log_id_.val_);
    }
  } else {
    failed_ = true;
    RATELIMIT_WARNING(std::chrono::seconds(10),
                      1,
                      "Could not read last released LSN from local log store "
                      "for log %" PRIu64 ": %s",
                      log_id_.val_,
                      error_description(err));
  }
}

void RecoverLogStateTask::readLastClean(LocalLogStore& store) {
  LastCleanMetadata last_clean_metadata{epoch_t(0)};
  int rv = store.readLogMetadata(log_id_, &last_clean_metadata);
  if (rv == 0 || err == E::NOTFOUND) {
    last_clean_metadata_ = last_clean_metadata;
    if (rv != 0) {
      ld_debug("Last clean epoch metadata entry not found for log %" PRIu64,
               log_id_.val_);
    }
  } else {
    failed_ = true;
    RATELIMIT_WARNING(std::chrono::seconds(10),
                      1,
                      "Could not read LCE from local log store for "
                      "log %" PRIu64 ": %s",
                      log_id_.val_,
                      error_description(err));
  }
}

void RecoverLogStateTask::onDone() {
  ServerProcessor* processor = ServerWorker::onThisThread()->processor_;
  ld_check(processor->runningOnStorageNode());

  // We were created after checking the LogStorageState instance so it must
  // exist
  LogStorageState& log_state = processor->getLogStorageStateMap().get(
      log_id_, storageThreadPool_->getShardIdx());

  RecordCache* cache = log_state.record_cache_.get();
  if (cache != nullptr) {
    cache->updateLastNonAuthoritativeEpoch(log_id_);
  }

  int rv;

  // We update the trim point regardless of whether we found it in the local
  // log store.  Worst case is we deliver more data to the client than they
  // wanted.
  if (trim_metadata_.hasValue()) {
    rv = log_state.updateTrimPoint(trim_metadata_.value().trim_point_);
    if (rv != 0 && err != E::UPTODATE) {
      RATELIMIT_WARNING(std::chrono::seconds(1),
                        1,
                        "Unable to update the trim point for log %lu: %s",
                        log_id_.val_,
                        error_description(err));
      return;
    }
  }

  // Last released LSN is also updated even if we haven't found it persisted in
  // the local log store. Readers don't care (unitialized or LSN_INVALID have
  // the same effect), while rebuilding will wait until it actually receives
  // the last released LSN from the sequencer (LastReleasedSource::RELEASE).

  if (last_released_metadata_.hasValue()) {
    rv = log_state.updateLastReleasedLSN(
        last_released_metadata_.value().last_released_lsn_,
        LogStorageState::LastReleasedSource::LOCAL_LOG_STORE);
    if (rv != 0 && err != E::UPTODATE) {
      RATELIMIT_WARNING(
          std::chrono::seconds(1),
          1,
          "Unable to update the last released LSN for log %lu: %s",
          log_id_.val_,
          error_description(err));
      return;
    }
  }

  // Update the in-memory last clean epoch if we were able to read it.  If the
  // metadata entry was not found in the local log store, we treat that as a
  // last clean epoch of 0.
  if (last_clean_metadata_.hasValue()) {
    epoch_t epoch = last_clean_metadata_.value().epoch_;
    log_state.purge_coordinator_->updateLastCleanInMemory(epoch);
  }

  // If we failed to read some of the metadata, tell readers to not expect the
  // LogStorageState to be populated.
  if (failed_) {
    log_state.notePermanentError("Reading log metadata");
  }

  // Grab the latest last released LSN for this log (this task found it or it
  // was already available) and notify workers to ping all read streams for
  // this log (so that reading that was blocked on getting the trim point or
  // last released LSN can continue).
  //
  // If we don't yet have a last released lsn (reading from local log store
  // failed), we'll call broadcastReleaseRequest() with LSN_INVALID, prompting
  // CatchupQueue to try again.
  //
  // TODO replace with proper subscription mechanism?

  auto state = log_state.getLastReleasedLSN();
  lsn_t released_lsn = state.hasValue() ? state.value() : LSN_INVALID;
  RecordID rid{lsn_to_esn(released_lsn), lsn_to_epoch(released_lsn), log_id_};

  // ReleaseRequests will cause Worker threads to retry reading
  ReleaseRequest::broadcastReleaseRequest(
      processor,
      rid,
      storageThreadPool_->getShardIdx(),
      [&](worker_id_t worker) { return log_state.isWorkerSubscribed(worker); },
      true // force (to make sure CatchupQueue tries to read again)
  );

  log_state.noteLogStateRecovered();
}

void RecoverLogStateTask::onDropped() {
  RATELIMIT_ERROR(
      std::chrono::seconds(10),
      10,
      "Dropped RecoverLogStateTask for log %lu.  Reading may stall.",
      log_id_.val_);
  // Should only happen in case of shutdown since we override highPriority()
  // to true
  ld_check(Worker::onThisThread()->shuttingDown());
}

void RecoverLogStateTask::getDebugInfoDetailed(
    StorageTaskDebugInfo& info) const {
  info.log_id = log_id_;
}

}} // namespace facebook::logdevice
