/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/storage_tasks/RecoverSealTask.h"

#include <folly/Memory.h>

#include "logdevice/common/CompletionRequest.h"
#include "logdevice/common/Metadata.h"
#include "logdevice/common/debug.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

namespace facebook { namespace logdevice {

void RecoverSealTask::execute() {
  SealMetadata seal_metadata;
  SoftSealMetadata softseal_metadata;

  auto read_seal_metadata = [&, this](SealMetadata& metadata) {
    int rv = storageThreadPool_->getLocalLogStore().readLogMetadata(
        log_id_, &metadata);
    if (rv == 0) {
      // readLogMetadata() checks Metadata::valid().
      ld_check(metadata.seal_.validOrEmpty());
    } else if (err != E::NOTFOUND) {
      status_ = err;
      return -1;
    } else { // err == E::NOTFOUND
      // if there's nothing in the store, we'll use the default-initialized
      // seal record (with EPOCH_INVALID)
      ld_check(status_ == E::OK);
    }

    return 0;
  };

  status_ = E::OK;

  if (read_seal_metadata(seal_metadata) == 0 &&
      read_seal_metadata(softseal_metadata) == 0) {
    ld_check(status_ == E::OK);
    // if NOTFOUND, use the default initialized seal in metadata
    seals_.setSeal(LogStorageState::SealType::NORMAL, seal_metadata.seal_);
    seals_.setSeal(LogStorageState::SealType::SOFT, softseal_metadata.seal_);
  } else {
    // there must be some error
    ld_check(status_ != E::OK);
  }
}

void RecoverSealTask::onDone() {
  invokeCallback();
}

void RecoverSealTask::onDropped() {
  status_ = E::DROPPED;
  invokeCallback();
}

void RecoverSealTask::invokeCallback() {
  ld_check(status_ == E::OK || status_ == E::LOCAL_LOG_STORE_READ ||
           status_ == E::DROPPED);

  // We were created after checking the LogStorageState instance so it must
  // exist
  LogStorageState& state =
      ServerWorker::onThisThread()->processor_->getLogStorageStateMap().get(
          log_id_, storageThreadPool_->getShardIdx());

  RecordCache* cache = state.record_cache_.get();
  if (cache != nullptr) {
    cache->updateLastNonAuthoritativeEpoch(log_id_);
  }

  auto update_seals = [&, this](LogStorageState::SealType type) {
    bool stale = false;
    // must have value on E::OK
    const Seal seal = seals_.getSeal(type);
    int rv = state.updateSeal(seal, type);
    if (rv != 0) {
      // if LogStorageStateMap already contains a seal record with a higher
      // epoch, the value read from local log store is stale; we'll need to
      // reread it from the map
      ld_check(err == E::UPTODATE);
      stale = (err == E::UPTODATE);
    }

    if (stale) {
      // LogStorageStateMap has a more recent value. Although unlikely,
      // it may happen nevertheless if a log is sealed after we (attempted to)
      // read the seal record from the local log store.
      folly::Optional<Seal> current_seal = state.getSeal(type);
      ld_check(current_seal.hasValue());
      seals_.setSeal(type, current_seal.value());
    }
  };

  if (status_ == E::OK) {
    // update both seals
    update_seals(LogStorageState::SealType::NORMAL);
    update_seals(LogStorageState::SealType::SOFT);
  } else if (status_ == E::LOCAL_LOG_STORE_READ) {
    state.notePermanentError("Recovering seal");
  }

  callback_(status_, seals_);
}
}} // namespace facebook::logdevice
