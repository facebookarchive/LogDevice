/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/ZookeeperEpochStoreRequest.h"

#include <folly/Memory.h>

#include "logdevice/common/Worker.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

ZookeeperEpochStoreRequest::ZookeeperEpochStoreRequest(
    logid_t logid,
    epoch_t epoch,
    EpochStore::CompletionMetaData cf,
    ZookeeperEpochStore* store)
    : store_(store),
      logid_(logid),
      epoch_store_shutting_down_(store_->getShuttingDownPtr()),
      epoch_(epoch),
      cf_meta_data_(cf),
      worker_idx_(Worker::onThisThread(false /* enforce_worker */)
                      ? Worker::onThisThread()->idx_
                      : worker_id_t(-1)) {
  ld_check(logid_ != LOGID_INVALID);
  ld_check(store_);
}

ZookeeperEpochStoreRequest::ZookeeperEpochStoreRequest(
    logid_t logid,
    epoch_t epoch,
    EpochStore::CompletionLCE cf,
    ZookeeperEpochStore* store)
    : store_(store),
      logid_(logid),
      epoch_store_shutting_down_(store_->getShuttingDownPtr()),
      epoch_(epoch),
      cf_lce_(cf),
      worker_idx_(Worker::onThisThread(false /* enforce_worker */)
                      ? Worker::onThisThread()->idx_
                      : worker_id_t(-1)) {
  ld_check(logid_ != LOGID_INVALID);
  ld_check(store_);
}

}} // namespace facebook::logdevice
