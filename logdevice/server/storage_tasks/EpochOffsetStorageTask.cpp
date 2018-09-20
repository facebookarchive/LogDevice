/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#define __STDC_FORMAT_MACROS // pull in PRId64 etc
#include "EpochOffsetStorageTask.h"

#include <memory>

#include "logdevice/common/debug.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/protocol/RECORD_Message.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/read_path/AllServerReadStreams.h"
#include "logdevice/server/read_path/LocalLogStoreReader.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

namespace facebook { namespace logdevice {

EpochOffsetStorageTask::EpochOffsetStorageTask(WeakRef<ServerReadStream> stream,
                                               logid_t log_id,
                                               epoch_t epoch)
    : StorageTask(StorageTask::Type::EPOCH_OFFSET),
      stream_(std::move(stream)),
      log_id_(log_id),
      epoch_(epoch),
      result_offset_(BYTE_OFFSET_INVALID) {
  ld_check(stream_);
}

void EpochOffsetStorageTask::execute() {
  if (!stream_) { // Only read if the ServerReadStream still exists.
    return;
  }
  // First try to read previous epoch
  LocalLogStore& store = storageThreadPool_->getLocalLogStore();
  EpochRecoveryMetadata metadata;
  int rv = store.readPerEpochLogMetadata(log_id_,
                                         epoch_t(epoch_.val_ - 1),
                                         &metadata,
                                         false, // find_last_available
                                         true); // allow_blocking_io
  if (rv == 0) {
    status_ = E::OK;
    result_offset_ = metadata.header_.epoch_end_offset;
    return;
  } else if (rv != 0 && err == E::LOCAL_LOG_STORE_READ) {
    ld_error("Error while reading PerEpochLogMetadata in "
             "EpochOffsetStorageTask for epoch %u",
             epoch_.val_ - 1);
    status_ = err;
    return;
  } else {
    ld_check(err == E::NOTFOUND || err == E::WOULDBLOCK);
    // Optimization failed. Continue looking for epoch offset in current epoch.
  }

  rv = store.readPerEpochLogMetadata(log_id_,
                                     epoch_,
                                     &metadata,
                                     false, // find_last_available
                                     true); // allow_blocking_io
  if (rv != 0) {
    ld_check(rv == -1);
    status_ = err;
    return;
  }
  result_offset_ =
      metadata.header_.epoch_end_offset - metadata.header_.epoch_size;
}

void EpochOffsetStorageTask::onDone() {
  ServerWorker::onThisThread()->serverReadStreams().onEpochOffsetTask(*this);
}

void EpochOffsetStorageTask::onDropped() {
  ld_check(result_offset_ == BYTE_OFFSET_INVALID);
  ServerWorker::onThisThread()->serverReadStreams().onEpochOffsetTask(*this);
}
}} // namespace facebook::logdevice
