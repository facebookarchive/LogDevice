/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#define __STDC_FORMAT_MACROS // pull in PRId64 etc
#include "logdevice/server/storage_tasks/EpochOffsetStorageTask.h"

#include <memory>

#include "logdevice/common/Processor.h"
#include "logdevice/common/debug.h"
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
                                               epoch_t epoch,
                                               ThreadType thread_type,
                                               StorageTaskPriority priority)
    : StorageTask(StorageTask::Type::EPOCH_OFFSET),
      stream_(std::move(stream)),
      log_id_(log_id),
      epoch_(epoch),
      thread_type_(thread_type),
      priority_(priority) {
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
    ld_check(metadata.epoch_end_offsets_.getCounter(BYTE_OFFSET) ==
             metadata.header_.epoch_end_offset);
    result_offsets_ = metadata.epoch_end_offsets_;
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
  ld_check(metadata.epoch_end_offsets_.getCounter(BYTE_OFFSET) ==
           metadata.header_.epoch_end_offset);
  ld_check(metadata.epoch_size_map_.getCounter(BYTE_OFFSET) ==
           metadata.header_.epoch_size);
  result_offsets_ = OffsetMap::getOffsetsDifference(
      std::move(metadata.epoch_end_offsets_), metadata.epoch_size_map_);
}

void EpochOffsetStorageTask::onDone() {
  ServerWorker::onThisThread()->serverReadStreams().onEpochOffsetTask(*this);
}

void EpochOffsetStorageTask::onDropped() {
  ld_check(!result_offsets_.isValid());
  ServerWorker::onThisThread()->serverReadStreams().onEpochOffsetTask(*this);
}
}} // namespace facebook::logdevice
