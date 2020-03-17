/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/LocalRSMSnapshotStoreImpl.h"

#include "logdevice/common/debug.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

namespace facebook { namespace logdevice {
void WriteRsmSnapshotStorageTask::execute() {
  LocalLogStore& store = storageThreadPool_->getLocalLogStore();
  RsmSnapshotMetadata metadata{
      snapshot_ver_,
      std::move(snapshot_blob_),
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())};
  LocalLogStore::WriteOptions options;
  int rv = store.writeLogMetadata(log_id_, metadata, options);
  if (!rv) {
    status_ = E::OK;
    ld_debug("Successfully wrote RsmSnapshotMetadata(%s) for log:%lu",
             metadata.toString().c_str(),
             log_id_.val_);
  } else {
    status_ = E::FAILED;
    ld_error("Could not write RsmSnapshotMetadata for log %lu: %s",
             log_id_.val_,
             error_description(err));
  }
}

void WriteRsmSnapshotStorageTask::onDone() {
  ld_debug("key_:%lu, status_:%s, snapshot_ver_:%s",
           log_id_.val_,
           error_name(status_),
           lsn_to_string(snapshot_ver_).c_str());
  cb_(status_, status_ == E::OK ? snapshot_ver_ : LSN_INVALID);
}

void WriteRsmSnapshotStorageTask::onDropped() {
  ld_error("StorageTask for key:%lu dropped.", log_id_.val_);
  cb_(E::FAILED, LSN_INVALID);
}

void ReadRsmSnapshotStorageTask::execute() {
  LocalLogStore& store = storageThreadPool_->getLocalLogStore();
  int rv = store.readLogMetadata(log_id_, &metadata_out_);
  if (rv == 0) {
    status_ = E::OK;
  } else {
    status_ = err;
    ld_info("Failed to read RsmSnapshotMetadata for log %lu, err:%s",
            log_id_.val_,
            error_description(err));
  }
}

void ReadRsmSnapshotStorageTask::onDone() {
  if (status_ == E::OK) {
    if (metadata_out_.version_ < min_ver_) {
      status_ = E::STALE;
    }
  }
  ld_info("key:%lu, status_:%s, min_ver:%s, metadata(%s)",
          log_id_.val_,
          error_name(status_),
          lsn_to_string(min_ver_).c_str(),
          metadata_out_.toString().c_str());
  cb_(status_,
      status_ == E::OK ? metadata_out_.snapshot_blob_ : "",
      RSMSnapshotStore::SnapshotAttributes(
          metadata_out_.version_, metadata_out_.update_time_));
}

void ReadRsmSnapshotStorageTask::onDropped() {
  ld_error("StorageTask for key:%lu dropped.", log_id_.val_);
  cb_(E::DROPPED,
      "",
      RSMSnapshotStore::SnapshotAttributes(
          LSN_INVALID, std::chrono::milliseconds(0)));
}

void LocalRSMSnapshotStoreImpl::getVersion(snapshot_ver_cb_t cb) {
  auto cb_read_storage_task =
      [this, cb](Status st,
                 std::string /* unused */,
                 RSMSnapshotStore::SnapshotAttributes snapshot_attrs) {
        cb(st, snapshot_attrs.base_version);
      };
  auto task = std::make_unique<ReadRsmSnapshotStorageTask>(
      logid_t(folly::to<logid_t::raw_type>(key_)),
      LSN_OLDEST,
      std::move(cb_read_storage_task));
  auto task_queue =
      ServerWorker::onThisThread()->getStorageTaskQueueForShard(shard_id_);
  task_queue->putTask(std::move(task));
}

void LocalRSMSnapshotStoreImpl::getSnapshot(lsn_t min_ver, snapshot_cb_t cb) {
  if (read_in_flight_) {
    cb(E::INPROGRESS,
       "",
       SnapshotAttributes(LSN_INVALID, std::chrono::milliseconds(0)));
    return;
  }

  read_in_flight_ = true;
  read_cb_ = std::move(cb);
  auto cb_read_storage_task =
      [this, min_ver](Status st,
                      std::string snapshot_blob_out,
                      RSMSnapshotStore::SnapshotAttributes snapshot_attrs) {
        read_cb_(st, std::move(snapshot_blob_out), snapshot_attrs);
        read_in_flight_ = false;
      };
  auto task = std::make_unique<ReadRsmSnapshotStorageTask>(
      logid_t(folly::to<logid_t::raw_type>(key_)),
      min_ver,
      std::move(cb_read_storage_task));
  auto task_queue =
      ServerWorker::onThisThread()->getStorageTaskQueueForShard(shard_id_);
  task_queue->putTask(std::move(task));
}

void LocalRSMSnapshotStoreImpl::writeSnapshot(lsn_t snapshot_ver,
                                              std::string snapshot_blob,
                                              completion_cb_t cb) {
  if (!isWritable()) {
    cb(E::NOTSUPPORTED, LSN_INVALID);
    return;
  }

  if (write_in_flight_) {
    cb(E::INPROGRESS, LSN_INVALID);
    return;
  }
  ld_info("key:%s, ver:%s, payload size:%lu",
          key_.c_str(),
          lsn_to_string(snapshot_ver).c_str(),
          snapshot_blob.size());

  write_in_flight_ = true;
  write_cb_ = std::move(cb);
  auto cb_write_storage_task = [this](Status st, lsn_t snapshot_ver_out) {
    write_cb_(st, snapshot_ver_out);
    write_in_flight_ = false;
  };
  auto task = std::make_unique<WriteRsmSnapshotStorageTask>(
      logid_t(folly::to<logid_t::raw_type>(key_)),
      snapshot_ver,
      std::move(snapshot_blob),
      std::move(cb_write_storage_task));
  auto task_queue =
      ServerWorker::onThisThread()->getStorageTaskQueueForShard(shard_id_);
  task_queue->putTask(std::move(task));
}

}} // namespace facebook::logdevice
