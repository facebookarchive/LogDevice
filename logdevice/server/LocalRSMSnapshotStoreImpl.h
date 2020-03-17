/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>

#include "logdevice/common/Metadata.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/replicated_state_machine/MessageBasedRSMSnapshotStore.h"
#include "logdevice/common/replicated_state_machine/RSMSnapshotStore.h"
#include "logdevice/server/storage_tasks/StorageTask.h"

/**
 * @file Implementation of Local RSMSnapshotStore on top of LogsDB
 */

namespace facebook { namespace logdevice {
class WriteRsmSnapshotStorageTask : public StorageTask {
 public:
  explicit WriteRsmSnapshotStorageTask(logid_t log_id,
                                       lsn_t snapshot_ver,
                                       std::string snapshot_blob,
                                       RSMSnapshotStore::completion_cb_t cb)
      : StorageTask(StorageTask::Type::WRITE_RSM_SNAPSHOT),
        log_id_(log_id),
        snapshot_ver_(snapshot_ver),
        snapshot_blob_(std::move(snapshot_blob)),
        cb_(std::move(cb)) {}

  StorageTaskPriority getPriority() const override {
    return StorageTaskPriority::MID;
  }

  Principal getPrincipal() const override {
    return Principal::METADATA;
  }

  void execute() override;
  void onDone() override;
  void onDropped() override;
  bool isDroppable() const override {
    return true;
  }

 private:
  logid_t log_id_;
  lsn_t snapshot_ver_;
  std::string snapshot_blob_;
  RSMSnapshotStore::completion_cb_t cb_;
  Status status_{E::UNKNOWN};
};

class ReadRsmSnapshotStorageTask : public StorageTask {
 public:
  explicit ReadRsmSnapshotStorageTask(logid_t log_id,
                                      lsn_t min_ver,
                                      RSMSnapshotStore::snapshot_cb_t cb)
      : StorageTask(StorageTask::Type::READ_RSM_SNAPSHOT),
        log_id_(log_id),
        min_ver_(min_ver),
        cb_(std::move(cb)) {}

  void execute() override;
  void onDone() override;
  void onDropped() override;

  StorageTaskPriority getPriority() const override {
    return StorageTaskPriority::MID;
  }

  Principal getPrincipal() const override {
    return Principal::METADATA;
  }

 private:
  logid_t log_id_;
  lsn_t min_ver_;
  RSMSnapshotStore::snapshot_cb_t cb_;
  Status status_{E::UNKNOWN};
  RsmSnapshotMetadata metadata_out_;
};

class LocalRSMSnapshotStoreImpl : public RSMSnapshotStore {
 public:
  explicit LocalRSMSnapshotStoreImpl(std::string key,
                                     bool writable,
                                     shard_index_t shard_id = 0)
      : RSMSnapshotStore(key, writable), shard_id_(shard_id) {}

  void getSnapshot(lsn_t min_ver, snapshot_cb_t) override;

  void writeSnapshot(lsn_t snapshot_ver,
                     std::string snapshot_blob,
                     completion_cb_t cb) override;

  void getVersion(snapshot_ver_cb_t cb) override;

  void getDurableVersion(snapshot_ver_cb_t cb) override {
    // TODO: This will be filled in a future diff which adds support
    // for distributing Durable versions via Gossip
    cb(E::NOTSUPPORTED, LSN_INVALID);
  }

 private:
  shard_index_t shard_id_{-1};
  snapshot_cb_t read_cb_;
  completion_cb_t write_cb_;
  bool read_in_flight_{false};
  bool write_in_flight_{false};
};
}} // namespace facebook::logdevice
