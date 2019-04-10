/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/GetEpochRecoveryMetadataRequest.h"
#include "logdevice/common/Metadata.h"
#include "logdevice/common/NodeSetAccessor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/ShardAuthoritativeStatusMap.h"
#include "logdevice/common/WeakRefHolder.h"
#include "logdevice/common/WorkerCallbackHelper.h"
#include "logdevice/server/storage/SealStorageTask.h"

namespace facebook { namespace logdevice {

/**
 * @file  PurgeSingleEpoch is the state machine responsible for purging one
 *        unclean epoch in purging.
 *
 *
 *  TODO: describe the workflow
 *  TODO: add a note on availability and current correctness issues
 */

class PurgeUncleanEpochs;
class TraceLogger;

class PurgeSingleEpoch {
 public:
  enum class State {
    UNKNOWN,
    // TODO: T23693338 remove this stage once
    // all servers are running version >= GET_EPOCH_RECOVERY_RANGE_SUPPORT
    GET_RECOVERY_METADATA,
    PURGE_RECORDS,
    WRITE_RECOVERY_METADATA,
    FINISHED
  };

  PurgeSingleEpoch(logid_t log_id,
                   shard_index_t shard,
                   epoch_t purge_to,
                   epoch_t epoch,
                   std::shared_ptr<EpochMetaData> epoch_metadata,
                   esn_t local_lng,
                   esn_t local_last_record,
                   Status status,
                   EpochRecoveryMetadata erm,
                   PurgeUncleanEpochs* driver);

  void start();

  void onGetEpochRecoveryMetadataComplete(
      Status status,
      const EpochRecoveryStateMap& epochRecoveryStateMap);

  void onPurgeRecordsTaskDone(Status status);

  void onWriteEpochRecoveryMetadataDone(Status status);

  const char* getStateString(bool shorter) const;

  epoch_t getEpoch() const {
    return epoch_;
  }

  PurgeUncleanEpochs* getParent() const {
    return driver_;
  }

  virtual StatsHolder* getStats();

  virtual ~PurgeSingleEpoch() {}

  // for creating weakref to itself for storage tasks created.
  WeakRefHolder<PurgeSingleEpoch> ref_holder_;

 protected:
  virtual std::unique_ptr<BackoffTimer> createRetryTimer();

  virtual void startStorageTask(std::unique_ptr<StorageTask>&& task);

  virtual void postGetEpochRecoveryMetadataRequest();

  virtual void deferredComplete();

 private:
  const logid_t log_id_;
  const shard_index_t shard_;
  const epoch_t purge_to_;
  const epoch_t epoch_;
  std::shared_ptr<EpochMetaData> epoch_metadata_;
  esn_t local_lng_;
  esn_t local_last_record_;
  Status get_epoch_recovery_metadata_status_{E::UNKNOWN};
  EpochRecoveryMetadata recovery_metadata_;

  PurgeUncleanEpochs* const driver_;

  std::unique_ptr<BackoffTimer> retry_timer_ = nullptr;
  std::unique_ptr<Timer> deferredCompleteTimer_;

  State state_{State::UNKNOWN};

  // final result of purging, we only conclude purging if result_ is E::OK or
  // E::NOTFOUND (log removed from the config). Otherwise PurgeSingleEpoch will
  // keep retrying.
  Status result_{E::UNKNOWN};

  // Used to ensure that callback when GetEpochRecoveryMetadataRequest completes
  // gets called on the worker that is running this PurgeSingleEpoch machine
  WorkerCallbackHelper<PurgeSingleEpoch> callbackHelper_;

  void purgeRecords();

  void writeEpochRecoveryMetadata();

  // complete the state machine by transistion it to State::FINISHED. The state
  // machine is not immediately finalized in this call
  void complete(Status status);

  // called at the end of each external events. Finalize the state machine if
  // it is in State::FINISHED
  void finalizeIfDone();

  static const char* getStateString(State state, bool shorter);
};

class PurgeDeleteRecordsStorageTask : public StorageTask {
 public:
  PurgeDeleteRecordsStorageTask(logid_t log_id,
                                epoch_t epoch,
                                esn_t start_esn,
                                esn_t end_esn,
                                WeakRef<PurgeSingleEpoch> driver);

  void execute() override;
  void executeImpl(LocalLogStore& store,
                   StatsHolder* stats,
                   TraceLogger* logger);
  void onDone() override;
  void onDropped() override;
  StorageTaskPriority getPriority() const override {
    return StorageTaskPriority::HIGH;
  }

  Durability durability() const override {
    // do not require syncing the delete task immediately.
    // The reason is that local LCE is advanced after delete is completed, so
    // if deletes are not persisted, it is likely that local LCE is not advanced
    // persistently either. The epoch will need to be purged again.
    return Durability::ASYNC_WRITE;
  }

 private:
  const logid_t log_id_;
  const epoch_t epoch_;
  const esn_t start_esn_;
  const esn_t end_esn_;
  WeakRef<PurgeSingleEpoch> driver_;
  Status status_;

  // if the ESN range contains less or equal number of records than this
  // threshold, delete key by key directly. Otherwise, create an iterator
  // to read actual records for deletion
  static const size_t PURGE_DELETE_BY_KEY_THRESHOLD = 4096;
};

class PurgeWriteEpochRecoveryMetadataStorageTask : public StorageTask {
 public:
  PurgeWriteEpochRecoveryMetadataStorageTask(
      logid_t log_id,
      epoch_t epoch,
      std::unique_ptr<EpochRecoveryMetadata> metadata,
      WeakRef<PurgeSingleEpoch> driver)
      : StorageTask(StorageTask::Type::PURGE_WRITE_EPOCH_RECOVERY_METADATA),
        log_id_(log_id),
        epoch_(epoch),
        metadata_(std::move(metadata)),
        driver_(std::move(driver)) {}

  void execute() override;
  void executeImpl(LocalLogStore& store);
  void onDone() override;
  void onDropped() override;
  StorageTaskPriority getPriority() const override {
    return StorageTaskPriority::HIGH;
  }

 private:
  const logid_t log_id_;
  const epoch_t epoch_;
  std::unique_ptr<EpochRecoveryMetadata> metadata_;
  WeakRef<PurgeSingleEpoch> driver_;
  Status status_;
};

}} // namespace facebook::logdevice
