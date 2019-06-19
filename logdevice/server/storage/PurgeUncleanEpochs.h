/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <map>
#include <memory>
#include <utility>
#include <vector>

#include <folly/IntrusiveList.h>
#include <folly/Optional.h>

#include "logdevice/common/AdminCommandTable-fwd.h"
#include "logdevice/common/IntrusiveUnorderedMap.h"
#include "logdevice/common/NodeSetFinder.h"
#include "logdevice/common/ResourceBudget.h"
#include "logdevice/common/Seal.h"
#include "logdevice/common/WeakRefHolder.h"
#include "logdevice/common/WorkerCallbackHelper.h"
#include "logdevice/common/protocol/GET_EPOCH_RECOVERY_METADATA_Message.h"
#include "logdevice/common/protocol/GET_EPOCH_RECOVERY_METADATA_REPLY_Message.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/Record.h"
#include "logdevice/server/storage/SealStorageTask.h"
#include "logdevice/server/storage_tasks/StorageTask.h"

namespace facebook { namespace logdevice {

/**
 * @file PurgeUncleanEpochs is a state machine that runs on storage nodes.
 * Its main responsibility is to delete any records that were deleted during
 * log recovery that this node did not participate in (because it was
 * unavailable during recovery).
 *
 * A full example scenario where this is needed:
 * - The sequencer is writing a record with LSN 10.  The record is written
 *   successfully on this storage node but not any others.
 * - The sequencer crashes.
 * - Another sequencer initiates recovery but cannot reach this storage node.
 *   During recovery the sequencer concludes that record 10 does not exist and
 *   acts accordingly.
 * - This storage node becomes reachable again and starts seeing RELEASEs for
 *   the log in a newer epoch.  It must not immediately accept the release and
 *   ship record 10, because recovery decided this record should not exist.
 *   Instead, the node launches a PurgeUncleanEpochs state machine which will
 *   talk to storage nodes that did participate in recovery, and delete from
 *   its local log store whatever should not exist.
 *
 * The following describes its workflow:
 *
 * 1. read the last clean epoch metadata from the local log store if it is not
 *    present in LogStorageState, otherwise skip to step 2.
 *
 * 2. Issue a SealStorageTask (the same task used for log recovery) to seal
 *    the epoch of purge_to_, as well as getting information (e.g., last known
 *    good esn, last record esn, etc) about all non-empty epochs in the epoch
 *    range of [lce+1, purge_to_]. Note that these information can usually be
 *    obtained from the RecordCache and do not need to access data key space.
 *
 * 3. get epoch metadata (i.e., replication attribute) for all epoch needs to
 *    to purged using NodeSetFinder
 *
 * 4. For every epoch, get EpochRecoveryMetadata from other servers. Note:
 *    We will fetch the EpochRecoveryMetadata for an epoch even if the epoch
 *    is empty locally.
 *
 * 5. With the information obtained by step 2,3, and 4, create one
 *    PurgeSingleEpoch state machine for each epochs. Start
 *    them in parallel. See doc in PurgeSingleEpoch.h for how purging purges a
 *    single epoch.
 *
 * 5. Once all PurgeSingleEpoch machines are completes and all epochs finish
 *    purging, start a storage task to bump the local last clean epoch to
 *    either purge_to_ (for RELEASE message) or purge_to_ + 1 (for CLEAN
 *    message). Notify PurgeCoordinator for completion.
 */
class BackoffTimer;
class EpochRecoveryMetadata;
class Timer;
class LocalLogStore;
class LogStorageState;
class PurgeCoordinator;
class PurgeSingleEpoch;
class StatsHolder;

class PurgeUncleanEpochs : public IntrusiveUnorderedMapHook {
 public:
  PurgeUncleanEpochs(PurgeCoordinator* parent,
                     logid_t log_id,
                     shard_index_t shard,
                     folly::Optional<epoch_t> current_last_clean_epoch,
                     epoch_t purge_to,
                     epoch_t new_last_clean_epoch,
                     NodeID node,
                     StatsHolder* stats);

  enum class State {
    UNKNOWN,
    READ_LAST_CLEAN,
    GET_PURGE_EPOCHS,
    GET_EPOCH_METADATA,
    GET_EPOCH_RECOVERY_METADATA,
    RUN_PURGE_EPOCHS,
    WRITE_LAST_CLEAN,
    PERMANENT_ERROR
  };

  // Used by IntrusiveUnorderedMap

  class Key {
   public:
    // In the current implementation, there can only be at most one active
    // PurgeUncleanEpochs state machine for a (log, shard_index_t) pair at the
    // any given time. Moreover, PurgeUncleanEpoch does not abort or retry until
    // it successfully purged epoch _purge_to_. So it is sufficient to use
    // <log_id, shard_index_t, purge_to> tuple to identify the state machine.
    logid_t log_id;
    epoch_t purge_to;
    shard_index_t shard;

    bool operator==(const Key& rhs) const {
      return log_id == rhs.log_id && purge_to == rhs.purge_to &&
          shard == rhs.shard;
    }
  };

  Key getKey() const {
    return Key{log_id_, purge_to_, shard_};
  }

  void start();

  /**
   * Called after PurgeReadLastCleanTask completes.
   */
  void onLastCleanRead(Status, epoch_t last_clean);

  using EpochInfoMap = SealStorageTask::EpochInfoMap;
  using EpochInfoSource = SealStorageTask::EpochInfoSource;
  /**
   * Called after SealStorageTask completes
   */
  void onGetPurgeEpochsDone(Status status,
                            std::unique_ptr<EpochInfoMap> map,
                            EpochInfoSource src = EpochInfoSource::INVALID);

  /**
   * Called after GetEpochRecoveryMetadataRequest completes.
   */
  void onGetEpochRecoveryMetadataComplete(
      Status status,
      const EpochRecoveryStateMap& epochRecoveryStateMap);

  /**
   * Called after one PurgeSingleEpoch state machine completes
   */
  void onPurgeSingleEpochDone(epoch_t epoch, Status status);

  /**
   * Called after PurgeWriteLastCleanTask completes.
   */
  void onWriteLastCleanDone(Status status);

  // @see PurgeCoordinator::onPermanentError.
  void onPermanentError(const char* context, Status status);

  // get a string of all epochs of currently active PurgeSingleEpoch state
  // machine
  std::string getActiveEpochsStr() const;

  void getDebugInfo(InfoPurgesTable& table);

  PurgeCoordinator* getParent() const {
    return parent_;
  }

  StatsHolder* getStats() {
    return stats_;
  }

  // delay used to retry on various stage of the purging process
  static const std::chrono::milliseconds INITIAL_RETRY_DELAY;
  static const std::chrono::milliseconds MAX_RETRY_DELAY;

  /**
   * Attach a resource token to the PurgeUncleanEpochs machine. The token is
   * used to limit the number of concurrent purges initiated by RELEASE
   * messages.
   */
  void setResourceToken(ResourceBudget::Token token) {
    token_ = std::move(token);
  }

  void onShutdown();

  virtual ~PurgeUncleanEpochs();

  // for creating weakref to itself for storage tasks created.
  WeakRefHolder<PurgeUncleanEpochs> ref_holder_;

 protected:
  virtual std::unique_ptr<BackoffTimer> createTimer();

  virtual void startStorageTask(std::unique_ptr<StorageTask>&& task);

  virtual const std::shared_ptr<const Configuration> getClusterConfig() const;

  virtual std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const;

  virtual const std::shared_ptr<LogsConfig> getLogsConfig() const;

  virtual void startReadingMetaData();

  virtual void startPurgingEpochs();

  virtual void complete(Status status);

  virtual void postEpochRecoveryMetadataRequest(
      epoch_t start,
      epoch_t end,
      std::shared_ptr<EpochMetaData> epochMetadata);

  // This is a wrapper around NodeSetFinder's get result for convenience
  // of testing.
  virtual std::shared_ptr<const EpochMetaDataMap> getEpochMetadataMap() {
    return nodeset_finder_->getResult();
  }

  virtual NodeID getMyNodeID() const;

  PurgeCoordinator* const parent_;

  const logid_t log_id_;
  const shard_index_t shard_;

  folly::Optional<epoch_t> current_last_clean_epoch_;
  // purge upto this epoch
  const epoch_t purge_to_;
  // New "last clean epoch" entry to write into the local log store once we
  // are done purging
  const epoch_t new_last_clean_epoch_;
  // the sequencer node that initiated purging
  const NodeID node_;
  StatsHolder* const stats_;

  ResourceBudget::Token token_;

 private:
  State state_{State::UNKNOWN};

  std::unique_ptr<BackoffTimer> retry_timer_;

  // stores EpochInfo of all **non-empty** epoch in the range
  // [last_clean_ + 1, seal_epoch_]
  std::unique_ptr<EpochInfoMap> epoch_info_;

  // NodeSetFinder is used to fetch epoch metadata.
  // Unused (nullptr) if the log being recovered is
  // a metadata log.
  std::unique_ptr<NodeSetFinder> nodeset_finder_;

  // bool flag indicating that we have collected all epoch metadata needed
  bool epoch_metadata_finalized_{false};

  // Populated in NodeSetFinder callback - onHistoricalMetadata
  std::shared_ptr<const EpochMetaDataMap> metadata_map_ = nullptr;

  // The first and last epoch to purge by this state machine
  // as determined by response for SealStorageTask.
  // see onGetPurgeEpochsDone on how these are computed
  epoch_t first_epoch_to_purge_;
  epoch_t last_epoch_to_purge_;

  // a map of PurgeSingleEpoch state machines
  std::map<epoch_t, PurgeSingleEpoch> purge_epochs_;

  // Used when --skip-recovery setting is set to complete this state machine in
  // the next iteration of the event loop.
  std::unique_ptr<Timer> deferred_complete_timer_;

  // Used to ensure that callback when GetEpochRecoveryMetadataRequest completes
  // gets called on the worker that is running this PurgeUncleanEpochs machine
  WorkerCallbackHelper<PurgeUncleanEpochs> callback_helper_;

  // count of number of GetEpochRecoveryMetadataRequest that are
  // running
  uint32_t num_running_get_erm_requests_ = 0;

  // create PurgeSingleEpoch state machine for the epoch
  void createPurgeSingleEpochMachine(epoch_t start_epoch,
                                     Status status,
                                     EpochRecoveryMetadata erm);

  void readLastCleanEpoch();

  // start SealStorageTask to seal the log and get information about all
  // non-empty epochs needs to be purged
  void getPurgeEpochs();

  // read the metadata log to find epoch metadata for each epoch
  void getEpochMetaData();

  // callback called when epoch metadata is read by nodeset_finder_
  void onHistoricalMetadata(Status st);

  void onAllEpochMetaDataGotten();

  void writeLastClean();

  void allEpochsPurged();
  virtual const Settings& getSettings() const;

  // return the replication attribute for the metadata log, fetched from the
  // config
  std::unique_ptr<const EpochMetaData> getEpochMetaDataForMetaDataLogs() const;

  // helper function to get the active PurgeSingleEpoch machine for a given
  // epoch. return nullptr if no such machine is found for the epoch
  PurgeSingleEpoch* getActivePurgeSingleEpoch(epoch_t epoch);

  static const char* stateString(State state);

  friend class PurgeUncleanEpochsTest;
  friend class MockPurgeUncleanEpochs;
};

class PurgeReadLastCleanTask : public StorageTask {
 public:
  PurgeReadLastCleanTask(logid_t log_id, WeakRef<PurgeUncleanEpochs> driver)
      : StorageTask(StorageTask::Type::PURGE_READ_LAST_CLEAN),
        log_id_(log_id),
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
  WeakRef<PurgeUncleanEpochs> driver_;
  Status status_;
  epoch_t result_;
};

class PurgeWriteLastCleanTask : public StorageTask {
 public:
  PurgeWriteLastCleanTask(logid_t log_id,
                          epoch_t epoch,
                          WeakRef<PurgeUncleanEpochs> driver)
      : StorageTask(StorageTask::Type::PURGE_WRITE_LAST_CLEAN),
        log_id_(log_id),
        epoch_(epoch),
        driver_(std::move(driver)) {}

  void execute() override;
  // @param log_state may be nullptr in tests
  void executeImpl(LocalLogStore& store, LogStorageState* log_state);
  Durability durability() const override {
    return Durability::SYNC_WRITE;
  }
  void onDone() override;
  void onDropped() override;
  StorageTaskPriority getPriority() const override {
    return StorageTaskPriority::HIGH;
  }

 private:
  const logid_t log_id_;
  epoch_t epoch_;
  WeakRef<PurgeUncleanEpochs> driver_;
  Status status_;
};

// Wrapper instead of typedef to allow forward-declaring in Worker.h
struct PurgeUncleanEpochsMap {
  struct Hasher {
    size_t operator()(const PurgeUncleanEpochs::Key& k) const {
      return folly::hash::hash_combine(k.log_id.val_, k.purge_to.val_, k.shard);
    }
  };
  struct KeyExtractor {
    PurgeUncleanEpochs::Key operator()(const PurgeUncleanEpochs& p) const {
      return p.getKey();
    }
  };
  struct Disposer {
    void operator()(PurgeUncleanEpochs* p) {
      p->onShutdown();
    }
  };

  explicit PurgeUncleanEpochsMap(size_t nbuckets) : map(nbuckets) {}
  IntrusiveUnorderedMap<PurgeUncleanEpochs,
                        PurgeUncleanEpochs::Key,
                        KeyExtractor,
                        Hasher,
                        std::equal_to<PurgeUncleanEpochs::Key>,
                        Disposer>
      map;
};

}} // namespace facebook::logdevice
