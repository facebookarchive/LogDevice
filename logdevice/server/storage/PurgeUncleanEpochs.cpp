/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/storage/PurgeUncleanEpochs.h"

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/EpochMetaDataMap.h"
#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/include/Record.h"
#include "logdevice/server/RecordCache.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/locallogstore/WriteOps.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/storage/PurgeCoordinator.h"
#include "logdevice/server/storage/PurgeSingleEpoch.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

namespace facebook { namespace logdevice {

const std::chrono::milliseconds PurgeUncleanEpochs::INITIAL_RETRY_DELAY{1000};
const std::chrono::milliseconds PurgeUncleanEpochs::MAX_RETRY_DELAY{30000};

PurgeUncleanEpochs::PurgeUncleanEpochs(
    PurgeCoordinator* parent,
    logid_t log_id,
    shard_index_t shard,
    folly::Optional<epoch_t> current_last_clean_epoch,
    epoch_t purge_to,
    epoch_t new_last_clean_epoch,
    NodeID node,
    StatsHolder* stats)
    : ref_holder_(this),
      parent_(parent),
      log_id_(log_id),
      shard_(shard),
      current_last_clean_epoch_(current_last_clean_epoch),
      purge_to_(purge_to),
      new_last_clean_epoch_(new_last_clean_epoch),
      node_(node),
      stats_(stats),
      callback_helper_(this) {
  // the lce target must be at least purge_to_
  ld_check(purge_to_ <= new_last_clean_epoch);
}

void PurgeUncleanEpochs::start() {
  STAT_INCR(stats_, purging_started);
  ld_spew("log %lu, shard %u: purging epochs up to %u "
          "(PurgeUncleanEpochs this = %p)",
          log_id_.val_,
          shard_,
          purge_to_.val_,
          this);

  retry_timer_ = createTimer();
  Worker* w = Worker::onThisThread(false);
  if (w && Worker::settings().skip_recovery) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   10,
                   "Skipping purging of epochs up to %u of log %lu of shard %u "
                   "because --skip-recovery setting is used. Setting lce to %u",
                   purge_to_.val_,
                   log_id_.val_,
                   shard_,
                   new_last_clean_epoch_.val_);

    // when skipping recovery and LCE is not in memory, just use EPOCH_INVALID
    // as the current LCE so that LCE will always be updated
    if (!current_last_clean_epoch_.hasValue()) {
      current_last_clean_epoch_ = EPOCH_INVALID;
    }
    // It is not safe to call allEpochsPurged() directly as this may cause more
    // PurgeUncleanEpochs state machines to be started. Ensure we trigger this
    // in the next iteration of the event loop.
    deferred_complete_timer_ =
        std::make_unique<Timer>([this] { allEpochsPurged(); });
    deferred_complete_timer_->activate(std::chrono::milliseconds(0));
    return;
  }

  if (current_last_clean_epoch_.hasValue()) {
    state_ = State::GET_PURGE_EPOCHS;
    getPurgeEpochs();
  } else {
    // last clean epoch is unknown, read it from the local log store
    state_ = State::READ_LAST_CLEAN;
    readLastCleanEpoch();
  }
}

void PurgeUncleanEpochs::readLastCleanEpoch() {
  ld_check(state_ == State::READ_LAST_CLEAN);
  ld_debug("Reading last clean epoch for log %lu purge upto epoch %u.",
           log_id_.val_,
           purge_to_.val_);

  startStorageTask(
      std::make_unique<PurgeReadLastCleanTask>(log_id_, ref_holder_.ref()));
}

void PurgeUncleanEpochs::onLastCleanRead(Status status, epoch_t last_clean) {
  ld_check(state_ == State::READ_LAST_CLEAN);
  if (status != E::OK) {
    if (status == E::FAILED) {
      // enter persisent error state, EpochRecovery should retry after a
      // timeout and the node will be excluded in the next round of digest.
      // In the meantime, this purging state machine will stay inactive until
      // the shard is repaired and logdeviced is restarted.
      onPermanentError("PurgeLCEEpochTask", status);
      return;
    }

    // for transient errors (e.g., task dropped) retry after a timeout
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    2,
                    "PurgeReadLastCleanTask failed with status %s, "
                    "starting timer",
                    error_description(status));
    retry_timer_->setCallback(
        std::bind(&PurgeUncleanEpochs::readLastCleanEpoch, this));
    retry_timer_->activate();
    return;
  }

  ld_spew("log %lu last clean epoch is %u", log_id_.val_, last_clean.val_);
  if (parent_) {
    parent_->updateLastCleanInMemory(last_clean);
  }
  current_last_clean_epoch_ = last_clean;

  state_ = State::GET_PURGE_EPOCHS;
  getPurgeEpochs();
}

void PurgeUncleanEpochs::getPurgeEpochs() {
  ld_check(state_ == State::GET_PURGE_EPOCHS);
  ld_check(current_last_clean_epoch_.hasValue());
  epoch_t last_clean = current_last_clean_epoch_.value();

  if (last_clean >= purge_to_) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   10,
                   "log %lu has already been cleaned. No purge needed. "
                   "last clean epoch: %u, purge to: %u.",
                   log_id_.val_,
                   last_clean.val_,
                   purge_to_.val_);
    allEpochsPurged();
    return;
  }

  ld_debug("Seal and get epoch information for log %lu with epoch range: "
           "[%u, %u]",
           log_id_.val_,
           last_clean.val_ + 1,
           purge_to_.val_);

  Seal seal(new_last_clean_epoch_, node_);
  startStorageTask(std::make_unique<SealStorageTask>(
      log_id_, last_clean, seal, ref_holder_.ref()));
}

void PurgeUncleanEpochs::onGetPurgeEpochsDone(
    Status status,
    std::unique_ptr<SealStorageTask::EpochInfoMap> epoch_info,
    SealStorageTask::EpochInfoSource src) {
  ld_check(state_ == State::GET_PURGE_EPOCHS);
  if (status != E::OK && status != E::PREEMPTED) {
    if (status == E::FAILED) {
      onPermanentError("PurgeSealStorageTask", status);
      return;
    }
    // for transient errors retry after a timeout
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    2,
                    "SealStorageTask (Purging) failed with status %s, "
                    "starting timer",
                    error_description(status));
    retry_timer_->setCallback(
        std::bind(&PurgeUncleanEpochs::getPurgeEpochs, this));
    retry_timer_->activate();
    return;
  }

  epoch_info_ = std::move(epoch_info);
  ld_check(epoch_info_ != nullptr);

  if (!epoch_info_->empty()) {
    // SealStorageTask should guarantee that the epochs in epoch_info_ must in
    // range of [lce + 1, new_last_clean_epoch]
    epoch_t last_clean = current_last_clean_epoch_.value();
    ld_check(epoch_info_->begin()->first >= last_clean.val_ + 1);
    ld_check(epoch_info_->rbegin()->first <= new_last_clean_epoch_.val_);

    // remove epochs _larger_ than purge_to_ from epoch_info since we should
    // only do purging until purge_to_
    auto it = epoch_info_->upper_bound(purge_to_.val_);
    epoch_info_->erase(it, epoch_info_->end());
  }

  if (epoch_info_->empty()) {
    if (!getSettings().get_erm_for_empty_epoch) {
      RATELIMIT_INFO(std::chrono::seconds(10),
                     10,
                     "All epochs in [%u, %u] in log %lu are empty. Consider "
                     "them all purged.",
                     current_last_clean_epoch_.value().val_ + 1,
                     purge_to_.val_,
                     log_id_.val_);

      // TODO (bridge record): we may still need to write bridge
      // record for empty epochs
      allEpochsPurged();
      return;
    }
  } else {
    // assert epochs larger than purge_to_ are removed from the map
    ld_check(epoch_info_->rbegin()->first <= purge_to_.val_);
    ld_debug("%lu non-empty epochs in the range of [%u, %u] for log %lu needs "
             "to be purged. Last clean epoch %u, purge to %u. "
             "epoch_info source: %s",
             epoch_info_->size(),
             epoch_info_->begin()->first,
             epoch_info_->rbegin()->first,
             log_id_.val_,
             current_last_clean_epoch_.value().val_,
             purge_to_.val_,
             SealStorageTask::EpochInfoSourceNames()[src].c_str());
  }

  state_ = State::GET_EPOCH_METADATA;
  first_epoch_to_purge_ = (getSettings().get_erm_for_empty_epoch)
      ? epoch_t(current_last_clean_epoch_.value().val_ + 1)
      : epoch_t(epoch_info_->begin()->first);
  last_epoch_to_purge_ = (getSettings().get_erm_for_empty_epoch)
      ? purge_to_
      : epoch_t(epoch_info_->rbegin()->first);
  getEpochMetaData();
}

void PurgeUncleanEpochs::getEpochMetaData() {
  ld_check(state_ == State::GET_EPOCH_METADATA);
  ld_check(!epoch_metadata_finalized_);
  ld_check(current_last_clean_epoch_.hasValue());
  // there should be no PurgeSingleEpoch machine already created
  ld_check(purge_epochs_.empty());

  if (MetaDataLog::isMetaDataLog(log_id_)) {
    auto metadata_for_metalogs = getEpochMetaDataForMetaDataLogs();
    if (metadata_for_metalogs == nullptr) {
      ld_check(err == E::NOTFOUND);
      complete(E::NOTFOUND);
      return;
    }
    EpochMetaDataMap::Map map{{EPOCH_MIN, *(metadata_for_metalogs.release())}};
    metadata_map_ = EpochMetaDataMap::create(
        std::make_shared<EpochMetaDataMap::Map>(std::move(map)),
        last_epoch_to_purge_);
    ld_check(metadata_map_ != nullptr);
    onAllEpochMetaDataGotten();
    return;
  }

  // start nodeset_finder to read metadata
  startReadingMetaData();
}

void PurgeUncleanEpochs::onHistoricalMetadata(Status st) {
  ld_check(state_ == State::GET_EPOCH_METADATA);
  ld_check(!epoch_metadata_finalized_);
  ld_check(current_last_clean_epoch_.hasValue());

  if (st == E::NOTINCONFIG) {
    epoch_metadata_finalized_ = true;
    complete(E::NOTFOUND);
    return;
  }

  if (st != E::OK) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Fetch historical epoch metadata for log %lu FAILED: %s. ",
                    log_id_.val_,
                    error_description(st));
    // keep retrying until success (e.g., metadata log is repaired)
    nodeset_finder_.reset();
    // clear all PurgeUncleanEpoch state machines already created
    purge_epochs_.clear();
    retry_timer_->setCallback(
        std::bind(&PurgeUncleanEpochs::getEpochMetaData, this));
    retry_timer_->activate();
    return;
  }

  metadata_map_ = getEpochMetadataMap();
  ld_check(metadata_map_ != nullptr);
  epoch_t effective_until = metadata_map_->getEffectiveUntil();
  if (effective_until < last_epoch_to_purge_) {
    // It is possible for the effective_until to be less than
    // last_epoch_to_purge_ if the metadata was read from metadata log
    // but the epoch in epoch store has been bumped beyond the effective_since
    // in the last record in metadata log. Since record in any epoch is released
    // only after the corresponding metadata record is written to metadata log,
    // it is safe to extend the effective_until till last_epoch_to_purge_
    metadata_map_ = metadata_map_->withNewEffectiveUntil(last_epoch_to_purge_);
  }
  onAllEpochMetaDataGotten();
}

void PurgeUncleanEpochs::onAllEpochMetaDataGotten() {
  ld_check(state_ == State::GET_EPOCH_METADATA);
  ld_check(!epoch_metadata_finalized_);
  ld_check(metadata_map_ != nullptr);
  ld_check(metadata_map_->getEffectiveUntil() >= last_epoch_to_purge_);
  epoch_metadata_finalized_ = true;
  state_ = State::GET_EPOCH_RECOVERY_METADATA;
  auto myShardId = ShardID(getMyNodeID().index(), shard_);

  for (auto entry : *metadata_map_) {
    epoch_t first_epoch, last_epoch;
    std::tie(first_epoch, last_epoch) = entry.first;
    // Is there an overlap between the epoch range in entry
    // and the epochs to be purged
    if (first_epoch <= last_epoch_to_purge_ &&
        first_epoch_to_purge_ <= last_epoch) {
      // Adjust both ends of the epoch range to
      // include only the epochs we care about
      if (first_epoch < first_epoch_to_purge_) {
        first_epoch = first_epoch_to_purge_;
      }
      if (last_epoch > last_epoch_to_purge_) {
        last_epoch = last_epoch_to_purge_;
      }
      std::shared_ptr<EpochMetaData> epochMetadata =
          std::make_shared<EpochMetaData>(entry.second);
      auto const& storageSet = epochMetadata->shards;
      // Post a GetEpochRecoveryMetadataRequest only if
      // this node is in the nodeset for this epoch range
      if (std::find(storageSet.begin(), storageSet.end(), myShardId) !=
          storageSet.end()) {
        // If we are trying to get the EpochRecoveryMetadata even for the epochs
        // that are empty locally, we post a single
        // GetEpochRecoveryMetadataRequest to batch the results for all epochs.
        // If not, we post GetEpochRecoveryMetadataRequest for individual epochs
        if (getSettings().get_erm_for_empty_epoch) {
          postEpochRecoveryMetadataRequest(
              first_epoch, last_epoch, epochMetadata);
        } else {
          ld_check(epoch_info_ != nullptr);
          ld_check(!epoch_info_->empty());
          for (auto it = epoch_info_->lower_bound(first_epoch.val_);
               it != epoch_info_->end() && it->first <= last_epoch.val_;
               ++it) {
            postEpochRecoveryMetadataRequest(
                epoch_t(it->first), epoch_t(it->first), epochMetadata);
          }
        }
      }
    }
  }

  if (num_running_get_erm_requests_ == 0) {
    // There was nothing to purge.
    allEpochsPurged();
    return;
  }
}

void PurgeUncleanEpochs::postEpochRecoveryMetadataRequest(
    epoch_t start_epoch,
    epoch_t end_epoch,
    std::shared_ptr<EpochMetaData> epochMetadata) {
  ld_check(epochMetadata && epochMetadata->isValid());
  ld_check(start_epoch <= end_epoch);

  auto callback_ticket = callback_helper_.ticket();
  auto cb = [=](Status status, std::unique_ptr<EpochRecoveryStateMap> map) {
    EpochRecoveryStateMap epochRecoveryStateMap = *map;

    callback_ticket.postCallbackRequest([=](PurgeUncleanEpochs* driver) {
      if (!driver) {
        ld_debug("GetEpochRecoveryMetadataRequest finished after "
                 "PurgeUncleanEpochs was destroyed, log:%lu, shard:%u,"
                 "purge_to:%u, epoch:%u",
                 log_id_.val_,
                 shard_,
                 purge_to_.val_,
                 start_epoch.val_);
        return;
      }

      ld_check(status == E::OK || status == E::ABORTED);
      driver->onGetEpochRecoveryMetadataComplete(status, epochRecoveryStateMap);
    });
  };

  ServerWorker* worker = ServerWorker::onThisThread();

  std::unique_ptr<Request> rq =
      std::make_unique<GetEpochRecoveryMetadataRequest>(worker->idx_,
                                                        log_id_,
                                                        shard_,
                                                        start_epoch,
                                                        end_epoch,
                                                        purge_to_,
                                                        epochMetadata,
                                                        cb);

  worker->processor_->postWithRetrying(rq);
  num_running_get_erm_requests_++;
}

NodeID PurgeUncleanEpochs::getMyNodeID() const {
  return Worker::onThisThread()->processor_->getMyNodeID();
}

void PurgeUncleanEpochs::onGetEpochRecoveryMetadataComplete(
    Status status,
    const EpochRecoveryStateMap& epochRecoveryStateMap) {
  ld_check(num_running_get_erm_requests_ > 0);
  num_running_get_erm_requests_--;
  ld_check(status == E::OK || status == E::ABORTED);
  ld_check(state_ == State::GET_EPOCH_RECOVERY_METADATA);

  for (auto& entry : epochRecoveryStateMap) {
    auto epoch = entry.first;
    const std::pair<Status, EpochRecoveryMetadata>& erm = entry.second;
    ld_check(erm.first == E::OK || erm.first == E::EMPTY ||
             erm.first == E::UNKNOWN);

    if (status != E::ABORTED && erm.first == E::UNKNOWN) {
      // If the GetEpochRecoveryMetadataRequest was not aborted and
      // the status for this epoch could not be determined, skip purging the
      // epoch.
      ld_info("GetEpochRecoveryMetadataRequest for log %lu did not get a valid "
              "response for epoch %u. Skipping purging for this epoch",
              log_id_.val_,
              epoch);
      continue;
    }

    if (!epoch_info_->count(epoch) && erm.first == E::EMPTY) {
      // The epoch is empty locally and status received is also
      // empty. There is nothing to purge.
      RATELIMIT_INFO(
          std::chrono::seconds(1),
          1,
          "Nothing to purge. Epoch is empty locally and "
          "EpochRecoveryMetadata consensus is also EMPTY for log: %lu,"
          "epoch:%u",
          log_id_.val_,
          epoch);
      continue;
    }

    createPurgeSingleEpochMachine(epoch_t(epoch), erm.first, erm.second);
  }

  if (num_running_get_erm_requests_ == 0) {
    // We have received the response for all epochs.
    // At this point we should have created PurgeSingleEpochMachine
    // for epochs in range [current_last_clean_epoch_ + 1, purge_to_]
    // that have this node in the nodeset and got a valid response from
    // GetEpochRecoveryMetadataRequest
    if (!purge_epochs_.empty()) {
      state_ = State::RUN_PURGE_EPOCHS;
      startPurgingEpochs();
    } else {
      // Nothing to purge
      allEpochsPurged();
    }
  }
}

void PurgeUncleanEpochs::createPurgeSingleEpochMachine(
    epoch_t epoch,
    Status status,
    EpochRecoveryMetadata erm) {
  esn_t local_last_record = ESN_INVALID;
  esn_t local_lng = ESN_INVALID;
  if (epoch_info_->count(epoch.val_)) {
    // This epoch was found to be non-empty locally.
    local_last_record = epoch_info_->at(epoch.val_).last_record;
    local_lng = epoch_info_->at(epoch.val_).lng;
  } else {
    ld_check(status != E::EMPTY);
  }

  auto insert_result = purge_epochs_.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(epoch),
      std::forward_as_tuple(log_id_,
                            shard_,
                            purge_to_,
                            epoch,
                            std::make_shared<EpochMetaData>(
                                metadata_map_->getValidEpochMetaData(epoch)),
                            local_lng,
                            local_last_record,
                            status,
                            erm,
                            this));

  // MetaDataLogReader guarantees that the interval [start_epoch, end_epoch]
  // must not overlap with each other and in order. So there must not be any
  // existing PurgeSingleEpoch machines in the same epoch.
  ld_check(insert_result.second);
}

void PurgeUncleanEpochs::startPurgingEpochs() {
  ld_check(state_ == State::RUN_PURGE_EPOCHS);
  ld_check(epoch_metadata_finalized_);
  ld_check(!purge_epochs_.empty());

  ld_debug("Starting PurgeSingleEpoch for log %lu for epochs [%s].",
           log_id_.val_,
           getActiveEpochsStr().c_str());

  // start all PurgeSingleEpoch machines at the same time, we can do this
  // because these epochs are independent and already cleaned (i.e., the
  // consensus regarding the state of the log are already established and
  // stored on other nodes.
  for (auto it = purge_epochs_.begin(); it != purge_epochs_.end();) {
    auto next_it = std::next(it);
    STAT_INCR(stats_, purging_v2_purge_epoch_started);
    it->second.start();
    it = next_it;
  }
}

PurgeSingleEpoch* PurgeUncleanEpochs::getActivePurgeSingleEpoch(epoch_t epoch) {
  auto it = purge_epochs_.find(epoch);
  if (it == purge_epochs_.end()) {
    return nullptr;
  }

  ld_check(it->second.getEpoch() == epoch);
  return &it->second;
}

void PurgeUncleanEpochs::onPurgeSingleEpochDone(epoch_t epoch, Status status) {
  ld_check(state_ == State::RUN_PURGE_EPOCHS);
  ld_check(epoch_metadata_finalized_);
  ld_check(!purge_epochs_.empty());

  auto it = purge_epochs_.find(epoch);
  // the PurgeSingleEpoch state machine must exist since it is owned by `this'
  if (it == purge_epochs_.end()) {
    RATELIMIT_CRITICAL(std::chrono::seconds(5),
                       1,
                       "PurgeSingleEpoch for log %lu epoch %u finshed with "
                       "status %s but it does not exist in the map! This "
                       "shouldn't happen.",
                       log_id_.val_,
                       epoch.val_,
                       error_description(status));
    ld_check(false);
    return;
  }

  STAT_INCR(stats_, purging_v2_purge_epoch_completed);

  if (status != E::OK) {
    // only case: log removed from the config
    complete(E::NOTFOUND);
    return;
  }

  RATELIMIT_INFO(
      std::chrono::seconds(10),
      100,
      "Successfully purged epoch %u for log %lu, %lu epochs remaining.",
      epoch.val_,
      log_id_.val_,
      purge_epochs_.size() - 1);

  purge_epochs_.erase(it);
  if (purge_epochs_.empty()) {
    allEpochsPurged();
  }
}

const Settings& PurgeUncleanEpochs::getSettings() const {
  return Worker::settings();
}

void PurgeUncleanEpochs::allEpochsPurged() {
  ld_check(current_last_clean_epoch_.hasValue());
  if (current_last_clean_epoch_.value() >= new_last_clean_epoch_) {
    complete(E::UPTODATE);
    return;
  }

  state_ = State::WRITE_LAST_CLEAN;
  writeLastClean();
}

void PurgeUncleanEpochs::writeLastClean() {
  ld_check(state_ == State::WRITE_LAST_CLEAN);
  ld_spew("Setting last clean epoch to %u for log %lu",
          new_last_clean_epoch_.val_,
          log_id_.val_);
  startStorageTask(std::make_unique<PurgeWriteLastCleanTask>(
      log_id_, new_last_clean_epoch_, ref_holder_.ref()));
}

void PurgeUncleanEpochs::onWriteLastCleanDone(Status status) {
  ld_check(state_ == State::WRITE_LAST_CLEAN);
  if (status != E::OK) {
    if (status == E::FAILED) {
      onPermanentError("PurgeWriteLastCleanTask", status);
      return;
    }

    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "PurgeWriteLastCleanTask failed with status %s"
                    ", starting timer",
                    error_description(status));
    retry_timer_->setCallback(
        std::bind(&PurgeUncleanEpochs::writeLastClean, this));
    retry_timer_->activate();
    return;
  }

  if (parent_) {
    parent_->updateLastCleanInMemory(new_last_clean_epoch_);
  }

  // all done, conclude purging
  complete(E::OK);
}

void PurgeUncleanEpochs::complete(Status status) {
  STAT_INCR(stats_, purging_done);

  if (status == E::OK) {
    STAT_INCR(stats_, purging_success);
    RATELIMIT_INFO(std::chrono::seconds(1),
                   1,
                   "Successfully purged epochs [%u, %u] for log %lu.",
                   current_last_clean_epoch_.value().val_,
                   purge_to_.val_,
                   log_id_.val_);
  } else if (status == E::UPTODATE) {
    STAT_INCR(stats_, purging_success);
    ld_debug("Epoch %u is already cleaned for log %lu.",
             purge_to_.val_,
             log_id_.val_);
  } else {
    STAT_INCR(stats_, purging_failed);
    RATELIMIT_INFO(std::chrono::seconds(10),
                   2,
                   "Purging for log %lu to epoch %u failed with status %s.",
                   log_id_.val_,
                   purge_to_.val_,
                   error_name(status));
  }

  if (parent_) {
    parent_->onStateMachineDone();
  }
}

std::unique_ptr<BackoffTimer> PurgeUncleanEpochs::createTimer() {
  return std::unique_ptr<BackoffTimer>(new ExponentialBackoffTimer(
      std::function<void()>(), INITIAL_RETRY_DELAY, MAX_RETRY_DELAY));
}

void PurgeUncleanEpochs::startStorageTask(std::unique_ptr<StorageTask>&& task) {
  ServerWorker::onThisThread()->getStorageTaskQueueForShard(shard_)->putTask(
      std::move(task));
}

std::unique_ptr<const EpochMetaData>
PurgeUncleanEpochs::getEpochMetaDataForMetaDataLogs() const {
  auto config = getClusterConfig();
  if (!config->logsConfig()->logExists(log_id_)) {
    err = E::NOTFOUND;
    return nullptr;
  }

  const auto& nodes_configuration = getNodesConfiguration();
  return std::make_unique<EpochMetaData>(
      EpochMetaData::genEpochMetaDataForMetaDataLog(
          log_id_, *nodes_configuration));
}

void PurgeUncleanEpochs::startReadingMetaData() {
  ld_check(nodeset_finder_ == nullptr);

  nodeset_finder_ = std::make_unique<NodeSetFinder>(
      log_id_,
      Worker::settings().read_historical_metadata_timeout * 2,
      [this](Status st) { onHistoricalMetadata(st); },
      // TODO:T28014582 Deprecate the setting once
      // all servers are migrated to a version that
      // supports INCLUDE_HISTORICAL_METADATA in
      // SyncSequencerRequest.
      Worker::settings().purging_use_metadata_log_only
          ? NodeSetFinder::Source::METADATA_LOG
          : NodeSetFinder::Source::BOTH);

  nodeset_finder_->start();
}

const std::shared_ptr<const Configuration>
PurgeUncleanEpochs::getClusterConfig() const {
  return Worker::getConfig();
}

const std::shared_ptr<LogsConfig> PurgeUncleanEpochs::getLogsConfig() const {
  return ServerWorker::onThisThread()->getLogsConfig();
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
PurgeUncleanEpochs::getNodesConfiguration() const {
  return Worker::onThisThread()->getNodesConfiguration();
}

const char* PurgeUncleanEpochs::stateString(State state) {
  switch (state) {
    case State::UNKNOWN:
      return "UNKNOWN";
    case State::READ_LAST_CLEAN:
      return "READ_LAST_CLEAN";
    case State::GET_PURGE_EPOCHS:
      return "GET_PURGE_EPOCHS";
    case State::GET_EPOCH_METADATA:
      return "GET_EPOCH_METADATA";
    case State::GET_EPOCH_RECOVERY_METADATA:
      return "GET_EPOCH_RECOVERY_METADATA";
    case State::RUN_PURGE_EPOCHS:
      return "RUN_PURGE_EPOCHS";
    case State::WRITE_LAST_CLEAN:
      return "WRITE_LAST_CLEAN";
    case State::PERMANENT_ERROR:
      return "PERMANENT_ERROR";
  }

  ld_check(false);
  return "INVALID";
}

void PurgeUncleanEpochs::onPermanentError(const char* context, Status status) {
  state_ = State::PERMANENT_ERROR;
  parent_->onPermanentError(context, status);
  complete(E::FAILED);
}

std::string PurgeUncleanEpochs::getActiveEpochsStr() const {
  std::vector<std::string> active_epochs;
  for (const auto& kv : purge_epochs_) {
    active_epochs.push_back(folly::to<std::string>(kv.first.val_));
  }
  return folly::join(",", active_epochs);
}

void PurgeUncleanEpochs::getDebugInfo(InfoPurgesTable& table) {
  table.next().set<0>(log_id_).set<1>(std::string(stateString(state_)));

  if (current_last_clean_epoch_.hasValue()) {
    table.set<2>(current_last_clean_epoch_.value());
  }

  table.set<3>(purge_to_);
  table.set<4>(new_last_clean_epoch_);
  table.set<5>(node_.toString());

  std::vector<std::string> epoch_states;
  for (const auto& kv : purge_epochs_) {
    epoch_states.push_back("e" + std::to_string(kv.first.val_) + ":" +
                           kv.second.getStateString(/*shorter*/ true));
  }

  if (!epoch_states.empty()) {
    table.set<6>(folly::join(",", epoch_states));
  }
}

void PurgeUncleanEpochs::onShutdown() {
  if (parent_ != nullptr) {
    parent_->shutdown();
  }
}

PurgeUncleanEpochs::~PurgeUncleanEpochs() {}

//
// Implementation of PurgeReadLastCleanTask
//

void PurgeReadLastCleanTask::execute() {
  executeImpl(storageThreadPool_->getLocalLogStore());
}

void PurgeReadLastCleanTask::executeImpl(LocalLogStore& store) {
  LastCleanMetadata meta;
  int rv = store.readLogMetadata(log_id_, &meta);
  if (rv == 0) {
    status_ = E::OK;
    result_ = meta.epoch_;
  } else if (err == E::NOTFOUND) {
    status_ = E::OK;
    result_ = epoch_t(0);
  } else {
    status_ = E::FAILED;
    result_ = EPOCH_INVALID;
  }
}

void PurgeReadLastCleanTask::onDone() {
  PurgeUncleanEpochs* driver = driver_.get();
  if (driver != nullptr) {
    driver->onLastCleanRead(status_, result_);
  }
}

void PurgeReadLastCleanTask::onDropped() {
  PurgeUncleanEpochs* driver = driver_.get();
  if (driver != nullptr) {
    STAT_INCR(driver->getStats(), purging_task_dropped);
  }
  status_ = E::DROPPED;
  result_ = EPOCH_INVALID;
  onDone();
}

//
// Implementation of PurgeWriteLastCleanTask
//

void PurgeWriteLastCleanTask::execute() {
  LogStorageStateMap& state_map =
      storageThreadPool_->getProcessor().getLogStorageStateMap();

  LogStorageState* log_state =
      state_map.find(log_id_, storageThreadPool_->getShardIdx());
  ld_check(log_state != nullptr);
  executeImpl(storageThreadPool_->getLocalLogStore(), log_state);
}

void PurgeWriteLastCleanTask::executeImpl(LocalLogStore& store,
                                          LogStorageState* /*log_state*/) {
  LocalLogStore::WriteOptions options;
  int rv = store.writeLogMetadata(log_id_, LastCleanMetadata(epoch_), options);
  status_ = rv == 0 ? E::OK : E::FAILED;

  // Note that here we no longer notify record cache on the local LCE
  // advancement for eviction purpose. Instead we would like to defer it
  // once we process the release we received. The reason is that recovery
  // may get restarted while in the cleaning phase and some of the CLEANs
  // are already processed by storage nodes. If we perform eviction as soon as
  // local LCE advances. The next recovery instance may suffer from a record
  // cache miss for getting the epoch. On the other hand, it is safe to defer
  // the eviction until RELEASEs are received because it is certain that epoch
  // recovery has successfully finished for the given epoch
}

void PurgeWriteLastCleanTask::onDone() {
  PurgeUncleanEpochs* driver = driver_.get();
  if (driver != nullptr) {
    driver->onWriteLastCleanDone(status_);
  }
}

void PurgeWriteLastCleanTask::onDropped() {
  PurgeUncleanEpochs* driver = driver_.get();
  if (driver != nullptr) {
    STAT_INCR(driver->getStats(), purging_task_dropped);
  }
  status_ = E::DROPPED;
  onDone();
}

}} // namespace facebook::logdevice
