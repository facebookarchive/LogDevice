/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/storage/PurgeSingleEpoch.h"

#include <functional>

#include <folly/Memory.h>

#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/PurgingTracer.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/protocol/GET_EPOCH_RECOVERY_METADATA_Message.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/include/Record.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/locallogstore/WriteOps.h"
#include "logdevice/server/storage/PurgeUncleanEpochs.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

namespace facebook { namespace logdevice {

using EpochInfo = SealStorageTask::EpochInfo;

namespace {
esn_t nextEsn(esn_t esn) {
  return esn == ESN_MAX ? ESN_MAX : esn_t(esn.val_ + 1);
}
} // namespace

PurgeSingleEpoch::PurgeSingleEpoch(
    logid_t log_id,
    shard_index_t shard,
    epoch_t purge_to,
    epoch_t epoch,
    std::shared_ptr<EpochMetaData> epoch_metadata,
    esn_t local_lng,
    esn_t local_last_record,
    Status status,
    EpochRecoveryMetadata erm,
    PurgeUncleanEpochs* driver)
    : ref_holder_(this),
      log_id_(log_id),
      shard_(shard),
      purge_to_(purge_to),
      epoch_(epoch),
      epoch_metadata_(epoch_metadata),
      local_lng_(local_lng),
      local_last_record_(local_last_record),
      get_epoch_recovery_metadata_status_(status),
      recovery_metadata_(erm),
      driver_(driver),
      callbackHelper_(this) {
  ld_check(epoch_metadata->isValid());
}

const char* PurgeSingleEpoch::getStateString(bool shorter) const {
  return getStateString(state_, shorter);
}

void PurgeSingleEpoch::start() {
  if (retry_timer_ == nullptr) {
    retry_timer_ = createRetryTimer();
  }

  state_ = State::GET_RECOVERY_METADATA;
  if (get_epoch_recovery_metadata_status_ != E::UNKNOWN) {
    ld_check(get_epoch_recovery_metadata_status_ == E::EMPTY ||
             recovery_metadata_.valid());
    // We already have the epoch recovery metadata.
    // Proceed to next stage.
    state_ = State::PURGE_RECORDS;
    purgeRecords();
  } else {
    postGetEpochRecoveryMetadataRequest();
  }
}

void PurgeSingleEpoch::postGetEpochRecoveryMetadataRequest() {
  ld_check(state_ == State::GET_RECOVERY_METADATA);
  auto callback_ticket = callbackHelper_.ticket();
  auto cb = [=](Status status, std::unique_ptr<EpochRecoveryStateMap> map) {
    EpochRecoveryStateMap epochRecoveryStateMap = *map;

    callback_ticket.postCallbackRequest([=](PurgeSingleEpoch* driver) {
      if (!driver) {
        ld_debug(
            "GetEpochRecoveryMetadataRequest finished after "
            "PurgeSingleEpoch was destroyed, log:%lu, shard:%u, purge_to:%u, "
            "epoch:%u",
            log_id_.val_,
            shard_,
            purge_to_.val_,
            epoch_.val_);
        return;
      }

      ld_check(status == E::OK);
      driver->onGetEpochRecoveryMetadataComplete(status, epochRecoveryStateMap);
    });
  };

  ServerWorker* worker = ServerWorker::onThisThread();

  std::unique_ptr<Request> rq =
      std::make_unique<GetEpochRecoveryMetadataRequest>(worker->idx_,
                                                        log_id_,
                                                        shard_,
                                                        epoch_,
                                                        epoch_,
                                                        purge_to_,
                                                        epoch_metadata_,
                                                        cb);

  worker->processor_->postWithRetrying(rq);
}

void PurgeSingleEpoch::onGetEpochRecoveryMetadataComplete(
    Status status,
    const EpochRecoveryStateMap& epochRecoveryStateMap) {
  ld_check(!epochRecoveryStateMap.empty() && epochRecoveryStateMap.size() == 1);
  ld_check(status == E::OK);
  auto const it = epochRecoveryStateMap.begin();
  ld_check(epoch_t(it->first) == epoch_);

  const std::pair<Status, EpochRecoveryMetadata>& erm = it->second;

  if (erm.first != E::OK && erm.first != E::EMPTY) {
    // Any other response means that we heard from
    // all authoritative nodes but could not make a
    // decision. Skip purging this epoch.
    complete(E::OK);
    finalizeIfDone();
    return;
  }
  get_epoch_recovery_metadata_status_ = erm.first;
  if (get_epoch_recovery_metadata_status_ == E::OK) {
    ld_check(erm.second.valid());
    recovery_metadata_ = erm.second;
  }

  // we must get a valid status from the reply, current only two status codes
  // are considered success: E::OK and E::EMPTY
  ld_check(get_epoch_recovery_metadata_status_ == E::OK ||
           get_epoch_recovery_metadata_status_ == E::EMPTY);
  ld_check(get_epoch_recovery_metadata_status_ == E::EMPTY ||
           recovery_metadata_.valid());

  state_ = State::PURGE_RECORDS;
  purgeRecords();
}

void PurgeSingleEpoch::purgeRecords() {
  SCOPE_EXIT {
    finalizeIfDone();
  };

  ld_check(state_ == State::PURGE_RECORDS);
  ld_check(get_epoch_recovery_metadata_status_ != E::UNKNOWN);
  ld_check(get_epoch_recovery_metadata_status_ == E::EMPTY ||
           (recovery_metadata_.valid()));

  // Deciding the start esn for the range of records to be deleted:
  // 1) if the epoch is not empty according to the epoch recovery consensus,
  //    use the LNG consensus of EpochRecoveryMetadata plus one.
  // 2) if the epoch is empty according to the epoch recovery consensus,
  //    it is very likely that the local lng for the epoch is also ESN_INVALID
  //    (otherwise EpochRecovery should have collected this LNG on other nodes).
  //    use local_lng + 1 as the starting esn.
  const esn_t start_esn =
      nextEsn(get_epoch_recovery_metadata_status_ == E::OK
                  ? recovery_metadata_.header_.last_known_good
                  : local_lng_);

  const esn_t end_esn = local_last_record_;

  if (start_esn > end_esn) {
    RATELIMIT_INFO(
        std::chrono::seconds(10),
        5,
        "No need to delete records for epoch %u, log %lu, shard %u, purge "
        "to %u start_esn: %u, end_esn: %u, local_lng: %u",
        epoch_.val_,
        log_id_.val_,
        shard_,
        purge_to_.val_,
        start_esn.val_,
        end_esn.val_,
        local_lng_.val_);

    state_ = State::WRITE_RECOVERY_METADATA;
    writeEpochRecoveryMetadata();
    return;
  }

  if (get_epoch_recovery_metadata_status_ == Status::EMPTY &&
      local_lng_ > ESN_INVALID) {
    RATELIMIT_WARNING(std::chrono::seconds(10),
                      5,
                      "Purging for epoch %u, log %lu, shard %u purge to %u "
                      "learned that epoch is empty according to "
                      "GET_EPOCH_RECOVERY_METADATA replies but the epoch has a "
                      "non-zero LNG stored locally. It is likely that recovery "
                      "was skipped for the epoch. Skipping purging to avoid "
                      "true dataloss.",
                      epoch_.val_,
                      log_id_.val_,
                      shard_,
                      purge_to_.val_);

    STAT_INCR(getStats(), purge_epoch_skipped_empty_positive_local_lng);
    state_ = State::WRITE_RECOVERY_METADATA;
    writeEpochRecoveryMetadata();
    return;
  }

  ld_info("Going to delete records in [%u, %u] in epoch %u, log %lu, shard %u, "
          "purge to %u, epoch recovery metadata status: %s, metadata: %s",
          start_esn.val_,
          end_esn.val_,
          epoch_.val_,
          log_id_.val_,
          shard_,
          purge_to_.val_,
          error_name(get_epoch_recovery_metadata_status_),
          recovery_metadata_.valid() ? recovery_metadata_.toString().c_str()
                                     : "n/a");

  // delete every record in [start, end] in this epoch
  startStorageTask(std::make_unique<PurgeDeleteRecordsStorageTask>(
      log_id_, epoch_, start_esn, end_esn, ref_holder_.ref()));
}

void PurgeSingleEpoch::onPurgeRecordsTaskDone(Status status) {
  ld_check(state_ == State::PURGE_RECORDS);
  ld_check(retry_timer_ != nullptr);

  SCOPE_EXIT {
    finalizeIfDone();
  };

  if (status != E::OK) {
    if (status == E::FAILED) {
      driver_->onPermanentError("PurgeDeleteRecordsStorageTask", status);
      return;
    }

    // for transient errors retry after a timeout
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    2,
                    "PurgeDeleteRecordsStorageTask failed with status %s, "
                    "starting timer",
                    error_description(status));
    retry_timer_->setCallback(std::bind(&PurgeSingleEpoch::purgeRecords, this));
    retry_timer_->activate();
    return;
  }

  // reset the delay
  retry_timer_->reset();
  state_ = State::WRITE_RECOVERY_METADATA;
  writeEpochRecoveryMetadata();
}

void PurgeSingleEpoch::writeEpochRecoveryMetadata() {
  ld_check(state_ == State::WRITE_RECOVERY_METADATA);

  if (get_epoch_recovery_metadata_status_ == E::EMPTY) {
    // epoch is _empty_ according to the consensus collected by epoch recovery,
    // do not write EpochRecoveryMetadata (see doc in CleanedResponseRequest.h).
    // The epoch is considered purged now.
    complete(E::OK);
    return;
  }

  // epoch is NOT empty according to epoch recovery, purging needs to
  // write the EpochRecoveryMetadata regardless whether the epoch is empty
  // locally. This is needed for correctness.
  ld_check(recovery_metadata_.valid());

  // a copy of metadata is needed since we might need retries
  auto metadata = std::make_unique<EpochRecoveryMetadata>();
  int rv = metadata->deserialize(recovery_metadata_.serialize());

  // must be able to deserialze a valid metadata
  ld_check(rv == 0);
  ld_check(metadata->valid());

  startStorageTask(std::make_unique<PurgeWriteEpochRecoveryMetadataStorageTask>(
      log_id_, epoch_, std::move(metadata), ref_holder_.ref()));
}

void PurgeSingleEpoch::onWriteEpochRecoveryMetadataDone(Status status) {
  ld_check(state_ == State::WRITE_RECOVERY_METADATA);
  ld_check(retry_timer_ != nullptr);

  SCOPE_EXIT {
    finalizeIfDone();
  };

  if (status != E::OK) {
    if (status == E::FAILED) {
      driver_->onPermanentError(
          "PurgeWriteEpochRecoveryMetadataStorageTask", status);
      return;
    }

    // for transient errors retry after a timeout
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    2,
                    "PurgeWriteEpochRecoveryMetadataStorageTask failed with "
                    "status %s, starting timer",
                    error_description(status));
    retry_timer_->setCallback(
        std::bind(&PurgeSingleEpoch::writeEpochRecoveryMetadata, this));
    retry_timer_->activate();
    return;
  }

  // EpochRecoveryMetadata written, all done!
  complete(E::OK);
}

void PurgeSingleEpoch::complete(Status status) {
  // currently only allowed failure is log not in config
  ld_check(status == E::OK || status == E::NOTFOUND);
  // the state machine should only complete once
  ld_check(result_ == E::UNKNOWN);

  state_ = State::FINISHED;
  result_ = status;
}

void PurgeSingleEpoch::finalizeIfDone() {
  if (state_ == State::FINISHED) {
    ld_check(result_ == E::OK || result_ == E::NOTFOUND);
    deferredComplete();
  }
}

void PurgeSingleEpoch::deferredComplete() {
  // Start a timer with zero delay.
  deferredCompleteTimer_ = std::make_unique<Timer>([this] {
    if (driver_ != nullptr) {
      driver_->onPurgeSingleEpochDone(epoch_, result_);
    }
  });
  deferredCompleteTimer_->activate(std::chrono::milliseconds(0));
}

std::unique_ptr<BackoffTimer> PurgeSingleEpoch::createRetryTimer() {
  auto timer = std::make_unique<ExponentialBackoffTimer>(

      std::function<void()>(),
      PurgeUncleanEpochs::INITIAL_RETRY_DELAY,
      PurgeUncleanEpochs::MAX_RETRY_DELAY);
  return std::move(timer);
}

void PurgeSingleEpoch::startStorageTask(std::unique_ptr<StorageTask>&& task) {
  ServerWorker::onThisThread()->getStorageTaskQueueForShard(shard_)->putTask(
      std::move(task));
}

StatsHolder* PurgeSingleEpoch::getStats() {
  return driver_ != nullptr ? driver_->getStats() : nullptr;
}

const char* PurgeSingleEpoch::getStateString(State state, bool shorter) {
  switch (state) {
    case State::UNKNOWN:
      return shorter ? "U" : "UNKNOWN";
    case State::GET_RECOVERY_METADATA:
      return shorter ? "G" : "GET_RECOVERY_METADATA";
    case State::PURGE_RECORDS:
      return shorter ? "P" : "PURGE_RECORDS";
    case State::WRITE_RECOVERY_METADATA:
      return shorter ? "W" : "WRITE_RECOVERY_METADATA";
    case State::FINISHED:
      return shorter ? "F" : "FINISHED";
  }

  ld_check(false);
  return "INVALID";
}

///////// PurgeDeleteRecordsStorageTask

const size_t PurgeDeleteRecordsStorageTask::PURGE_DELETE_BY_KEY_THRESHOLD;

PurgeDeleteRecordsStorageTask::PurgeDeleteRecordsStorageTask(
    logid_t log_id,
    epoch_t epoch,
    esn_t start_esn,
    esn_t end_esn,
    WeakRef<PurgeSingleEpoch> driver)
    : StorageTask(StorageTask::Type::PURGE_DELETE_RECORDS),
      log_id_(log_id),
      epoch_(epoch),
      start_esn_(start_esn),
      end_esn_(end_esn),
      driver_(std::move(driver)) {
  ld_check(start_esn_ <= end_esn_);
}

void PurgeDeleteRecordsStorageTask::execute() {
  executeImpl(storageThreadPool_->getLocalLogStore(),
              stats_,
              storageThreadPool_->getProcessor().getTraceLogger().get());
}

void PurgeDeleteRecordsStorageTask::executeImpl(LocalLogStore& store,
                                                StatsHolder* stats,
                                                TraceLogger* logger) {
  ld_check(end_esn_ >= start_esn_);
  STAT_INCR(stats, purging_delete_started);

  std::vector<DeleteWriteOp> deletes;
  std::vector<const WriteOp*> ops;
  if (end_esn_.val_ - start_esn_.val_ <= PURGE_DELETE_BY_KEY_THRESHOLD - 1) {
    // there are not so many records to delete, delete all possible keys to
    // avoid reading from the data key space, which may incur expensive
    // I/O operations (e.g., disk seeks).
    STAT_INCR(stats, purging_v2_delete_by_keys);

    // shouldn't overflow here
    const size_t num_keys = end_esn_.val_ - start_esn_.val_ + 1;
    deletes.reserve(num_keys);

    for (size_t i = 0; i < num_keys; ++i) {
      esn_t::raw_type esn = start_esn_.val_ + static_cast<esn_t::raw_type>(i);

      deletes.emplace_back(log_id_, compose_lsn(epoch_, esn_t(esn)));
    }

    if (MetaDataLog::isMetaDataLog(log_id_)) {
      ld_info("Maybe deleting metadata log records; log: %lu epoch: %u "
              "start esn: %u end esn: %u",
              log_id_.val_,
              epoch_.val_,
              start_esn_.val_,
              end_esn_.val_);
    }
    PurgingTracer::traceRecordPurge(
        logger, log_id_, epoch_, ESN_INVALID, start_esn_, end_esn_, true);

    ld_check(deletes.size() == num_keys);
  } else {
    // the range contains too many keys, read the data space to collect records
    // that were actually stored in this range
    STAT_INCR(stats, purging_v2_delete_by_reading_data);

    LocalLogStore::ReadOptions read_options("PurgeDeleteRecords");
    read_options.allow_blocking_io = true;
    read_options.tailing = false;
    std::unique_ptr<LocalLogStore::ReadIterator> store_it =
        store.read(log_id_, read_options);

    for (store_it->seek(compose_lsn(epoch_, start_esn_));
         store_it->state() == IteratorState::AT_RECORD;
         store_it->next()) {
      lsn_t lsn = store_it->getLSN();
      if (lsn_to_epoch(lsn) != epoch_) {
        // No longer in epoch being purged, stop reading
        break;
      }

      if (lsn_to_esn(lsn) > end_esn_) {
        ld_error("Internal error: new records appeared during purging: "
                 "log %lu, epoch %u, expected records up to ESN %u, got "
                 "record %u.",
                 log_id_.val_,
                 epoch_.val_,
                 end_esn_.val_,
                 lsn_to_esn(lsn).val_);
        break;
      }

      if (MetaDataLog::isMetaDataLog(log_id_)) {
        ld_info("Deleting metadata log record; log: %lu lsn: %s "
                "start esn: %u end esn: %u",
                log_id_.val_,
                lsn_to_string(lsn).c_str(),
                start_esn_.val_,
                end_esn_.val_);
      }
      PurgingTracer::traceRecordPurge(
          logger, log_id_, epoch_, lsn_to_esn(lsn), start_esn_, end_esn_, true);

      deletes.emplace_back(log_id_, lsn);
    }

    switch (store_it->state()) {
      case IteratorState::AT_RECORD:
      case IteratorState::AT_END:
        break;
      case IteratorState::ERROR:
        status_ = E::FAILED;
        return;
      case IteratorState::WOULDBLOCK:
      case IteratorState::LIMIT_REACHED:
      case IteratorState::MAX:
        ld_check(false);
        status_ = E::FAILED;
        return;
    }
  }

  ops.resize(deletes.size());
  for (int i = 0; i < deletes.size(); ++i) {
    ops[i] = &deletes[i];
  }

  int rv = store.writeMulti(ops);
  status_ = (rv == 0 ? E::OK : E::FAILED);
  STAT_INCR(stats, purging_delete_done);
}

void PurgeDeleteRecordsStorageTask::onDone() {
  PurgeSingleEpoch* driver = driver_.get();
  if (driver != nullptr) {
    driver->onPurgeRecordsTaskDone(status_);
  }
}

void PurgeDeleteRecordsStorageTask::onDropped() {
  PurgeSingleEpoch* driver = driver_.get();
  if (driver != nullptr) {
    STAT_INCR(driver->getStats(), purging_task_dropped);
  }
  status_ = E::DROPPED;
  onDone();
}

///////// PurgeWriteEpochRecoveryMetadataStorageTask

void PurgeWriteEpochRecoveryMetadataStorageTask::execute() {
  executeImpl(storageThreadPool_->getLocalLogStore());
}

void PurgeWriteEpochRecoveryMetadataStorageTask::executeImpl(
    LocalLogStore& store) {
  ld_check(metadata_ != nullptr);
  ld_check(metadata_->valid());

  // disable seal preemption checks since we are purging an epoch that is
  // already recovered. Otherwise, we are guaranteed to be preempted by the
  // Seal as purging populates seal metadata on CLEAN and RELEASE messages.
  int rv = store.updatePerEpochLogMetadata(
      log_id_, epoch_, *metadata_, LocalLogStore::SealPreemption::DISABLE);

  if (rv != 0) {
    if (err == E::UPTODATE) {
      RATELIMIT_WARNING(std::chrono::seconds(10),
                        10,
                        "Write EpochRecoveryMetadata for log %lu shard %u epoch"
                        " %u failed as there is already a more up-to-date "
                        "metadata stored. Metadata that prevents the update: "
                        "%s. This should be rare.",
                        log_id_.val_,
                        storageThreadPool_->getShardIdx(),
                        epoch_.val_,
                        metadata_->toString().c_str());
      status_ = E::OK;
    } else {
      status_ = E::FAILED;
    }
  } else {
    // write success
    status_ = E::OK;
  }
}

void PurgeWriteEpochRecoveryMetadataStorageTask::onDone() {
  PurgeSingleEpoch* driver = driver_.get();
  if (driver != nullptr) {
    driver->onWriteEpochRecoveryMetadataDone(status_);
  }
}

void PurgeWriteEpochRecoveryMetadataStorageTask::onDropped() {
  PurgeSingleEpoch* driver = driver_.get();
  if (driver != nullptr) {
    STAT_INCR(driver->getStats(), purging_task_dropped);
  }
  status_ = E::DROPPED;
  onDone();
}

}} // namespace facebook::logdevice
