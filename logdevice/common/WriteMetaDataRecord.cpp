/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/WriteMetaDataRecord.h"

#include <folly/Memory.h>

#include "logdevice/common/LogIDUniqueQueue.h"
#include "logdevice/common/LogRecoveryRequest.h"
#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/MetaSequencer.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sequencer.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/protocol/RELEASE_Message.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

WriteMetaDataRecord::WriteMetaDataRecord(logid_t log_id,
                                         epoch_t epoch,
                                         MetaDataLogWriter* driver)
    : log_id_(log_id), epoch_(epoch), state_(State::READY), driver_(driver) {
  created_on_ = Worker::onThisThread()->idx_;
  ld_check(log_id != LOGID_INVALID);
  ld_check(epoch != EPOCH_INVALID);
  ld_check(driver != nullptr);
}

RunAppenderStatus WriteMetaDataRecord::start(Appender* appender) {
  ld_check(state_ == State::READY && meta_seq_.get() == nullptr);
  if (createAndActivate(Worker::settings().bypass_recovery && appender != 0)) {
    ld_error("Failed to activate MetaData Sequencer for data log %lu "
             "in epoch %u: %s",
             log_id_.val_,
             epoch_.val_,
             error_description(err));
    // collapse all errors to E::NOSEQUENCER for sending to the client
    err = E::NOSEQUENCER;
    return RunAppenderStatus::ERROR_DELETE;
  }

  if (meta_seq_->checkIfPreempted(epoch_).isNodeID()) {
    // should not happen for a newly created sequencer
    ld_check(false);
    err = E::PREEMPTED;
    return RunAppenderStatus::ERROR_DELETE;
  }

  RunAppenderStatus rv = RunAppenderStatus::SUCCESS_KEEP;
  if (appender) {
    rv = meta_seq_->runAppender(appender);
    if (rv != RunAppenderStatus::ERROR_DELETE) {
      target_lsn_ = appender->getLSN();
      ld_check(target_lsn_ != LSN_INVALID);
      // SUCCESS_DELETED means storing the record is already done.  Can't happen
      // with metadata records.  If the code changes, would need a different
      // value for state_ here, as onAppenderRetired() and other methods won't
      // be called.
      ld_check(rv == RunAppenderStatus::SUCCESS_KEEP);
      state_ = State::STARTED;
    } else {
      // Couldn't run the appender, destroying the Metadata log sequencer
      ld_error("Failed to run the appender for metadata log %lu for data log "
               "%lu in epoch %u: %s",
               MetaDataLog::metaDataLogID(log_id_).val(),
               log_id_.val(),
               epoch_.val(),
               error_description(err));
      meta_seq_.reset();
    }
  } else {
    // appender not provided, this is a recovery only operation,
    // skip the append stages and advance to State::STORED, waiting for
    // recovery to complete and releases to send to meta storage nodes
    recovery_only_ = true;
    // Once recovery is completed, we expect it to advance last released lsn
    // to at lease lsn(epoch_, 0)
    target_lsn_ = compose_lsn(epoch_, ESN_INVALID);
    state_ = State::STORED;
  }

  return rv;
}

int WriteMetaDataRecord::createAndActivate(bool bypass_recovery) {
  std::shared_ptr<Configuration> cfg(Worker::getConfig());
  if (!cfg->logsConfig()->logExists(log_id_)) {
    err = E::NOTFOUND;
    return -1;
  }

  const auto& nodes_configuration =
      Worker::onThisThread()->getNodesConfiguration();
  ld_check(meta_seq_ == nullptr);
  // create the metadata sequencer in INITIALIZING state
  meta_seq_ = std::make_shared<MetaSequencer>(
      MetaDataLog::metaDataLogID(log_id_), // MetaData logid for the data log
      Worker::onThisThread()->processor_->updateableSettings(),
      Worker::stats(),
      this);

  ld_check(meta_seq_ != nullptr);

  meta_seq_->startActivation(
      // metadata function, no-op
      [](logid_t /*logid*/) -> int { return 0; },
      // activation predicate
      nullptr);

  // create epoch metadata from configuration and use it to activate the meta
  // sequencer. It is important to set effective_since to EPOCH_MIN so that
  // recovery of the meta sequencer can always proceed without not knowing
  // epoch metadata for any epoch.
  auto meta_storage_set = EpochMetaData::nodesetToStorageSet(
      nodes_configuration->getStorageMembership()->getMetaDataNodeIndices(),
      MetaDataLog::metaDataLogID(log_id_),
      *nodes_configuration);

  std::unique_ptr<EpochMetaData> metadata = std::make_unique<EpochMetaData>(
      EpochMetaData::genEpochMetaDataForMetaDataLog(
          log_id_, *nodes_configuration, epoch_, EPOCH_MIN));

  // activate the metadata sequencer using the epoch from parent,
  // as well as meta nodeset and replication factor stored in configuration
  auto result = meta_seq_->completeActivationWithMetaData(
      epoch_, cfg, std::move(metadata));
  if (result == ActivateResult::FAILED) {
    err = E::PREEMPTED;
    return -1;
  }

  // for metadata logs, activation should not be deferred since there is only
  // one epoch running with no previous epoch for this MetaSequencer. Therefore
  // ActivateResult::GRACEFUL_DRAINING can never be returned.
  ld_check(result == ActivateResult::RECOVERY);

  if (bypass_recovery) {
    ld_warning("bypassing recovery according to test options.");
  } else {
    WORKER_STAT_INCR(recovery_scheduled);

    // start recovery for the meta sequencer. The entire recovery procedure
    // is also pinned to the same Worker thread.
    if (meta_seq_->startRecovery()) {
      ld_error("Failed to start recovery for metadata log %lu in epoch %u: %s",
               meta_seq_->getLogID().val_,
               epoch_.val_,
               error_description(err));
      return -1;
    }
  }

  // populate the release_sent_ map
  for (ShardID shard : meta_storage_set) {
    release_sent_[shard.node()] = false;
  }

  ld_debug("Activated metadata sequencer [%lu] for log %lu with epoch %u.",
           meta_seq_->getLogID().val_,
           log_id_.val_,
           epoch_.val_);
  return 0;
}

void WriteMetaDataRecord::onAppenderRetired(Status st, lsn_t lsn) {
  ld_check(Worker::onThisThread()->idx_ == created_on_);
  ld_check(state_ == State::STARTED);
  ld_check(!recovery_only_);
  ld_check(meta_seq_ != nullptr);
  // appender was retired, also, since there is only one appender
  // in the sliding window, it must have also been reaped
  ld_check(meta_seq_->getNumAppendsInFlight() == 0);
  // appender must be with the current epoch
  ld_check(lsn_to_epoch(lsn) == epoch_);
  ld_check(target_lsn_ == lsn);

  if (st != E::OK) {
    // appender failed to store the copy, this could be caused by shutting down
    // of the Worker, or the appender being preempted because the epoch has
    // already been sealed, in either case, abort the state machine despite the
    // status of the recovery
    ld_check(st == E::PREEMPTED || st == E::SHUTDOWN);
    abortRecovery();
    finalize(st);
  } else if (recovery_status_.hasValue() && recovery_status_.value() != E::OK) {
    // appender has fully stored but recovery encountered unrecoverable error
    // abort
    finalize(recovery_status_.value());
  } else {
    state_ = State::STORED;
  }
}

void WriteMetaDataRecord::onReleaseSentSuccessful(node_index_t node,
                                                  lsn_t released_lsn,
                                                  ReleaseType release_type) {
  if (release_type == ReleaseType::PER_EPOCH) {
    // Per-epoch releases currently have no effect here.
    return;
  }

  ld_check(Worker::onThisThread()->idx_ == created_on_);
  if (state_ != State::STORED) {
    // we are not expecting releases or have already collected enough releases
    // to advance to the next stage. Ignore.
    return;
  }

  // If the job is recovery only, expect the released_lsn to be lng_ when
  // recovery completes. Otherwise, expect the record written to be released
  ld_check(target_lsn_ != LSN_INVALID);

  if (released_lsn < target_lsn_) {
    // possibly a stale notification for a previous epoch. Ignore.
    return;
  }

  ld_check(meta_seq_ != nullptr);
  if (meta_seq_->getLastReleased() < released_lsn) {
    // release not sent by the current metadata sequencer, ignore.
    return;
  }

  // note: release may originated from log recoveries that were
  // previously aborted but actually finished, treat these RELEASES as valid
  // since the epoch was recovered and their release_lsn is the same as
  // target_lsn_

  auto it = release_sent_.find(node);
  if (it == release_sent_.end()) {
    // release sent to a node not belonging the meta nodeset, ignore.
    return;
  }

  if (it->second == false /* release not yet sent */) {
    it->second = true;
    // meta_seq_ must exist in State::STORED
    ld_assert(meta_seq_->getNumAppendsInFlight() == 0);

    const int replication =
        meta_seq_->getCurrentMetaData()->replication.getReplicationFactor();
    const int fmajority =
        std::max(replication,
                 static_cast<int>(release_sent_.size()) - (replication - 1));

    /**
     * Currently WriteMetaDataRecord concludes when releases are successfully
     * sent to f-majority of the metadata nodeset. Similar to the release
     * handling for data logs, this is a best effort mechanism that does not
     * guarantee that releases are actually processed in storage nodes.
     *
     * TODO: add release confirmation if this turns out to be a problem and
     * requires stronger delivery gurarantees
     * TODO: use real f-majority detection instead of just
     * nodeset_size - replication
     * TODO: get rid of releases and recoveries in metadata logs
     */
    if (++num_releases_sent_ >= fmajority) {
      finalize(E::OK);
    }
  }
}

void WriteMetaDataRecord::onRecoveryCompleted(Status status, epoch_t epoch) {
  // the LogRecoveryRequest object is also pinned to the worker that created the
  // state machine
  ld_check(Worker::onThisThread()->idx_ == created_on_);
  ld_check(epoch <= epoch_);

  if (epoch < epoch_) {
    // a stale recovery notification, ignore.
    return;
  }

  if (status == E::PREEMPTED || status == E::NOTINCONFIG ||
      status == E::SHUTDOWN) {
    // Recovery for metadata log cannot complete. In other words, there is no
    // hope for the current record to be released.

    if (state_ == State::STORED) {
      // appender already finished or does not exist, conclude
      ld_check(meta_seq_->getNumAppendsInFlight() == 0);
      finalize(status);
    } else {
      // appender is still running, waiting for it to retire before destroying
      // the sequencer
      recovery_status_ = status;
    }
  } else if (status == E::OK) {
    recovery_status_ = status;
  }

  // For other status, recovery will be retried by the sequencer
}

void WriteMetaDataRecord::abortRecovery() {
  // delete the running LogRecoveryRequest, if any
  logid_t log = MetaDataLog::metaDataLogID(log_id_);
  auto& recovery_map = Worker::onThisThread()->runningLogRecoveries().map;
  auto it = recovery_map.find(log);
  if (it != recovery_map.end()) {
    WORKER_STAT_INCR(recovery_completed);
    recovery_map.erase(it);
  }

  // delete recovery from queue if it's there
  auto& queue = Worker::onThisThread()->recoveryQueueMetaDataLog().q;
  auto& index = queue.get<1>();
  auto it2 = index.find(log);
  if (it2 != index.end()) {
    WORKER_STAT_DECR(recovery_enqueued);
    WORKER_STAT_INCR(recovery_completed);
    index.erase(it2);
  }
}

void WriteMetaDataRecord::abort() {
  if (!isRecoveryOnly()) {
    // must be called with recovery_only_
    ld_check(false);
    return;
  }

  abortRecovery();
  finalize(E::ABORTED);
}

void WriteMetaDataRecord::finalize(Status st) {
  // destruction of the state machine must happen on the same worker thread
  ld_check(Worker::onThisThread()->idx_ == created_on_);

  // no appender should be running at this time
  ld_check(meta_seq_->getNumAppendsInFlight() == 0);
  driver_->onWriteMetaDataRecordDone(st, meta_seq_->getLastReleased());
  /* `this' is deleted */
}

std::shared_ptr<Sequencer> WriteMetaDataRecord::getMetaSequencer() const {
  // expose the meta sequencer only to the Worker on which it is running
  ld_check(Worker::onThisThread()->idx_ == created_on_);
  return meta_seq_;
}

void WriteMetaDataRecord::notePreempted(epoch_t epoch, NodeID preempted_by) {
  ld_spew("PREEMPTED by %s for epoch %u of log %lu",
          preempted_by.toString().c_str(),
          epoch.val(),
          log_id_.val());
  driver_->notePreempted(epoch, preempted_by);
}

void WriteMetaDataRecord::noteConfigurationChanged() {
  auto meta_seq = getMetaSequencer();
  if (meta_seq != nullptr) {
    // propagate config change to meta sequencer, do not care about
    // the sequencer node property
    meta_seq->noteConfigurationChanged(
        Worker::onThisThread()->getConfiguration(),
        Worker::onThisThread()->getNodesConfiguration(),
        /*is_sequencer_node*/ true);
  }
}

WriteMetaDataRecord::~WriteMetaDataRecord() {
  ld_check(meta_seq_ == nullptr || meta_seq_->getNumAppendsInFlight() == 0);
}

void WriteMetaDataRecordMap::noteConfigurationChanged() {
  if (map.empty()) {
    return;
  }

  folly::small_vector<logid_t> writes;
  writes.reserve(map.size());
  for (const auto& it : map) {
    writes.push_back(it.first);
  }

  for (auto log_id : writes) {
    auto it = map.find(log_id);
    if (it != map.end()) {
      ld_check(it->second != nullptr);
      it->second->noteConfigurationChanged();
    }
  }
}

}} // namespace facebook::logdevice
