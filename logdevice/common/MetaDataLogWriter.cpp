/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/MetaDataLogWriter.h"

#include <folly/Memory.h>
#include <folly/hash/Hash.h>

#include "logdevice/common/AllSequencers.h"
#include "logdevice/common/AppenderBuffer.h"
#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/MetaSequencer.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sequencer.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/protocol/RECORD_Message.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

const std::chrono::milliseconds
    MetaDataLogWriter::RETRY_ACTIVATION_INITIAL_DELAY{1000};
const std::chrono::milliseconds MetaDataLogWriter::RETRY_ACTIVATION_MAX_DELAY{
    30000};

/* static */
int MetaDataLogWriter::recoveryTargetThread(logid_t meta_logid, int nthreads) {
  ld_check(nthreads > 0);
  return folly::hash::twang_mix64(meta_logid.val_) % nthreads;
}

namespace {

// Request used to initiate metadata log recovery on a Worker thread
class StartMetaDataLogRecoveryRequest : public Request {
 public:
  explicit StartMetaDataLogRecoveryRequest(logid_t meta_logid)
      : Request(RequestType::START_METADATA_LOG_RECOVERY),
        meta_logid_(meta_logid) {
    ld_check(MetaDataLog::isMetaDataLog(meta_logid_));
  }

  Request::Execution execute() override {
    auto& seqmap = Worker::onThisThread()->processor_->allSequencers();
    auto data_sequencer =
        seqmap.findSequencer(MetaDataLog::dataLogID(meta_logid_));
    if (data_sequencer && data_sequencer->getCurrentEpoch() > EPOCH_MIN) {
      // run a WriteMetaDataRecord state machine without an Appender to try to
      // recover last released lsn for the metadata log.
      // we don't care if runAppender() is successful or not, because if not,
      // there is likely a WriteMetaDataRecord state machine running, and
      // last_released_ will be updated once it finishes.
      data_sequencer->getMetaDataLogWriter()->runAppender(nullptr);
    }
    return Execution::COMPLETE;
  }

  // recovery-only WriteMetaDataRecord state machine for a metadata log
  // is pinned to the same worker thread
  int getThreadAffinity(int nthreads) override {
    return MetaDataLogWriter::recoveryTargetThread(meta_logid_, nthreads);
  }

 private:
  const logid_t meta_logid_;
};

// abort a recovery-only WriteMetaDataRecord state machine for @param meta_logid
// on the current worker thread
void abortMetaDataRecoveryOnWorker(logid_t meta_logid) {
  auto& writes_map = Worker::onThisThread()->runningWriteMetaDataRecords().map;
  auto it = writes_map.find(meta_logid);
  if (it != writes_map.end()) {
    WriteMetaDataRecord* write = it->second;
    ld_check(write != nullptr);
    if (write->isRecoveryOnly()) {
      write->abort();
    }
  }
}

// if there are buffered metadata appenders on the current thread,
// drain by restarting them
void drainBufferedAppenders(logid_t meta_logid) {
  ld_check(MetaDataLog::isMetaDataLog(meta_logid));
  Worker::onThisThread()->appenderBuffer().processQueue(
      meta_logid, AppenderBuffer::processBufferedAppender);
}

// request to cancel metadata log recovery on the target worker thread
class AbortMetaDataLogRecoveryRequest : public Request {
 public:
  explicit AbortMetaDataLogRecoveryRequest(logid_t meta_logid)
      : Request(RequestType::ABORT_METADATA_LOG_RECOVERY),
        meta_logid_(meta_logid) {
    ld_check(MetaDataLog::isMetaDataLog(meta_logid_));
  }

  Request::Execution execute() override {
    abortMetaDataRecoveryOnWorker(meta_logid_);
    return Execution::COMPLETE;
  }

  int getThreadAffinity(int nthreads) override {
    return MetaDataLogWriter::recoveryTargetThread(meta_logid_, nthreads);
  }

 private:
  const logid_t meta_logid_;
};

} // anonymous namespace

MetaDataLogWriter::MetaDataLogWriter(Sequencer* sequencer)
    : state_(State::READY), sequencer_(sequencer) {
  ld_check(sequencer != nullptr);
  // parent must be a data log sequencer
  ld_check(!MetaDataLog::isMetaDataLog(sequencer->getLogID()));
}

lsn_t MetaDataLogWriter::getLastReleased(bool recover) {
  lsn_t last_released = last_released_.load();
  if (recover && last_released == LSN_INVALID &&
      !Worker::settings().bypass_recovery) {
    recoverMetaDataLog();
  }
  return last_released;
}

void MetaDataLogWriter::notePreempted(epoch_t epoch, NodeID preempted_by) {
  ld_spew("PREEMPTED by %s for epoch %u of log %lu",
          preempted_by.toString().c_str(),
          epoch.val(),
          getDataLogID().val());
  sequencer_->notePreempted(epoch, preempted_by);
}

void MetaDataLogWriter::recoverMetaDataLog() {
  if (sequencer_->getCurrentEpoch() > EPOCH_MIN) {
    std::unique_ptr<Request> rq =
        std::make_unique<StartMetaDataLogRecoveryRequest>(getMetaDataLogID());
    Worker::onThisThread()->processor_->postWithRetrying(rq);
  }
}

void MetaDataLogWriter::abortRecovery() {
  const logid_t meta_logid = getMetaDataLogID();
  std::unique_ptr<Request> rq =
      std::make_unique<AbortMetaDataLogRecoveryRequest>(meta_logid);
  // best effort, do not care if the request failed to post
  Worker::onThisThread()->processor_->postRequest(rq);
}

int MetaDataLogWriter::checkAppenderPayload(Appender* appender,
                                            epoch_t epoch_from_parent) {
  ld_check(appender != nullptr);
  PayloadHolder* ph = const_cast<PayloadHolder*>(appender->getPayload());
  EpochMetaData metadata;

  Payload pl = ph->getPayload();
  STORE_flags_t flags = appender->getPassthruFlags();
  size_t checksum_bytes = 0;
  if (flags & RECORD_Header::CHECKSUM) {
    checksum_bytes = flags & RECORD_Header::CHECKSUM_64BIT ? 8 : 4;
  }
  const auto& nc = Worker::onThisThread()->getNodesConfiguration();
  if (metadata.fromPayload(
          Payload(static_cast<const char*>(pl.data()) + checksum_bytes,
                  pl.size() - checksum_bytes),
          getDataLogID(),
          *nc)) {
    err = E::BADPAYLOAD;
    return -1;
  }

  ld_check(metadata.isValid());
  if (metadata.writtenInMetaDataLog()) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Epoch metadata to be written for log %lu should not have "
                    "WRITTEN_IN_METADATALOG flag set.",
                    getDataLogID().val_);
    err = E::BADPAYLOAD;
    return -1;
  }

  if (metadata.disabled()) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Epoch metadata to be written for log %lu should not have "
                    "DISABLED flag set.",
                    getDataLogID().val_);
    err = E::BADPAYLOAD;
    return -1;
  }

  // valid metadata implies metadata->h.epoch.val_ >= 1
  if (epoch_from_parent.val_ < metadata.h.epoch.val_ - 1) {
    err = E::STALE;
    return -1;
  }

  return 0;
}

// caller of this function owns the appender
RunAppenderStatus MetaDataLogWriter::runAppender(Appender* appender) {
  const logid_t log_id(getDataLogID());
  const logid_t meta_logid(getMetaDataLogID());
  const epoch_t epoch_from_parent = sequencer_->getCurrentEpoch();

  if (epoch_from_parent == EPOCH_INVALID) {
    // data sequencer has not gotten its first epoch from the epoch store. It is
    // possible that it is in ACTIVATING, try buffer the appender until data
    // sequencer gets its first epoch. It is also possible that the activation
    // of data sequencer encountered a transient error, return E::NOSEQUENCER
    // and logdeviced will retry the activation later.
    if (appender && sequencer_->getState() == Sequencer::State::ACTIVATING) {
      // E::INPROGRESS will cause the metadata appender buffered on the worker
      err = E::INPROGRESS;
      return RunAppenderStatus::ERROR_DELETE;
    }

    err = E::NOSEQUENCER;
    return RunAppenderStatus::ERROR_DELETE;
  }

  if (appender != nullptr) {
    auto acceptable_epoch = appender->getAcceptableEpoch();
    if (acceptable_epoch.hasValue()) {
      if (acceptable_epoch.value() != epoch_from_parent) {
        // Current epoch is unacceptable to this appender
        err = E::ABORTED;
        return RunAppenderStatus::ERROR_DELETE;
      }
    }
  }

  State prev = State::READY;
  if (!state_.compare_exchange_strong(prev, State::WRITE_STARTED)) {
    if (appender != nullptr && prev != State::SHUTDOWN &&
        recovery_only_.load()) {
      // In case that there is a WriteMetaDataRecord state machine running, and:
      // 1) a metadata record needs to be appended; and
      // 2) system is not shutting down; and
      // 3) the running state machine is just for recovering the metadata log
      // Try buffering the current appender while sending a request to the
      // worker that runs the last WriteMetaDataRecord to abort the recovery
      // effort so that the append can run sooner.
      // Note that since state_ and recovery_only_ are not updated atomically,
      // there are race conditions in which recovery_only_ and state_ may have
      // inconsistent values. But it is still fine
      // since 1) the abort of recovery is a best effort approach and it is OK
      // even if we miss it by posting to the wrong worker; and 2) we ensure
      // that the abort request won't cancel a WriteMetaDataRecord that actually
      // does append.

      ld_check(current_worker_ != worker_id_t(-1));
      abortRecovery();

      // E::INPROGRESS will cause the metadata appender buffered on worker
      err = E::INPROGRESS;
      return RunAppenderStatus::ERROR_DELETE;
    }

    // logdevice is shutting down or current write operation is in progress,
    // report back to the client
    err = prev == State::SHUTDOWN ? E::SHUTDOWN : E::NOBUFS;
    return RunAppenderStatus::ERROR_DELETE;
  }

  // the rest of the function is ensured to run on a SINGLE Worker which
  // changed the state_
  current_worker_ = Worker::onThisThread()->idx_;

  auto& writes_map = Worker::onThisThread()->runningWriteMetaDataRecords().map;
  std::pair<WriteMetaDataRecordMap::RawMap::iterator, bool> insert_result;

  ld_check(running_write_ == nullptr);

  if (epoch_from_parent.val_ <= last_writer_epoch_.load()) {
    // This indicates that the previous run of metadata writer does not
    // clean up properly before transitioning into State::READY
    ld_critical("Attempt to get an epoch for WriteMetaDataRecord state machine "
                "but it is not greater than the previous one. Data log %lu, "
                "parent epoch %u, previous epoch %u",
                log_id.val_,
                epoch_from_parent.val_,
                current_epoch_.load());
    ld_check(false);
    err = E::INTERNAL;
    goto not_started;
  }

  // examine the payload and expect it to be valid epoch metadata, also check if
  // the epoch of data sequencer is too old to store the record. Reject the
  // append if it failed to meet either condition. This is the last line of
  // defense since we really don't want to pollute the metadata log
  if (appender) {
    if (checkAppenderPayload(appender, epoch_from_parent) != 0) {
      if (err == E::BADPAYLOAD) {
        PayloadHolder* ph = const_cast<PayloadHolder*>(appender->getPayload());
        Payload pl = ph->getPayload();
        RATELIMIT_ERROR(std::chrono::seconds(1),
                        10,
                        "Failed to write metadata log for log %lu: appender "
                        "payload does not contain valid epoch metadata! "
                        "hex dump of payload: %s, size %lu",
                        log_id.val_,
                        hexdump_buf(pl.data(), pl.size()).c_str(),
                        pl.size());
      } else if (err == E::STALE) {
        RATELIMIT_WARNING(std::chrono::seconds(1),
                          10,
                          "Failed to write metadata log for log %lu: epoch %u "
                          "of the data sequencer is too old.",
                          log_id.val_,
                          epoch_from_parent.val_);

      } else {
        // currently only the above two errs are possible
        ld_check(false);
      }

      goto not_started;
    }
  }

  // only place where current_epoch_ got written
  current_epoch_.store(epoch_from_parent.val_);
  recovery_only_.store(appender == nullptr);

  running_write_ =
      std::make_unique<WriteMetaDataRecord>(log_id, epoch_from_parent, this);

  // state machine created, insert a _raw_ pointer into worker's map
  insert_result =
      writes_map.insert(std::make_pair(meta_logid, running_write_.get()));

  if (!insert_result.second) {
    ld_critical("Attempt to insert WriteMetaDataRecord state machine "
                "(epoch %u) to Worker but there already exist one with "
                "epoch %u for data log %lu",
                current_epoch_.load(),
                insert_result.first->second->getEpoch().val_,
                log_id.val_);
    ld_check(false);
    err = E::INTERNAL;
    goto not_started;
  }

  if (running_write_->start(appender) == RunAppenderStatus::ERROR_DELETE) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "Failed to start WriteMetaDataRecord state machine "
                    "for data log %lu, epoch %u. err: %s",
                    log_id.val_,
                    current_epoch_.load(),
                    error_description(err));
    // err set by start()
    goto not_started;
  }

  ld_debug("Started WriteMetaDataRecord state machine for data log %lu, "
           "epoch %u, recovery only: %s",
           log_id.val_,
           current_epoch_.load(),
           appender ? "false" : "true");

  WORKER_STAT_INCR(write_metadata_record_started);
  return RunAppenderStatus::SUCCESS_KEEP;

not_started:
  ld_check(err != E::OK);
  writes_map.erase(meta_logid);
  running_write_.reset();
  // reset State back to READY
  prev = state_.exchange(State::READY);
  ld_check(prev == State::WRITE_STARTED);
  return RunAppenderStatus::ERROR_DELETE;
}

void MetaDataLogWriter::onWriteMetaDataRecordDone(Status st,
                                                  lsn_t last_released) {
  // must be called on the same worker thread that creates the state machine
  ld_check(Worker::onThisThread()->idx_ == current_worker_);

  ld_check(state_.load() == State::WRITE_STARTED);
  ld_check(running_write_);
  ld_check(current_epoch_.load() == running_write_->getEpoch().val_);

  const logid_t log_id(sequencer_->getLogID());
  const bool recovery_only = running_write_->isRecoveryOnly();

  if (st == E::OK) {
    ld_debug("WriteMetaDataRecord state machine finished with SUCCESS for "
             "data log %lu, epoch %u",
             log_id.val_,
             running_write_->getEpoch().val_);

    // update last_released_ to the value reported
    lsn_t prev = atomic_fetch_max(last_released_, last_released);
    ld_check(prev <= last_released);
  } else {
    ld_warning("WriteMetaDataRecord state machine (epoch %u) finished with "
               "status %s for data log %lu",
               running_write_->getEpoch().val_,
               error_description(st),
               log_id.val_);
  }

  if (!recovery_only && st == E::OK) {
    // Only update the last_writer_epoch_ for actual writes and if
    // they succeeded.
    ld_check(last_writer_epoch_.load() < current_epoch_.load());
    last_writer_epoch_ = current_epoch_.load();
  }

  Worker* w = Worker::onThisThread();
  WriteMetaDataRecordMap::RawMap& writes = w->runningWriteMetaDataRecords().map;
  auto it = writes.find(MetaDataLog::metaDataLogID(log_id));
  ld_check(it != writes.end() && it->second == running_write_.get());
  writes.erase(it);
  running_write_.reset();

  WORKER_STAT_INCR(write_metadata_record_finished);

  State next = (st == E::SHUTDOWN ? State::SHUTDOWN : State::WRITE_DONE);
  State prev = state_.exchange(next);
  ld_check(prev == State::WRITE_STARTED);

  if (next == State::SHUTDOWN) {
    // logdeviced is shutting down, do not perform activation or drain appenders
    return;
  }

  epoch_t data_sequencer_epoch = sequencer_->getCurrentEpoch();
  if (data_sequencer_epoch == EPOCH_INVALID) {
    // sequencer is in UNAVAILABLE or PERMANENT_ERROR
    ld_warning("WriteMetaDataRecord state machine (epoch %u) finished with "
               "status %s for data log %lu but the sequencer current epoch is "
               "EPOCH_INVALID, sequencer state: %s.",
               current_epoch_.load(),
               error_description(st),
               log_id.val_,
               Sequencer::stateString(sequencer_->getState()));
    return;
  }

  // parent data sequencer can be activated in the meantime, so expect its
  // epoch equal or larger than the epoch which the state machine was started
  // with.
  ld_check(data_sequencer_epoch.val_ >= current_epoch_.load());
  if (!recovery_only && data_sequencer_epoch.val_ == current_epoch_.load() &&
      st != E::PREEMPTED) {
    // Reactivate parent sequencer to a higher epoch to ensure:
    // 1) sequencer is able to get the latest epoch metadata from epoch store
    // 2) next metadata log write will use a strictly higher epoch
    // It is possible that parent sequencer may have already been activated
    // by others to a new epoch greater than current_epoch_. Skip reactivation
    // in such case.
    RATELIMIT_INFO(std::chrono::seconds(10),
                   100,
                   "Attempting sequencer re-activation for log:%lu"
                   ", current_epoch_:%u",
                   log_id.val_,
                   current_epoch_.load());
    STAT_INCR(Worker::stats(), sequencer_activations_metadata_record_written);
    w->processor_->allSequencers().reactivateSequencer(
        log_id, "wrote to metadata log");
    // start a timer on this worker to ensure the parent sequencer eventually
    // activates to a higer epoch
    ExponentialBackoffTimerNode* node =
        w->registerTimer(std::bind(&MetaDataLogWriter::checkActivation,
                                   log_id,
                                   epoch_t(current_epoch_.load()),
                                   std::placeholders::_1),
                         RETRY_ACTIVATION_INITIAL_DELAY,
                         RETRY_ACTIVATION_MAX_DELAY);
    node->timer->activate();
  } else {
    if (st == E::PREEMPTED) {
      ld_warning("WriteMetaDataRecord state machine (epoch %u) finished with "
                 "status PREEMPTED for data log %lu, sequencer state: %s.",
                 current_epoch_.load(),
                 log_id.val(),
                 Sequencer::stateString(sequencer_->getState()));
    }

    // the job has finished, get back to ready state.
    // note that we may race with onNormalSequencerReactivated()
    State expected = State::WRITE_DONE;
    state_.compare_exchange_strong(expected, State::READY);
    drainBufferedAppenders(MetaDataLog::metaDataLogID(log_id));

    if (recovery_only &&
        sequencer_->getState() != Sequencer::State::ACTIVATING) {
      // post completion request to all worker thread draining all buffered
      // appenders
      w->processor_->allSequencers().notifyWorkerActivationCompletion(
          MetaDataLog::metaDataLogID(log_id), E::OK);
    }
  }
}

void MetaDataLogWriter::checkActivation(logid_t log_id,
                                        epoch_t epoch_before,
                                        ExponentialBackoffTimerNode* node) {
  Worker* worker = Worker::onThisThread();
  std::shared_ptr<Sequencer> seq =
      worker->processor_->allSequencers().findSequencer(log_id);
  if (!seq || seq->getState() == Sequencer::State::PERMANENT_ERROR) {
    // sequencer not exist or in permanent error, cancel timer
    ld_error("Sequencer for log %lu does not exist or is in permanent error "
             "MetaDataLogWriter cannot continue.",
             log_id.val_);
    ld_check(node->list_hook.is_linked());
    delete node;
    return;
  }
  const epoch_t epoch_now = seq->getCurrentEpoch();
  if (epoch_now > epoch_before) {
    // sequencer advanced to a new epoch larger than epoch_before
    ld_debug("Sequencer for log %lu has successfully activated to a new epoch "
             "%u larger than %u, destroying the timer.",
             log_id.val_,
             epoch_now.val_,
             epoch_before.val_);
    ld_check(node->list_hook.is_linked());
    delete node;
    return;
  }
  // sequencer is not yet activated, retry reactivation
  RATELIMIT_INFO(std::chrono::seconds(10),
                 100,
                 "Attempting sequencer re-activation for log:%lu"
                 ", epoch_now:%u",
                 log_id.val_,
                 epoch_now.val_);
  worker->processor_->allSequencers().reactivateSequencer(
      log_id, "wrote to metadata log (retry)");
  node->timer->activate();
}

void MetaDataLogWriter::onDataSequencerReactivated(epoch_t /*epoch*/) {
  // Note: this may happen on a different thread for an activation not requested
  // by us

  State next, prev = state_.load();
  do {
    /**
     * We are ready to process the next append only when:
     * 1. the current WriteMetaDataRecord machine was finished; and
     * 2. the data log sequencer has reactivaed to an epoch > current_epoch_
     */
    if (prev != State::WRITE_DONE ||
        sequencer_->getCurrentEpoch().val_ <= current_epoch_.load()) {
      return;
    }
    next = State::READY;
  } while (!state_.compare_exchange_strong(prev, next));

  drainBufferedAppenders(getMetaDataLogID());
  ld_debug("COMPLETED metadata log write for data log %lu, epoch %u.",
           sequencer_->getLogID().val_,
           current_epoch_.load());
}

void MetaDataLogWriter::onWorkerShutdown() {
  // must called on the same worker thread that creates the state machine
  ld_check(Worker::onThisThread()->idx_ == current_worker_);

  // onWorkerShutdown() is only called by Worker on MetaDataLogWriter object
  // that are running a WriteMetaDataRecord state machine.
  // Destory the state machine as well as the meta sequencer that it owns.
  // At this time we expect all appenders to have been retired and reaped.
  ld_check(running_write_);
  running_write_.reset();
  state_.store(State::SHUTDOWN);
}

MetaDataLogWriter::~MetaDataLogWriter() {
  // currently MetaDataLogWriter destructs with the sequencer. At that time
  // all workers have already been destructed.
  ld_check(running_write_ == nullptr);
}

logid_t MetaDataLogWriter::getDataLogID() const {
  return sequencer_->getLogID();
}

logid_t MetaDataLogWriter::getMetaDataLogID() const {
  return MetaDataLog::metaDataLogID(getDataLogID());
}

}} // namespace facebook::logdevice
