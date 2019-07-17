/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/LogRecoveryRequest.h"

#include <tuple>
#include <utility>

#include <folly/Memory.h>
#include <folly/hash/Hash.h>

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/AllSequencers.h"
#include "logdevice/common/LogIDUniqueQueue.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sequencer.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/stats/ServerHistograms.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

const std::chrono::milliseconds LogRecoveryRequest::SEAL_RETRY_INITIAL_DELAY{
    500};
const std::chrono::milliseconds LogRecoveryRequest::SEAL_RETRY_MAX_DELAY{5000};
const std::chrono::milliseconds LogRecoveryRequest::SEAL_CHECK_INTERVAL{15000};

RecoveredLSNs::~RecoveredLSNs() {}

void RecoveredLSNs::noteMutationsCompleted(
    const EpochRecovery& epoch_recovery) {
  esn_t last_esn = epoch_recovery.getDigest().getLastEsn();
  if (last_esn == ESN_INVALID) {
    return;
  }

  ld_debug("Epoch recovery completed for epoch %d of log:%lu: last_esn:%d",
           epoch_recovery.epoch_.val(),
           epoch_recovery.getLogID().val(),
           last_esn.val());
  last_esns_[epoch_recovery.epoch_] = last_esn;

  for (const auto& esn_and_entry : epoch_recovery.getDigest()) {
    if (esn_and_entry.second.isHolePlug()) {
      lsn_t lsn = compose_lsn(epoch_recovery.epoch_, esn_and_entry.first);
      ld_debug("Adding hole %s for log:%lu",
               lsn_to_string(lsn).c_str(),
               epoch_recovery.getLogID().val());
      hole_lsns_.insert(lsn);
    }
  }
}

RecoveredLSNs::RecoveryStatus RecoveredLSNs::recoveryStatus(lsn_t lsn) const {
  epoch_t epoch = lsn_to_epoch(lsn);
  auto it = last_esns_.find(epoch);
  if (it == last_esns_.end()) {
    ld_debug("Couldn't find epoch %d in recovered epochs", epoch.val());
    return RecoveryStatus::UNKNOWN;
  }

  if (lsn_to_esn(lsn) > it->second) {
    ld_debug("lsn %s is greater than last lsn of recovered epoch %d",
             lsn_to_string(lsn).c_str(),
             epoch.val());
    return RecoveryStatus::HOLE;
  }

  if (hole_lsns_.find(lsn) != hole_lsns_.end()) {
    ld_debug("A hole was plugged at lsn %s", lsn_to_string(lsn).c_str());
    return RecoveryStatus::HOLE;
  } else {
    ld_debug("lsn %s was successfully recovered", lsn_to_string(lsn).c_str());
    return RecoveryStatus::REPLICATED;
  }
}

int LogRecoveryRequest::getThreadAffinity(int nthreads) {
  // If setTargetWorker() is called, pin the recovery request to the specified
  // worker thread
  if (target_worker_.hasValue()) {
    worker_id_t target = target_worker_.value();
    ld_check(target.val_ >= 0 && target.val_ < nthreads);
    return target.val_;
  }

  // Otherwise, in order to make it easy to ensure that at most one
  // LogRecoveryRequest is running for each log, all requests for the same log
  // id are pinned to the same worker thread.
  return folly::hash::twang_mix64(log_id_.val_) % nthreads;
}

LogRecoveryRequest::LogRecoveryRequest(
    logid_t log_id,
    epoch_t next_epoch,
    std::shared_ptr<const EpochMetaData> seq_metadata,
    std::chrono::milliseconds delay)
    : Request(RequestType::LOG_RECOVERY),
      log_id_(log_id),
      next_epoch_(next_epoch),
      seq_metadata_(std::move(seq_metadata)),
      delay_(delay),
      recovered_lsns_(new RecoveredLSNs()),
      creation_timestamp_(SteadyTimestamp::now()) {
  ld_check(log_id != LOGID_INVALID);
  ld_check(next_epoch > EPOCH_INVALID);
  ld_check(seq_metadata_ != nullptr);
  // sequencer metadata shouldn't be disabled, otherwise sequencer would
  // refuse to activate
  ld_check(!seq_metadata_->disabled());
  ld_check(next_epoch == seq_metadata_->h.epoch);
}

LogRecoveryRequest::~LogRecoveryRequest() {
  // It is possible that the LogRecoveryRequest object is destructed within a
  // MetaDataLogReader callback (e.g., encountered an error reading the metadata
  // log). Therefore, it is unsafe to destroy these metadata reader objects in
  // destructor, instead, pass the ownership to Worker to dispose them later.
  if (meta_reader_) {
    Worker::onThisThread()->disposeOfMetaReader(std::move(meta_reader_));
  }
}

Request::Execution LogRecoveryRequest::execute() {
  Worker* w = Worker::onThisThread();
  auto& map = w->runningLogRecoveries().map;

  auto it = map.find(log_id_);
  if (it != map.end()) {
    WORKER_STAT_INCR(recovery_completed);

    if (it->second->getEpoch() >= getEpoch()) {
      // Existing LogRecoveryRequest is for an epoch that's at least as recent
      // as this one. This should be impossible since Processor::postImportant()
      // guarantees ordering.

      ld_warning("Attempting to start log recovery for log %lu epoch %u while "
                 "there is already a LogRecoveryRequest running with epoch %u.",
                 log_id_.val_,
                 getEpoch().val_,
                 it->second->getEpoch().val_);

      w->popRecoveryRequest();
      return Execution::COMPLETE;
    }

    // Delete the stale LogRecoveryRequest
    map.erase(it);
  }

  const auto sequencer = w->processor_->allSequencers().findSequencer(log_id_);
  if (!sequencer || lsn_to_epoch(sequencer->getLastReleased()) >= next_epoch_) {
    // There's nothing to do if the sequencer no longer exists or the log was
    // already recovered at least up to `next_epoch_'.
    RATELIMIT_INFO(std::chrono::seconds(10),
                   10,
                   "Log %lu was already recovered up to epoch %u, skipping.",
                   log_id_.val_,
                   next_epoch_.val_);
    WORKER_STAT_INCR(recovery_completed);
    w->popRecoveryRequest();
    return Execution::COMPLETE;
  }

  // Check if this is an internal log. If it is, ensure it is not queued and
  // recovered immediately. This is important for the event log for instance as
  // recoveries of other logs may depend on the event log being readable.
  const bool internal = configuration::InternalLogs::isInternal(log_id_);

  const auto& settings = Worker::settings();
  const size_t recovery_requests_per_worker =
      (settings.concurrent_log_recoveries + settings.num_workers - 1) /
      settings.num_workers;

  // If log_id_ is a data log, stop running this recovery and enqueue the logid
  // when number of active requests reaches recovery_requests_per_worker.
  // However, if log_id_ is a metadata log, the threshold is doubled so that
  // metadata log recovery can be prioritized.
  if (!internal &&
      ((!MetaDataLog::isMetaDataLog(log_id_) &&
        map.size() >= recovery_requests_per_worker) ||
       (MetaDataLog::isMetaDataLog(log_id_) &&
        map.size() >= recovery_requests_per_worker * 2))) {
    // Too many recovery requests are currently active. Add this log to Worker's
    // recoveryQueue_ so it'll be picked up when another LogRecoveryRequest
    // completes.

    auto& recovery_queue = MetaDataLog::isMetaDataLog(log_id_)
        ? Worker::onThisThread()->recoveryQueueMetaDataLog().q
        : Worker::onThisThread()->recoveryQueueDataLog().q;

    auto& index = recovery_queue.get<LogIDUniqueQueue::FIFOIndex>();
    auto result = index.push_back(log_id_);
    if (!result.second) {
      // insertion failed. It is possible that the same logid is already
      // enqueued.
      RATELIMIT_INFO(std::chrono::seconds(10),
                     10,
                     "Failed to insert log recovery request of log %lu next "
                     "epoch %u to the recovery queue: it is likely that the "
                     "same logid has already been enqueued.",
                     log_id_.val_,
                     next_epoch_.val_);
      WORKER_STAT_INCR(recovery_completed);
    } else {
      WORKER_STAT_INCR(recovery_enqueued);
    }
    return Execution::COMPLETE;
  }

  std::unique_ptr<LogRecoveryRequest> rq(this);
  auto insert_it = map.insert(std::make_pair(log_id_, std::move(rq)));
  ld_check(insert_it.second);

  if (settings.skip_recovery) {
    skipRecovery();
    return Execution::CONTINUE;
  }

  if (delay_ > std::chrono::milliseconds::zero()) {
    start_delay_timer_ = std::make_unique<Timer>([this] { start(); });

    // Defer log recovery, for delay_ milliseconds. We do this when we retry
    // recovery after a soft error.
    start_delay_timer_->activate(delay_);
    return Execution::CONTINUE;
  }

  registerForShardAuthoritativeStatusUpdates();
  start();
  return Execution::CONTINUE;
}

void LogRecoveryRequest::skipRecovery() {
  RATELIMIT_INFO(std::chrono::seconds(10),
                 10,
                 "Skipping recovery of epochs up to %u of log %lu because "
                 "--skip-recovery setting is used",
                 next_epoch_.val_,
                 log_id_.val_);

  EpochStore& epoch_store =
      Worker::onThisThread()->processor_->allSequencers().getEpochStore();
  TailRecord tail_record(
      {getLogID(),
       LSN_INVALID,
       0,
       {BYTE_OFFSET_INVALID /* deprecated, OffsetMap used instead */},
       /*flags*/ 0,
       {}},
      OffsetMap(),
      std::shared_ptr<PayloadHolder>());

  ld_check(tail_record.isValid());
  ld_check(!tail_record.containOffsetWithinEpoch());

  // Immediately update the epoch store to set LCE to next_epoch_ - 1.
  // By setting invalid attributes epoch store forced to use latest previous
  // stored attributes if lce is going to be changed. This is best effort, we
  // do not care if the request succeeds or not.
  epoch_store.setLastCleanEpoch(
      log_id_,
      epoch_t(next_epoch_.val_ - 1),
      tail_record,
      [](Status status, logid_t log_id, epoch_t lce, TailRecord) {
        if (status != E::OK) {
          ld_error("Could not set last clean epoch %u for log %lu: %s",
                   lce.val_,
                   log_id.val_,
                   error_description(err));
        }
      });

  // Start a timer with zero delay to call complete(E::OK) in the next iteration
  // of the event loop.
  seq_metadata_read_ = true;
  all_epochs_recovered_ = true;
  tail_record_.assign(tail_record);
  completeSoon(E::OK);
}

void LogRecoveryRequest::start() {
  start_delay_timer_.reset();

  // If the log is a data log, log recovery can only complete when epoch
  // metadata for next_epoch_ can be found in the metadata log. Therefore,
  // before starting the first step of the recovery, start a MetaDataLogReader
  // object that asynchronously reads metadata log and wait for the sequencer
  // metadata to appear.
  if (!MetaDataLog::isMetaDataLog(log_id_)) {
    readSequencerMetaData();
    WORKER_STAT_INCR(num_recovery_reading_sequencer_metadata);
  } else {
    // for metadata log, directly set the seq metadata read flag to true;
    // also skip reading metadata log if
    // force-no-metadata-logs-in_recovery is set.
    seq_metadata_read_ = true;
  }

  ld_info("Started LogRecoveryRequest of log %lu with next_epoch %u",
          log_id_.val_,
          next_epoch_.val_);

  // begin the first step of recovery: get last clean epoch of the log from the
  // epoch store.
  getLastCleanEpoch();
}

void LogRecoveryRequest::getLastCleanEpoch() {
  EpochStore& epoch_store =
      Worker::onThisThread()->processor_->allSequencers().getEpochStore();

  if (!lce_backoff_timer_) {
    lce_backoff_timer_.reset(new ExponentialBackoffTimer(

        std::bind(&LogRecoveryRequest::getLastCleanEpoch, this),
        std::chrono::milliseconds(100),
        std::chrono::milliseconds(10000)));
  }

  const epoch_t recovery_epoch = next_epoch_;
  const request_id_t rqid = id_;
  int rv = epoch_store.getLastCleanEpoch(
      log_id_,
      [=](Status status,
          logid_t log_id,
          epoch_t epoch,
          TailRecord tail_record) {
        const auto& recovery_map =
            Worker::onThisThread()->runningLogRecoveries().map;
        auto it = recovery_map.find(log_id);
        if (it == recovery_map.end() ||
            it->second->getEpoch() != recovery_epoch ||
            it->second->id_ != rqid) {
          // stale response for a LogRecoveryRequest that no longer exists
          return;
        }

        it->second->getLastCleanEpochCF(status, epoch, std::move(tail_record));
      });

  if (rv != 0) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Failed to request last clean epoch number for log %lu: %s",
                    log_id_.val_,
                    error_description(err));

    // try again later
    lce_backoff_timer_->activate();
  }
}

void LogRecoveryRequest::getLastCleanEpochCF(Status status,
                                             epoch_t lce,
                                             TailRecord tail_record) {
  ld_check(lce_backoff_timer_ != nullptr);

  if (status != E::OK) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Attempt to get last clean epoch number for log %lu "
                    "failed: %s",
                    log_id_.val_,
                    error_description(status));

    lce_backoff_timer_->activate();
    return;
  }

  ld_check(tail_record.isValid());
  lce_backoff_timer_->reset(); // no longer needed
  last_clean_epoch_.assign(lce);
  tail_record_.assign(std::move(tail_record));

  if (lce >= next_epoch_) {
    ld_warning(
        "Last Clean Epoch (%u) for metadata log %lu is greater or equal to the "
        "epoch of this Sequencer (%u). This sequencer was preempted. However, "
        "we took a shortcut in implementation, and we're going to ignore the "
        "preemption. It's a (supposedly harmless) hack.",
        lce.val_,
        log_id_.val_,
        next_epoch_.val_);
    // Can't handle the preemption properly because the LCE znode doesn't have
    // preempting node ID. So we'll just: (a) for metadata logs, pretend that
    // the recovery succeeded, (b) for data logs, fail the recovery but not
    // transition the sequencer into preempted state; hopefully it'll notice
    // the preemption through seals soon.
    // An alternative would be to reactivate the sequencer, but that would be
    // slightly more code. Ways to fix it properly:
    //  * add preempting node ID to LCE znode, or
    //  * change epoch store entirely, replacing zookeeper with metadata log;
    //    this will also fix the correctness problem with seals when changing
    //    nodeset.
    if (MetaDataLog::isMetaDataLog(log_id_)) {
      allEpochsRecovered();
    } else {
      complete(E::PREEMPTED);
    }
    return;
  }

  if (lce.val_ == next_epoch_.val_ - 1) {
    ld_info("Last Clean Epoch (%u) for log %lu is one less than the epoch "
            "of this Sequencer (%u). Skipping recovery for these epochs.",
            lce.val_,
            log_id_.val_,
            next_epoch_.val_);

    // there is no epoch that needs recovery, however, LogRecoveryRequest still
    // needs to wait until epoch metadata for next_epoch_ can be read from
    // metadata log before finishing recovery and releasing records in the new
    // epoch
    allEpochsRecovered();
    return;
  }

  // advance to the second step of recovery: get epoch metadata from the
  // metadata log for all epochs that need recovery
  getEpochMetaData();
}

void LogRecoveryRequest::getEpochMetaData() {
  // must have fetched lce
  ld_check(last_clean_epoch_.hasValue());
  const epoch_t last_clean_epoch = last_clean_epoch_.value();

  // There should be unclean epochs to recover otherwise the state machine would
  // have finished early
  ld_check(last_clean_epoch.val_ < next_epoch_.val_ - 1);

  // epochs that are going to be recovered are [lce + 1, next_epoch - 1].
  // Noted that epoch metadata for epoch range [effective_since, next_epoch - 1]
  // (valid if effective_since <= next_epoch-1) are already known to us, where
  // effective_since is the `effective_since' field of the sequencer metadata
  // for next_epoch. The epoch metadata is fetched from the epoch store by the
  // sequencer, and it should be consistent with the epoch metadata in the
  // metadata log. So if effective_since <= lce + 1, we can conclude without
  // reading metadata log records from meta storage nodes. Otherwise, start a
  // metadata log reader to request epoch metadata for epochs in
  // [lce + 1, effective_since - 1] in an asynchronous manner.
  ld_check(seq_metadata_);
  const epoch_t effective_since = seq_metadata_->h.effective_since;
  ld_check(effective_since <= next_epoch_);

  // if the log is a metadata log, we expect the effective_since field of
  // its sequencer metadata to be EPOCH_MIN, so that it is guaranteed that
  // epoch metadata for all recoverying epochs are known
  ld_check(!MetaDataLog::isMetaDataLog(log_id_) ||
           effective_since == EPOCH_MIN);

  if (effective_since.val_ <= last_clean_epoch.val_ + 1) {
    // no need to read epoch metadata
    finalizeEpochMetaData();
  } else {
    // must not be a metadata log
    ld_check(!MetaDataLog::isMetaDataLog(log_id_));
    ld_debug("Started reading epoch metadata for epochs [%u, %u] of log %lu, "
             "from metadata storage nodes",
             last_clean_epoch.val_ + 1,
             effective_since.val_ - 1,
             log_id_.val_);

    ld_check(meta_reader_ == nullptr);
    meta_reader_ = std::make_unique<MetaDataLogReader>(
        log_id_,
        epoch_t(last_clean_epoch.val_ + 1),
        epoch_t(effective_since.val_ - 1),
        std::bind(&LogRecoveryRequest::onEpochMetaData,
                  this,
                  std::placeholders::_1,
                  std::placeholders::_2));

    meta_reader_->start();
  }
}

int LogRecoveryRequest::createEpochRecoveryMachines(
    epoch_t start,
    epoch_t until,
    const EpochMetaData& metadata) {
  ld_check(start <= until);
  ld_check(metadata.isValid());
  auto config = Worker::onThisThread()->getConfig();
  const auto& nc = Worker::onThisThread()->getNodesConfiguration();
  auto log = config->getLogGroupByIDShared(log_id_);
  if (!log) {
    // config has changed since the time this Sequencer was activated
    // log_id_ is no longer there.
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    100,
                    "Log %lu is no longer in cluster config. "
                    "Skipping recovery.",
                    log_id_.val_);
    err = E::NOTINCONFIG;
    return -1;
  }

  // Merge the nodeset into recovery_nodes_ to know that we should seal them.
  recovery_nodes_.insert(metadata.shards.begin(), metadata.shards.end());

  // create machines for epochs [start, until]
  for (epoch_t::raw_type e = start.val_; e <= until.val_; ++e) {
    auto erm_deps = std::make_unique<EpochRecoveryDependencies>(this);

    epoch_recovery_machines_.emplace_back(log_id_,
                                          epoch_t(e),
                                          metadata,
                                          nc,
                                          std::move(erm_deps),
                                          log->attrs().tailOptimized().value());
  }
  return 0;
}

void LogRecoveryRequest::onEpochMetaData(Status st,
                                         MetaDataLogReader::Result result) {
  ld_check(result.log_id == log_id_);
  ld_check(last_clean_epoch_.hasValue());
  const epoch_t last_clean_epoch = last_clean_epoch_.value();
  const epoch_t seq_metadata_effective_since = seq_metadata_->h.effective_since;

  const epoch_t epoch = result.epoch_req;
  epoch_t until = result.epoch_until;

  if (epoch_metadata_finalized_) {
    // this is a stale callback, should not happen since we destroy the metadata
    // reader once complete
    ld_critical("Got a stale epoch metadata callback for epoch %u when all "
                "epoch metadata for epochs [%u, %u] are already known.",
                epoch.val_,
                last_clean_epoch.val_ + 1,
                next_epoch_.val_ - 1);
    ld_check(false);
    return;
  }

  if (st == E::NOTINCONFIG) {
    epoch_metadata_finalized_ = true;
    complete(st);
    // `this' is destroyed
    return;
  }

  if (st != E::OK) {
    // Data loss or malformed epoch metadata record! Similar to the client read
    // stream case, a few options are available:
    // (1) Abort the log recovery request with a failure status, the sequencer
    //     will keep retrying recovery until the metadata log is repaired
    // (2) Fall back to performing recovery from all storage nodes in the
    //     cluster. This may still yield correct results
    //     (with replicationFactorHistorical) but may be undesirable when the
    //     size of the cluster is large.
    // Currently option (1) is used.

    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Failed to fetch metadata for epoch %u of log %lu: %s. "
                    "No metadata until epoch %u. Will retry.",
                    epoch.val_,
                    log_id_.val_,
                    error_description(st),
                    until.val_);
    if (st == E::NOTFOUND) {
      // In recovery, empty metadata log indicates data loss, corruption or bug.
      // MetaDataLogReader doesn't treat NOTFOUND as error, so let's escalate.
      WORKER_STAT_INCR(metadata_log_read_failed_corruption);
    }

    epoch_metadata_finalized_ = true;
    complete(st);
    // `this' is destroyed
    return;
  }

  // MetaDataLogReader must deliver us epoch intervals in order and next to
  // each other
  const epoch_t::raw_type epoch_expected = epoch_recovery_machines_.empty()
      ? last_clean_epoch.val_ + 1
      : epoch_recovery_machines_.back().epoch_.val_ + 1;

  ld_check(epoch_expected == epoch.val_);
  ld_check(result.metadata);
  EpochMetaData metadata = *result.metadata;
  ld_check(metadata.isValid());

  ld_spew("Got metadata for epoch %u for log %lu. metadata epoch %u, "
          "effective until %u, metadata: %s",
          epoch.val_,
          log_id_.val_,
          metadata.h.epoch.val_,
          until.val_,
          metadata.toString().c_str());

  // create epoch state machines for recovering epochs upto
  // seq_metadata_effective_since - 1, the rest of the state machines (if any)
  // will be created by finalizeEpochmetadata() using the sequencer metadata
  until = std::min(until, epoch_t(seq_metadata_effective_since.val_ - 1));

  // got epoch metadata for [epoch, until], create epoch recovery state machines
  // for these epochs and update the recovery nodeset
  if (createEpochRecoveryMachines(epoch, until, metadata) != 0) {
    ld_check(err == E::NOTINCONFIG);
    epoch_metadata_finalized_ = true;
    complete(err);
    // `this' is destroyed
    return;
  }

  if (until.val_ == seq_metadata_effective_since.val_ - 1) {
    // got the last one needed, finalize the epoch metadata collection
    finalizeEpochMetaData();
  }
}

void LogRecoveryRequest::finalizeEpochMetaData() {
  ld_check(!epoch_metadata_finalized_);
  ld_check(seq_metadata_);
  ld_check(last_clean_epoch_.hasValue());
  const epoch_t last_clean_epoch = last_clean_epoch_.value();
  const epoch_t effective_since = seq_metadata_->h.effective_since;

  // if effective_since <= next_epoch - 1, create the last batch of epoch
  // state machines for [ max(lce + 1, effective_since), next_epoch - 1]
  // using the sequencer metadata
  if (effective_since.val_ <= next_epoch_.val_ - 1) {
    epoch_t start =
        std::max(effective_since, epoch_t(last_clean_epoch.val_ + 1));
    if (createEpochRecoveryMachines(
            start, epoch_t(next_epoch_.val_ - 1), *seq_metadata_) != 0) {
      ld_check(err == E::NOTINCONFIG);
      epoch_metadata_finalized_ = true;
      complete(err);
      // `this' is destroyed
      return;
    }
  }

  // check the final epoch state machine list, it should have exactly one
  // machine for every single epoch in [lce + 1, next_epoch_ - 1]
  auto it = epoch_recovery_machines_.begin();
  epoch_t::raw_type e = last_clean_epoch.val_ + 1;
  for (; e < next_epoch_.val_; ++e, ++it) {
    ld_check(it != epoch_recovery_machines_.end());
    ld_check(it->epoch_.val_ == e);
  }

  ld_spew("Finished collecting all epoch metadata [%u, %u] for log %lu.",
          last_clean_epoch.val_ + 1,
          next_epoch_.val_ - 1,
          log_id_.val_);

  epoch_metadata_finalized_ = true;
  // Dispose the metadata reader used to read the metadata log. Since the
  // current context may be within its metadata callback, it is not safe to
  // destroy the reader directly but rather transfer it to the worker to detroy
  // it later
  if (meta_reader_) {
    Worker::onThisThread()->disposeOfMetaReader(std::move(meta_reader_));
  }

  // We are done collecting epoch metadata for all recovering epochs, begin
  // the next stage of recovery of sealing logs
  sealLog();
}

void LogRecoveryRequest::readSequencerMetaData() {
  ld_check(!MetaDataLog::isMetaDataLog(log_id_));
  ld_check(!seq_metadata_read_);

  if (!seq_metadata_timer_) {
    auto timer = std::make_unique<ExponentialBackoffTimer>(

        [this]() {
          ld_check(!seq_metadata_read_); // Otherwise timer should be cancelled
          readSequencerMetaData();
        },
        Worker::settings().recovery_seq_metadata_timeout);

    seq_metadata_timer_ = std::move(timer);
  }

  const epoch_t effective_since = seq_metadata_->h.effective_since;
  ld_debug("Waiting for current epoch metadata for epoch %u of log %lu "
           "to appear in its metadata log...",
           effective_since.val_,
           log_id_.val_);

  if (seq_metadata_->writtenInMetaDataLog()) {
    // the epoch metadata got from the sequencer already indicate that
    // it's successfully written in metadata log
    onSequencerMetaDataFromEpochStore(E::OK, seq_metadata_.get());
    return;
  }

  // read from epoch store
  EpochStore& epoch_store =
      Worker::onThisThread()->processor_->allSequencers().getEpochStore();

  const epoch_t recovery_epoch = next_epoch_;
  const request_id_t rqid = id_;
  int rv = epoch_store.readMetaData(
      log_id_,
      [=](Status status,
          logid_t log_id,
          std::unique_ptr<EpochMetaData> metadata,
          std::unique_ptr<EpochStoreMetaProperties>) {
        const auto& recovery_map =
            Worker::onThisThread()->runningLogRecoveries().map;
        auto it = recovery_map.find(log_id);
        if (it == recovery_map.end() ||
            it->second->getEpoch() != recovery_epoch ||
            it->second->id_ != rqid) {
          // stale response for a LogRecoveryRequest that no longer exists
          return;
        }

        it->second->onSequencerMetaDataFromEpochStore(status, metadata.get());
      });

  if (rv != 0) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Failed to request reading sequencer metadata for "
                    "log %lu: %s",
                    log_id_.val_,
                    error_description(err));
    // start timer for retrying
    seq_metadata_timer_->activate();
  }
}

void LogRecoveryRequest::onSequencerMetaDataFromEpochStore(
    Status status,
    const EpochMetaData* metadata) {
  ld_check(seq_metadata_timer_ != nullptr);

  bool metadata_written = false;
  if (status != E::OK) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Failed to read epoch metadata from epoch store for "
                    "log %lu: %s",
                    log_id_.val_,
                    error_description(status));
  } else {
    ld_check(metadata != nullptr);
    const epoch_t seq_since = seq_metadata_->h.effective_since;

    ld_debug("Got metadata from EpochStore for log %lu: %s",
             log_id_.val_,
             metadata->toString().c_str());

    if (metadata->h.effective_since < seq_since) {
      RATELIMIT_CRITICAL(std::chrono::seconds(1),
                         1,
                         "Got the latest epoch metadata from epoch store for "
                         "log %lu but it has a smaller effective_since epoch "
                         "%u than the effective_since %u from sequencer "
                         "metadata when recovery started.",
                         log_id_.val_,
                         metadata->h.effective_since.val_,
                         seq_since.val_);
    } else if (metadata->h.effective_since == seq_since) {
      // this is the epoch metadata that sequencer got for next_epoch_ and
      // starts this log recovery request. Whether this metadata was
      // successfully written to metadata log is indicated in the
      // corresponding flag.
      metadata_written = metadata->writtenInMetaDataLog();
    } else {
      // metadata->h.effective_since > seq_since. The epoch metadata in epoch
      // store has changed since sequencer started this log recovery request.
      // A very important assumption here is that LogDevice and/or its
      // administriative tools _NEVER_ writes a new epoch metadata to epoch
      // store _unless_ the previous metadata record is fully replicated in
      // metadata logs. Therefore, we assume that the epoch metadata record
      // that sequencer got on activate next_epoch_ must already been fully
      // stored in the metadata log.
      metadata_written = true;
    }
  }

  if (!metadata_written) {
    // start timer for retrying
    seq_metadata_timer_->activate();
    return;
  }

  ld_spew("Successfully read current epoch metadata in epoch store and "
          "confirmed it is stored in metadata log for log %lu.",
          log_id_.val_);

  seq_metadata_read_ = true;
  WORKER_STAT_DECR(num_recovery_reading_sequencer_metadata);

  // inform the sequencer
  informSequencerOfMetaDataRead(seq_metadata_->h.effective_since);

  // cancel and destroy the retry timer
  seq_metadata_timer_.reset();
  if (all_epochs_recovered_) {
    complete(E::OK);
  }
}

void LogRecoveryRequest::informSequencerOfMetaDataRead(epoch_t effective_since,
                                                       Worker* worker) {
  if (worker == nullptr) {
    worker = Worker::onThisThread();
  }
  auto sequencer = worker->processor_->allSequencers().findSequencer(log_id_);
  ld_check(sequencer != nullptr);
  sequencer->onSequencerMetaDataRead(effective_since);
}

// send SEALs to the recovery nodeset, which is the union of all nodesets
// for each recovering epoch
void LogRecoveryRequest::sealLog() {
  // must finished last step of getting epoch metadata
  ld_check(epoch_metadata_finalized_);
  ld_check(last_clean_epoch_.hasValue());
  const epoch_t last_clean_epoch = last_clean_epoch_.value();
  ld_check(last_clean_epoch.val_ < next_epoch_.val_ - 1);

  auto config = Worker::onThisThread()->getConfig();
  auto log = config->getLogGroupByIDShared(log_id_);
  if (!log) {
    // config has changed since the time this Sequencer was activated
    // log_id_ is no longer there.
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    100,
                    "Log %lu is no longer in cluster config. "
                    "Skipping recovery.",
                    log_id_.val_);
    complete(E::NOTINCONFIG);
    return;
  }

  seal_header_ = std::make_unique<SEAL_Header>();
  seal_header_->rqid = id_;
  seal_header_->log_id = log_id_;
  seal_header_->seal_epoch = getSealEpoch();
  seal_header_->shard = -1; // Will be populated by sendSeal().
  seal_header_->sealed_by = Worker::onThisThread()->processor_->getMyNodeID();
  seal_header_->last_clean_epoch = last_clean_epoch;
  ld_check(node_statuses_.empty());

  const auto& nodes_configuration =
      Worker::onThisThread()->getNodesConfiguration();
  const auto& storage_membership = nodes_configuration->getStorageMembership();

  // send SEALs to nodes that belong to the union of all nodesets of
  // recovering epochs and are still present in cluster config
  for (const ShardID& s : recovery_nodes_) {
    if (storage_membership->shouldReadFromShard(s)) {
      auto insert_result =
          node_statuses_.emplace(std::piecewise_construct,
                                 std::forward_as_tuple(s),
                                 std::forward_as_tuple(this, s));
      ld_check(insert_result.second);
    }
  }

  nodeset_size_ = node_statuses_.size();
  ld_check(nodeset_size_ > 0);

  // attempt to seal all the nodes in the recovery set
  seal_retry_handler_.reset(new RetryHandler(
      [this](ShardID shard) {
        ld_check(seal_header_ != nullptr);
        auto it = node_statuses_.find(shard);
        if (it == node_statuses_.end()) {
          // currently we don't remove nodes from the recovery set
          // (node_statuses_), but may need to do that in the future to
          // support cluster shrinking.
          // TODO 4408213: support cluster shrinking in LogRecoveryRequest
          ld_check(false);
          // stop retrying on nodes does not belong to the recovery set
          return 0;
        }

        NodeStatus& node_status = it->second;
        if (node_status.state == NodeStatus::State::SEALED) {
          // shard is already sealed, consider the timer callback stale.
          // do not retry further
          return 0;
        }
        return this->sendSeal(shard);
      },
      SEAL_RETRY_INITIAL_DELAY,
      SEAL_RETRY_MAX_DELAY));

  for (const auto& kv : node_statuses_) {
    seal_retry_handler_->execute(kv.first);
  }

  // also, start a periodical timer to ensure that all nodes are eventually
  // sealed
  activateCheckSealTimer();

  if (last_clean_epoch.val_ + 1 == next_epoch_.val_ - 1) {
    ld_info("Starting recovery of epoch %u of log %lu",
            last_clean_epoch.val_ + 1,
            log_id_.val_);
  } else {
    ld_info("Starting recovery of epochs [%u, %u] of log %lu",
            last_clean_epoch.val_ + 1,
            next_epoch_.val_ - 1,
            log_id_.val_);
  }

  applyShardStatus();

  ld_check(epoch_recovery_machines_.size() > 0);

  ld_check(tail_record_.hasValue());
  epoch_recovery_machines_.begin()->activate(tail_record_.value());
}

void LogRecoveryRequest::checkNodesForSeal() {
  ld_check(seal_header_ != nullptr);
  ld_check(seal_retry_handler_ != nullptr);

  const auto& nodes_configuration =
      Worker::onThisThread()->getNodesConfiguration();
  const auto& storage_membership = nodes_configuration->getStorageMembership();

  for (const auto& kv : node_statuses_) {
    if (kv.second.state == NodeStatus::State::SEALED) {
      continue;
    }
    ShardID shard = kv.first;
    if (!storage_membership->shouldReadFromShard(shard)) {
      continue;
    }
    seal_retry_handler_->activateTimer(shard);
  }
}

void LogRecoveryRequest::activateCheckSealTimer() {
  if (check_seal_timer_ == nullptr) {
    check_seal_timer_ = std::make_unique<Timer>([this] {
      checkNodesForSeal();
      // run the job periodically throughout the life time of this
      // LogRecoveryRequest
      activateCheckSealTimer();
    });
  }
  check_seal_timer_->activate(SEAL_CHECK_INTERVAL);
}

int LogRecoveryRequest::sendSeal(ShardID shard) {
  ld_check(seal_header_ != nullptr);
  auto it = node_statuses_.find(shard);
  if (it == node_statuses_.end()) {
    ld_critical("Sending SEAL message to a shard (%s) whose state "
                "is not populated",
                shard.toString().c_str());
    ld_check(false);
    return -1;
  }

  NodeStatus& node_status = it->second;

  SocketClosedCallback& socket_cb = node_status.socket_close_callback;

  // make sure socket close callback is not already added to a socket
  socket_cb.deactivate();

  node_status.last_seal_time = std::chrono::steady_clock::now();

  SEAL_Header header = *seal_header_;
  header.shard = shard.shard();

  auto msg = std::make_unique<SEAL_Message>(header);
  return Worker::onThisThread()->sender().sendMessage(
      std::move(msg), NodeID(shard.node()), &socket_cb);
}

void LogRecoveryRequest::onSealReply(ShardID from,
                                     const SEALED_Message& reply) {
  if (all_epochs_recovered_) {
    // Ignore this reply, we finished recovery without this node.
    return;
  }

  ld_check(reply.header_.log_id == log_id_);

  if (!seal_header_ || reply.header_.seal_epoch != seal_header_->seal_epoch) {
    // Since LogRecoveryRequests are indexed by log_id, if another recovery
    // request is started (for a higher epoch) before the previous one
    // completed, we might receive stale SEALED replies. It's safe to ignore
    // them.
    if (seal_header_ && reply.header_.seal_epoch > seal_header_->seal_epoch) {
      ld_error("PROTOCOL ERROR: Received a SEALED message for log %lu from %s "
               "with a higher epoch number %u (expected <= %u)",
               log_id_.val_,
               from.toString().c_str(),
               reply.header_.seal_epoch.val_,
               seal_header_->seal_epoch.val_);
      return;
    }

    ld_debug("Ignoring SEALED message for log %lu from %s for an old epoch %u",
             log_id_.val_,
             from.toString().c_str(),
             reply.header_.seal_epoch.val_);
    return;
  }

  auto it = node_statuses_.find(from);
  if (it == node_statuses_.end()) {
    // received a sealed reply from a node not in recovery set
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "Received a SEALED message for log %lu from an "
                    "unexpected node %s",
                    log_id_.val_,
                    from.toString().c_str());
    return;
  }

  NodeStatus& node_status = it->second;
  if (node_status.state != NodeStatus::State::NOT_SEALED) {
    RATELIMIT_WARNING(std::chrono::seconds(1),
                      10,
                      "Received a duplicate SEALED message for log %lu from "
                      "node %s with status %s.",
                      log_id_.val_,
                      from.toString().c_str(),
                      error_description(reply.header_.status));
    return;
  }

  ld_check(seal_header_ != nullptr);

  if (reply.header_.status == E::PREEMPTED) {
    // some other sequencer is in the process or has finished sealing; fail
    // immediately
    ld_check(reply.seal_.valid());
    onPreempted(reply.seal_.epoch, reply.seal_.seq_node);

    complete(E::PREEMPTED);
    return;
  }

  ld_check(epoch_metadata_finalized_);

  if (reply.header_.status == E::REBUILDING &&
      node_status.authoritative_status ==
          AuthoritativeStatus::FULLY_AUTHORITATIVE) {
    // We are probably lagging behind reading the event log. Hopefully we can
    // complete recovery without this node. Worst case, recovery will stall
    // until we catch up reading the event log.
    RATELIMIT_WARNING(
        std::chrono::seconds(1),
        10,
        "Node %s sent a SEALED message for epoch %u of log %lu with "
        "E::REBUILDING but event log says the node is FULLY_AUTHORITATIVE. "
        "Ignoring this reply. We will keep retrying until what the node says "
        "matches what the event log says.",
        from.toString().c_str(),
        reply.header_.seal_epoch.val_,
        log_id_.val_);
    seal_retry_handler_->activateTimer(from);
    return;
  }

  if (reply.header_.status == E::OK &&
      node_status.authoritative_status !=
          AuthoritativeStatus::FULLY_AUTHORITATIVE) {
    RATELIMIT_WARNING(
        std::chrono::seconds(1),
        10,
        "Node %s sent a SEALED message for epoch %u of log %lu with status == "
        "E::OK but event log says the node is %s. "
        "This epoch recovery state machine is now considering this node "
        "FULLY_AUTHORITATIVE.",
        from.toString().c_str(),
        reply.header_.seal_epoch.val_,
        log_id_.val_,
        toString(node_status.authoritative_status).c_str());
    // This is a unperfect workaround for a race where a node completes
    // rebuilding and starts accepting new writes but recovery is still behind
    // reading the event log and decides to make progress without it.
    // Here we decide to consider the node fully authoritative to redice the
    // chances of us missing records.
    setNodeAuthoritativeStatus(from,
                               AuthoritativeStatus::FULLY_AUTHORITATIVE,
                               AuthoritativeSource::NODE);
  }

  if (reply.header_.status == E::OK) {
    // In case the sequencer metadata is already found in metadata log,
    // as soon as the last epoch recovery machine is retired LogRecoveryRequest
    // is destroyed. Therefore, if we are in a LogRecoveryRequest method,
    // we must have at least one active epoch recovery machine.
    ld_check(!seq_metadata_read_ || epoch_recovery_machines_.size() > 0);

    ld_check(next_epoch_.val_ > 0);
    ld_check(seal_header_->last_clean_epoch.val_ < next_epoch_.val_ - 1);

    // asserts above guarantee that n_recovering_epochs did not wrap-around
    const size_t n_recovering_epochs =
        next_epoch_.val_ - 1 - seal_header_->last_clean_epoch.val_;

    if (reply.epoch_lng_.size() != n_recovering_epochs) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      10,
                      "A SEALED message for epoch %u of log %lu received from "
                      "node %s has an incorrect number of LNG values (%zu). "
                      "Expected %zu values.",
                      reply.header_.seal_epoch.val_,
                      log_id_.val_,
                      from.toString().c_str(),
                      reply.epoch_lng_.size(),
                      n_recovering_epochs);
      complete(E::BADMSG);
      return;
    }

    node_status.state = NodeStatus::State::SEALED;

    if (node_status.last_seal_time !=
        std::chrono::steady_clock::time_point::min()) {
      // for metadata logs, we may get stale SEALED replies for the same epoch
      // since their LogRecoveryRequest can be restarted, in such case
      // last_seal_time can be unset.
      // TODO 10120529: include rqid in SEALED replies
      HISTOGRAM_ADD(Worker::stats(),
                    log_recovery_seal_node,
                    usec_since(node_status.last_seal_time));
    }

    // Inform EpochRecovery machines that are still running that we got
    // SEALED from this node, and what the LNG is for each of the epochs.
    //
    // Be careful to correctly match the LNGs in reply.epoch_lng_[] vector
    // to machines as the size of epoch_recovery_machines_ vector may be
    // smaller than the size of reply.epoch_lng_ by now because of some
    // epochs having been fully recovered and their machines retired.

    ld_check(epoch_recovery_machines_.size() <= n_recovering_epochs);
    // the following are guaranteed by the deserializaton method for SEALED
    ld_check(reply.epoch_lng_.size() == n_recovering_epochs);
    ld_check(reply.epoch_offset_map_.size() == reply.epoch_lng_.size());
    ld_check(reply.last_timestamp_.size() == reply.epoch_lng_.size());
    ld_check(reply.max_seen_lsn_.size() == n_recovering_epochs);

    updateTrimPoint(reply.header_.status, reply.header_.trim_point);

    // if proto < Compatibility::TAIL_RECORD_IN_SEALED,
    // reply.header_.num_tail_records will be -1
    const bool support_tail_records = (reply.header_.num_tail_records >= 0);
    auto tr_it = reply.tail_records_.begin();

    std::list<EpochRecovery>::iterator erm = epoch_recovery_machines_.begin();

    for (int i = n_recovering_epochs - epoch_recovery_machines_.size();
         i < reply.epoch_lng_.size();
         i++, erm++) {
      const auto& offsets = reply.epoch_offset_map_[i];
      ld_check(erm != epoch_recovery_machines_.end());
      if (erm->epoch_ != lsn_to_epoch(reply.epoch_lng_[i])) {
        RATELIMIT_ERROR(std::chrono::seconds(1),
                        10,
                        "A SEALED message for epoch %u of log %lu received from"
                        " node %s has an invalid epoch_lng_ list: LSN %s at"
                        " offsets %i has epoch number %u. Expected %u.",
                        seal_header_->seal_epoch.val_,
                        log_id_.val_,
                        from.toString().c_str(),
                        lsn_to_string(reply.epoch_lng_[i]).c_str(),
                        i,
                        lsn_to_epoch(reply.epoch_lng_[i]).val_,
                        erm->epoch_.val_);
        complete(E::BADMSG);
        return;
      }

      if (reply.max_seen_lsn_[i] != LSN_INVALID &&
          erm->epoch_ != lsn_to_epoch(reply.max_seen_lsn_[i])) {
        RATELIMIT_ERROR(std::chrono::seconds(1),
                        10,
                        "A SEALED message for epoch %u of log %lu received from"
                        " node %s has an invalid max_seen_lsn_ list: LSN %s at"
                        " offsets %i expected to be in epoch %u.",
                        seal_header_->seal_epoch.val_,
                        log_id_.val_,
                        from.toString().c_str(),
                        lsn_to_string(reply.max_seen_lsn_[i]).c_str(),
                        i,
                        erm->epoch_.val_);
        complete(E::BADMSG);
        return;
      }

      folly::Optional<TailRecord> epoch_tail;
      if (support_tail_records) {
        epoch_t find_epoch = erm->epoch_;
        tr_it = std::find_if(tr_it,
                             reply.tail_records_.end(),
                             [find_epoch](const TailRecord& tr) {
                               return lsn_to_epoch(tr.header.lsn) == find_epoch;
                             });
        if (tr_it != reply.tail_records_.end()) {
          epoch_tail = *tr_it;
          if (!epoch_tail.value().isValid() ||
              !epoch_tail.value().containOffsetWithinEpoch()) {
            RATELIMIT_ERROR(std::chrono::seconds(10),
                            10,
                            "A SEALED message for epoch %u of log %lu received"
                            " from node %s has an invalid TailRecord at "
                            " epoch %u. record header flags: %u.",
                            seal_header_->seal_epoch.val_,
                            log_id_.val_,
                            from.toString().c_str(),
                            erm->epoch_.val_,
                            epoch_tail.value().header.flags);
            complete(E::BADMSG);
            return;
          }
          ++tr_it;
        }
      } else {
        // the sealed node does not support sending TailRecord yet, compose a
        // tail record from its (lng, last_timestamp, epoch_offset_map).
        // Note: only consider it as a tail if lng > ESN_INVALID
        if (lsn_to_esn(reply.epoch_lng_[i]) > ESN_INVALID) {
          epoch_tail = TailRecord(
              {log_id_,
               reply.epoch_lng_[i],
               reply.last_timestamp_[i],
               {BYTE_OFFSET_INVALID /* deprecated, OffsetMap below */},
               TailRecordHeader::OFFSET_WITHIN_EPOCH,
               {}},
              offsets,
              std::shared_ptr<PayloadHolder>());

          ld_check(epoch_tail.value().isValid());
        }
      }

      esn_t max_seen_esn =
          (reply.max_seen_lsn_[i] == LSN_INVALID ? ESN_MAX
                                                 : // protocol not supported
               lsn_to_esn(reply.max_seen_lsn_[i]));

      if (erm->onSealed(from,
                        lsn_to_esn(reply.epoch_lng_[i]),
                        max_seen_esn,
                        offsets,
                        std::move(epoch_tail))) {
        return;
      }
    }

    ld_check(erm == epoch_recovery_machines_.end());
  } else {
    const bool severe = reply.header_.status != E::REBUILDING;
    if (severe) {
      RATELIMIT_INFO(std::chrono::seconds(10),
                     2,
                     "Failed to SEAL log %lu, epoch %u on node %s: %s",
                     log_id_.val_,
                     seal_header_->seal_epoch.val_,
                     from.toString().c_str(),
                     error_description(reply.header_.status));
    } else {
      ld_debug("Failed to SEAL log %lu, epoch %u on node %s: %s",
               log_id_.val_,
               seal_header_->seal_epoch.val_,
               from.toString().c_str(),
               error_description(reply.header_.status));
    }

    // keep trying
    seal_retry_handler_->activateTimer(from);
  }
}

void LogRecoveryRequest::onSealMessageSent(ShardID to,
                                           epoch_t seal_epoch,
                                           Status status) {
  if (!seal_header_ || seal_epoch < seal_header_->seal_epoch) {
    // see onSealReply()
    return;
  }

  ld_check(seal_epoch == seal_header_->seal_epoch);

  auto it = node_statuses_.find(to);
  if (it == node_statuses_.end()) {
    RATELIMIT_WARNING(std::chrono::seconds(1),
                      10,
                      "SEAL message for log %lu with epoch %u sent to node "
                      "%s which is not in the recovery nodeset; status: %s",
                      log_id_.val_,
                      seal_epoch.val_,
                      to.toString().c_str(),
                      error_description(status));
    return;
  }

  if (status != E::OK && it->second.state != NodeStatus::State::SEALED) {
    seal_retry_handler_->activateTimer(to);
  }
}

void LogRecoveryRequest::allEpochsRecovered() {
  all_epochs_recovered_ = true;
  if (seq_metadata_read_) {
    // conclude recovery when sequencer metadata has already been read from
    // the metadata log. Otherwise, wait for the record to appear.
    completeSoon(E::OK);
  }
}

void LogRecoveryRequest::completeSoon(Status status) {
  epoch_recovery_machines_.clear();
  deferredCompleteTimer_ =
      std::make_unique<Timer>([this, status] { complete(status); });
  deferredCompleteTimer_->activate(std::chrono::milliseconds(0));
}

void LogRecoveryRequest::updateTrimPoint(Status status, lsn_t trim_point) {
  Worker* worker = Worker::onThisThread();
  std::shared_ptr<Sequencer> sequencer =
      worker->processor_->allSequencers().findSequencer(log_id_);

  if (sequencer != nullptr &&
      sequencer->getState() == Sequencer::State::ACTIVE) {
    sequencer->updateTrimPoint(status, trim_point);
  }
}

void LogRecoveryRequest::complete(Status status) {
  Worker* worker = Worker::onThisThread();
  std::shared_ptr<Sequencer> sequencer =
      worker->processor_->allSequencers().findSequencer(log_id_);
  // for metasequencer, sequencer may not be valid here
  ld_check(sequencer != nullptr || MetaDataLog::isMetaDataLog(log_id_));

  if (status == Status::OK) {
    ld_check(seq_metadata_read_ && all_epochs_recovered_);
    ld_info("Log recovery completed for log %lu, next_epoch:%u",
            log_id_.val_,
            next_epoch_.val_);
  } else {
    ld_info("Log recovery for log %lu failed with error code %s, next_epoch:%u",
            log_id_.val_,
            error_name(status),
            next_epoch_.val_);
  }

  if (!seq_metadata_read_) {
    ld_check(status != Status::OK);
    // although recovery failed, we still need to decrease the current
    // number of recovery requests that are reading sequencer metadata
    WORKER_STAT_DECR(num_recovery_reading_sequencer_metadata);
  }

  if (sequencer) {
    if (!tail_record_.hasValue()) {
      ld_check(status != E::OK);
      tail_record_.assign(TailRecord());
    }

    sequencer->onRecoveryCompleted(
        status, next_epoch_, tail_record_.value(), std::move(recovered_lsns_));
  } else {
    RATELIMIT_WARNING(std::chrono::seconds(1),
                      10,
                      "Recovery finished for log %lu but its sequencer "
                      "no longer exists.",
                      log_id_.val_);
    WORKER_STAT_INCR(recovery_completed);
  }

  // let the next LogRecoveryRequest from the queue run
  worker->popRecoveryRequest();

  auto& rqmap = worker->runningLogRecoveries().map;
  auto it = rqmap.find(log_id_);
  ld_check(it != rqmap.end() && it->second.get() == this);

  rqmap.erase(it);
  // `this' is no longer valid here
}

void LogRecoveryRequest::onPreempted(epoch_t epoch, NodeID preempted_by) {
  std::shared_ptr<Sequencer> sequencer =
      Worker::onThisThread()->processor_->allSequencers().findSequencer(
          log_id_);

  // sequencer may get destroyed at this time.
  if (sequencer) {
    sequencer->notePreempted(epoch, preempted_by);
  }
}

void LogRecoveryRequest::onShardRemovedFromConfig(ShardID shard) {
  // TODO(T4408213): Handle cluster shrinking.
  RATELIMIT_ERROR(
      std::chrono::seconds(10),
      1,
      "Shard %s is on a node that was removed from the config or became "
      "non-storage node during recovery of log %lu. Recovery may stall.",
      shard.toString().c_str(),
      log_id_.val_);
}

void LogRecoveryRequest::onConnectionClosed(ShardID shard, Status status) {
  auto it = node_statuses_.find(shard);
  ld_check(it != node_statuses_.end());
  NodeStatus& node_status = it->second;

  // Re-register the callback.
  SocketClosedCallback& cb = node_status.socket_close_callback;
  const int rv = Worker::onThisThread()->sender().registerOnSocketClosed(
      Address(NodeID(shard.node())), cb);
  if (rv != 0) {
    ld_check(err == E::NOTFOUND);
    // TODO (#4408213): handle cluster shrinking)
    return;
  }

  if (node_status.authoritative_status_source == AuthoritativeSource::NODE) {
    // If the authorititave status of the shard was defined by its behavior (it
    // successfully SEALED while the event log says it is not in state
    // AuthoritativeStatus::FULLY_AUTHORITATIVE), then we guarantee that its
    // behavior overrides the state defined in the event log for as long as the
    // socket to it remains connected. We just lost connection to this shard so
    // now it is time to re-apply the authoritative status we read from the
    // event log.
    node_status.authoritative_status_source = AuthoritativeSource::EVENT_LOG;
    applyShardStatus();
  }

  if (status == E::NOTINCONFIG) {
    // We were disconnected from a node because of the config change.
    const auto& storage_membership =
        Worker::onThisThread()->getNodesConfiguration()->getStorageMembership();
    if (!storage_membership->shouldReadFromShard(shard)) {
      return;
    }
  }

  // Ensure a new SEAL is eventually sent to the shard.
  if (node_status.state == NodeStatus::State::NOT_SEALED) {
    seal_retry_handler_->activateTimer(shard, /*reset=*/true);
  }
}

void LogRecoveryRequest::onEpochRecovered(epoch_t epoch,
                                          TailRecord epoch_tail,
                                          Status status,
                                          Seal seal) {
  ld_check(epoch_recovery_machines_.begin() != epoch_recovery_machines_.end());
  ld_check(epoch_recovery_machines_.begin()->epoch_ == epoch);

  ld_debug("Recovery complete for epoch %u of log %lu with status %s",
           epoch.val_,
           log_id_.val_,
           error_description(status));

  if (status == E::PREEMPTED) {
    if (seal.valid()) {
      onPreempted(seal.epoch, seal.seq_node);
    }
    all_epochs_recovered_ = true;
    completeSoon(status);
    return;
  }

  ld_check(status == E::OK);
  ld_check(epoch_tail.isValid());
  WORKER_STAT_INCR(epoch_recovery_success);

  tail_record_.assign(std::move(epoch_tail));

  epoch_recovery_machines_.pop_front();

  if (epoch_recovery_machines_.begin() != epoch_recovery_machines_.end()) {
    ld_check(tail_record_.hasValue());
    epoch_recovery_machines_.begin()->activate(tail_record_.value());
  } else {
    // finished recoverying all epochs in [lce+1, next_epoch-1]
    allEpochsRecovered();
    return;
  }
}

void LogRecoveryRequest::overrideShardStatus(ShardID shard,
                                             AuthoritativeStatus status) {
  auto it = node_statuses_.find(shard);
  if (it == node_statuses_.end()) {
    return;
  }

  NodeStatus& node_status = it->second;
  if (node_status.authoritative_status == status) {
    // Node is already using this status.
    return;
  }

  setNodeAuthoritativeStatus(shard, status, AuthoritativeSource::OVERRIDE);
}

void LogRecoveryRequest::applyShardStatus() {
  Worker* w = Worker::onThisThread();

  const auto& shard_status =
      w->shardStatusManager().getShardAuthoritativeStatusMap();
  if (shard_status.getVersion() == LSN_INVALID) {
    return;
  }

  for (auto& it : node_statuses_) {
    if (it.second.authoritative_status_source == AuthoritativeSource::NODE) {
      // The node knows better of its authoritative status.
      continue;
    }

    ShardID shard = it.first;
    const auto st =
        w->shardStatusManager().getShardAuthoritativeStatusMap().getShardStatus(
            shard.node(), shard.shard());
    setNodeAuthoritativeStatus(shard, st, AuthoritativeSource::EVENT_LOG);
  }
}

void LogRecoveryRequest::setNodeAuthoritativeStatus(ShardID shard,
                                                    AuthoritativeStatus st,
                                                    AuthoritativeSource src) {
  // Recovery should not be stalled because some nodes are rebuilding but there
  // is a chance to recover their data. Consider them UNDERREPLICATION.
  if (st == AuthoritativeStatus::UNAVAILABLE) {
    st = AuthoritativeStatus::UNDERREPLICATION;
  }

  auto it = node_statuses_.find(shard);
  ld_check(it != node_statuses_.end());
  NodeStatus& node_status = it->second;
  auto current_auth_status = node_status.authoritative_status;
  if (st == current_auth_status) {
    return;
  }

  if (st == AuthoritativeStatus::UNDERREPLICATION &&
      src == AuthoritativeSource::NODE) {
    RATELIMIT_WARNING(std::chrono::seconds(1),
                      1,
                      "Node %s reported that it is %s but the event log says "
                      "the node is %s for recovery of log %lu. We are probably "
                      "not caught up reading it.",
                      shard.toString().c_str(),
                      toString(st).c_str(),
                      toString(current_auth_status).c_str(),
                      log_id_.val_);
  }

  node_status.authoritative_status = st;
  node_status.authoritative_status_source = src;

  // If needed, change the state of the node back to NOT_SEALED. Regardless of
  // the new authoritative status of the node, we are going to re-schedule
  // trying to seal it.
  if (node_status.state != NodeStatus::State::NOT_SEALED) {
    node_status.state = NodeStatus::State::NOT_SEALED;
    node_status.last_seal_time = std::chrono::steady_clock::time_point::min();
    seal_retry_handler_->activateTimer(shard, /*reset=*/true);
  }

  auto itr = epoch_recovery_machines_.begin();
  while (itr != epoch_recovery_machines_.end()) {
    // setNodeAuthoritativeStatus() might cause erm to be destroyed.
    auto& erm = *itr;
    ++itr;
    erm.setNodeAuthoritativeStatus(shard, st);
  }
}

void LogRecoveryRequest::SocketClosedCallback::
operator()(Status st, const Address& address) {
  ld_check(!address.isClientAddress());
  request_->onConnectionClosed(shard_, st);
}

void LogRecoveryRequest::getDebugInfo(InfoRecoveriesTable& table) const {
  auto started =
      creation_timestamp_.approximateSystemTimestamp().toMilliseconds();

  if (!last_clean_epoch_.hasValue()) {
    // We still haven't fetched lce from the epoch store.
    table.next()
        .set<0>(log_id_)
        .set<1>(next_epoch_.val_ - 1)
        .set<2>("FETCHING_LCE");
    table.set<14>(started);
    return;
  }

  for (const auto& r : epoch_recovery_machines_) {
    r.getDebugInfo(table);
  }

  if (!epoch_metadata_finalized_) {
    // There are still EpochRecovery machines yet to be created because we have
    // not fetched the metadata for them.
    const epoch_t::raw_type start_epoch = epoch_recovery_machines_.empty()
        ? last_clean_epoch_.value().val_ + 1
        : epoch_recovery_machines_.back().epoch_.val_ + 1;

    for (epoch_t::raw_type epoch = start_epoch; epoch < next_epoch_.val_;
         ++epoch) {
      table.next().set<0>(log_id_).set<1>(epoch).set<2>("READING_METADATA_LOG");
      table.set<14>(started);
    }
  }

  if (!seq_metadata_read_) {
    table.next()
        .set<0>(log_id_)
        // Note: the `epoch' column here does not mean the log is recovering
        // this epoch, but rather indicates that log recovery is waiting for
        // epoch metadata of `epoch' to appear in the metadata log.
        .set<1>(next_epoch_.val_)
        .set<2>("READING_SEQUENCER_METADATA");
    table.set<14>(started);
  }
}

SteadyTimestamp LogRecoveryRequest::getCreationTimestamp() {
  return creation_timestamp_;
}

}} // namespace facebook::logdevice
