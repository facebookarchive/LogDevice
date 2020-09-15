/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/storage/PurgeCoordinator.h"

#include "logdevice/common/Metadata.h"
#include "logdevice/common/RecordID.h"
#include "logdevice/common/RequestType.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/LogsConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/CLEANED_Message.h"
#include "logdevice/common/protocol/CLEAN_Message.h"
#include "logdevice/common/protocol/RELEASE_Message.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/CleanedResponseRequest.h"
#include "logdevice/server/EpochRecordCache.h"
#include "logdevice/server/RecordCache.h"
#include "logdevice/server/ReleaseRequest.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/locallogstore/WriteOps.h"
#include "logdevice/server/read_path/LogStorageState.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/storage/PurgeScheduler.h"
#include "logdevice/server/storage/PurgeUncleanEpochs.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"
#include "logdevice/server/storage_tasks/WriteStorageTask.h"

namespace facebook { namespace logdevice {

namespace {

void broadcastReleaseRequest(LogStorageState* parent,
                             const RecordID& rid,
                             shard_index_t shard,
                             bool force = false) {
  ReleaseRequest::broadcastReleaseRequest(
      ServerWorker::onThisThread()->processor_,
      rid,
      shard,
      [parent](worker_id_t idx) { return parent->isWorkerSubscribed(idx); },
      force);
}

} // namespace

PurgeCoordinator::PurgeCoordinator(logid_t log_id,
                                   shard_index_t shard,
                                   LogStorageState* parent)
    : log_id_(log_id), shard_(shard), parent_(parent) {}

PurgeCoordinator::~PurgeCoordinator() {
  // There shouldn't be an active purge.  If we're getting destroyed but there
  // is one, chances are we're not on the proper Worker that the purge is
  // running on.  Worker should have already called this->shutdown().
  ld_check(!active_purge_);
}

Message::Disposition PurgeCoordinator::onReceived(CLEAN_Message* msg,
                                                  const Address& from) {
  ld_debug("CLEAN message from %s: log %lu, epoch %u, recovery_id %lu",
           Sender::describeConnection(from).c_str(),
           msg->header_.log_id.val_,
           msg->header_.epoch.val_,
           msg->header_.recovery_id.val_);

  ServerWorker* w = ServerWorker::onThisThread();

  if (w->sender().isClosed(from)) {
    RATELIMIT_INFO(std::chrono::seconds(1),
                   1,
                   "CLEAN message from disconnected client %s log %lu",
                   Sender::describeConnection(from).c_str(),
                   msg->header_.log_id.val_);
    return Message::Disposition::NORMAL;
  }

  if (msg->header_.last_known_good > msg->header_.last_digest_esn) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "Got a CLEAN message with last_known_good %u larger than "
                    "last_digest_esn %u for log %lu.",
                    msg->header_.last_known_good.val_,
                    msg->header_.last_digest_esn.val_,
                    msg->header_.log_id.val_);
    err = E::BADMSG;
    return Message::Disposition::ERROR;
  }

  if (!epoch_valid(msg->header_.epoch)) {
    RATELIMIT_CRITICAL(std::chrono::seconds(10),
                       10,
                       "Got CLEAN message from %s with invalid epoch %u "
                       "for log %lu!",
                       Sender::describeConnection(from).c_str(),
                       msg->header_.epoch.val_,
                       msg->header_.log_id.val_);
    err = E::BADMSG;
    return Message::Disposition::ERROR;
  }

  ServerProcessor* const processor = w->processor_;

  const shard_size_t n_shards = w->getNodesConfiguration()->getNumShards();
  shard_index_t shard = msg->header_.shard;
  if (shard >= n_shards) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Got CLEAN message from %s with invalid shard %u, "
                    "this node only has %u shards for log %lu",
                    Sender::describeConnection(from).c_str(),
                    shard,
                    n_shards,
                    msg->header_.log_id.val_);
    return Message::Disposition::NORMAL;
  }

  // Ignore the message during shutdown.
  if (!w->isAcceptingWork()) {
    return Message::Disposition::NORMAL;
  }

  if (!w->processor_->runningOnStorageNode()) {
    RATELIMIT_ERROR(
        std::chrono::seconds(10),
        10,
        "got CLEAN message for log %lu from %s but not a storage node",
        msg->header_.log_id.val_,
        Sender::describeConnection(from).c_str());
    return Message::Disposition::NORMAL;
  }

  // We cannot proceed reading this log unless it's an internal log!
  if (!w->getLogsConfig()->isFullyLoaded() &&
      !w->getLogsConfig()->isInternalLogID(msg->header_.log_id) &&
      !MetaDataLog::isMetaDataLog(msg->header_.log_id)) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "got CLEAN message for log %lu from %s but config is not "
                    "fully loaded yet",
                    msg->header_.log_id.val_,
                    Sender::describeConnection(from).c_str());
    err = E::AGAIN;
    return Message::Disposition::NORMAL;
  }

  if (processor->isDataMissingFromShard(shard)) {
    RATELIMIT_ERROR(
        std::chrono::seconds(1),
        10,
        "Got CLEAN for log %lu for an empty shard waiting for rebuilding; "
        "something's wrong; ignoring",
        msg->header_.log_id.val_);
    return Message::Disposition::NORMAL;
  }

  LogStorageState* log_state = processor->getLogStorageStateMap().insertOrGet(
      msg->header_.log_id, shard);
  if (log_state == nullptr) {
    // LogStorageStateMap is at capacity.  Try to send a reply with status
    // E::FAILED.
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "Error processing CLEAN (log %lu, epoch %u): "
                    "LogStorageStateMap is full",
                    msg->header_.log_id.val_,
                    msg->header_.epoch.val_);
    CLEANED_Header hdr{msg->header_.log_id,
                       msg->header_.epoch,
                       msg->header_.recovery_id,
                       E::FAILED,
                       Seal(),
                       shard};
    w->sender().sendMessage(std::make_unique<CLEANED_Message>(hdr), from);
    return Message::Disposition::NORMAL;
  }

  auto peer_idx = w->sender().getNodeIdx(from);
  auto peer_nid = peer_idx ? NodeID(*peer_idx) : NodeID();
  checked_downcast<PurgeCoordinator&>(*log_state->purge_coordinator_)
      .onCleanMessage(
          std::unique_ptr<CLEAN_Message>(msg), peer_nid, from, w->idx_);

  // ownership transferred to PurgeCoordinator
  return Message::Disposition::KEEP;
}

Message::Disposition PurgeCoordinator::onReceived(RELEASE_Message* msg,
                                                  const Address& from) {
  ServerWorker* w = ServerWorker::onThisThread();

  const RELEASE_Header& header = msg->getHeader();

  const shard_size_t n_shards = w->getNodesConfiguration()->getNumShards();
  shard_index_t shard = header.shard;
  ld_check(shard != -1);

  if (shard >= n_shards) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Got RELEASE message for log %lu from client %s with "
                    "invalid shard %u, this node only has %u shards",
                    header.rid.logid.val_,
                    Sender::describeConnection(from).c_str(),
                    shard,
                    n_shards);
    return Message::Disposition::NORMAL;
  }

  if (!epoch_valid(header.rid.epoch)) {
    RATELIMIT_CRITICAL(std::chrono::seconds(10),
                       10,
                       "Got RELEASE message from %s with invalid epoch %u "
                       "for log %lu!",
                       Sender::describeConnection(from).c_str(),
                       header.rid.epoch.val_,
                       header.rid.logid.val_);
    err = E::BADMSG;
    return Message::Disposition::ERROR;
  }

  // Ignore the message during shutdown.
  if (!w->isAcceptingWork()) {
    return Message::Disposition::NORMAL;
  }

  ServerProcessor* const processor = w->processor_;
  if (!processor->runningOnStorageNode()) {
    RATELIMIT_ERROR(
        std::chrono::seconds(1),
        10,
        "Got RELEASE %s from %s but not configured as a storage node",
        header.rid.toString().c_str(),
        Sender::describeConnection(from).c_str());
    err = E::NOTSTORAGE;
    return Message::Disposition::NORMAL;
  }

  // We cannot proceed reading this log unless it's an internal log!
  if (!w->getLogsConfig()->isFullyLoaded() &&
      !w->getLogsConfig()->isInternalLogID(header.rid.logid) &&
      !MetaDataLog::isMetaDataLog(header.rid.logid)) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "got RELEASE %s from %s but config is not fully loaded yet",
                    header.rid.toString().c_str(),
                    Sender::describeConnection(from).c_str());
    err = E::AGAIN;
    return Message::Disposition::NORMAL;
  }

  auto peer_idx = w->sender().getNodeIdx(from);
  if (!peer_idx) {
    RATELIMIT_INFO(
        std::chrono::seconds(1),
        10,
        "got RELEASE %s but the socket to the node but socket was closed "
        "while message was waiting in the queue to be processed.",
        header.rid.toString().c_str());
    err = E::AGAIN;
    return Message::Disposition::NORMAL;
  }

  LogStorageState* log_state =
      processor->getLogStorageStateMap().insertOrGet(header.rid.logid, shard);
  if (log_state == nullptr) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    1,
                    "LogStorageStateMap is full. RELEASE messages for new "
                    "logs will not be processed.");
    return Message::Disposition::NORMAL;
  }

  RecordCache* cache = log_state->record_cache_.get();

  if (cache != nullptr) {
    // Update RecordCache on release in the hope of evicting some records
    // and/or epochs. May bump up the LNG of the epoch record cache (hence we
    // do this after checking whether to update the metadata).
    cache->onRelease(header.rid.lsn());
  }

  auto peer_nid = peer_idx ? NodeID(*peer_idx) : NodeID();
  checked_downcast<PurgeCoordinator&>(*log_state->purge_coordinator_)
      .onReleaseMessage(header.rid.lsn(), peer_nid, header.release_type);

  return Message::Disposition::NORMAL;
}

void PurgeCoordinator::onReleaseMessage(lsn_t lsn,
                                        NodeID from,
                                        ReleaseType release_type,
                                        OffsetMap epoch_offsets) {
  // During rebuilding, don't purge, don't persist last released LSN and
  // don't broadcast the release.
  ServerWorker* worker = ServerWorker::onThisThread();

  bool release_now;
  epoch_t release_epoch = lsn_to_epoch(lsn);
  if (release_epoch.val_ == 0) {
    // We shouldn't receive a RELEASE in epoch 0 but if we do, we can process
    // it immediately (there is nothing to clean).
    release_now = true;
  } else {
    epoch_t last_clean = parent_->getLastCleanEpoch();
    // We can immediately process this RELEASE if its epoch is at most
    // last_clean + 1.  The + 1 allows RELEASEs in the currently active epoch.
    epoch_t max_epoch_immediate = epoch_t(last_clean.val_ + 1);
    release_now = release_epoch <= max_epoch_immediate;
  }
  if (release_now) {
    this->doRelease(lsn, release_type, std::move(epoch_offsets));
    return;
  }
  if (parent_->hasPermanentError()) {
    // The purge is unlikely to succeed because LocalLogStore is broken.
    // Don't bother even trying.
    return;
  }

  // Do not process the release if the log does not exist.
  // as purging will fail. Log may not exists becasue
  // 1. It is deleted - Dropping release in innocuous
  // 2. This node has a stale config - if log does exist
  // and is being written to, future release will be processed
  // once config catches up to version that has the log. Dropping
  // the release now should be innocuous as well
  if (!logExistsInConfig()) {
    // Log does not exists in the config. Do not
    // process this release as purging will fail anyway
    RATELIMIT_INFO(std::chrono::seconds(10),
                   2,
                   "Not processing release message for log %lu as it does not "
                   "exist in config",
                   log_id_.val_);
    return;
  }

  // We need to purge (or just load the last clean epoch) before we can
  // process this RELEASE.
  ld_check(release_type == ReleaseType::GLOBAL);
  std::unique_lock<std::mutex> guard(mutex_);

  // Buffer the RELEASE.  Once the PurgeUncleanEpochs state machine finishes,
  // we will re-examine the RELEASE, probably going through the fast path.
  if (!buffered_release_.has_value() || lsn > buffered_release_.value().lsn) {
    buffered_release_ = BufferedRelease{lsn, from};
  }

  ld_check(worker->purge_scheduler_ != nullptr);
  ResourceBudget::Token token =
      worker->purge_scheduler_->tryStartPurgeForRelease(log_id_, shard_);

  if (!token.valid()) {
    // we cannot start purging immediately as the current number of active
    // purges already reaches the limit for this shard on the worker. Instead,
    // the log_id is enqueued and will be retried later.
    return;
  }

  ld_check(release_epoch.val_ > 0);
  epoch_t purge_to = epoch_t(release_epoch.val_ - 1);
  startPurge(std::move(guard), purge_to, purge_to, from, std::move(token));
  // No longer holding lock here
}

bool PurgeCoordinator::logExistsInConfig() {
  ServerWorker* w = ServerWorker::onThisThread();
  return w->getLogsConfig()->logExists(log_id_);
}

std::pair<Status, Seal>
PurgeCoordinator::checkPreemption(epoch_t sequencer_epoch) {
  ld_check(sequencer_epoch != EPOCH_INVALID);

  folly::Optional<Seal> normal_seal =
      parent_->getSeal(LogStorageState::SealType::NORMAL);
  folly::Optional<Seal> soft_seal =
      parent_->getSeal(LogStorageState::SealType::SOFT);

  if (!normal_seal.has_value() || !soft_seal.has_value() ||
      !normal_seal->valid()) {
    // We expect both normal seal and soft seal are likely to have values,
    // since by the time the node received CLEAN message, it must have been
    // Sealed by the same sequencer node already. Sealed implies that both
    // normal seal and soft seal have values (See SealStorageTask).
    // The unlikely scenario is that LD may get restarted and EpochRecovery
    // state machine sends a CLEAN after retry, in such case, currently we do
    // not check for preemption as this is not strictly required for
    // correctness.
    RATELIMIT_WARNING(std::chrono::seconds(1),
                      10,
                      "Unable to find seal of soft seal in LogStorageState "
                      "after received CLEAN message for log %lu from sequencer "
                      "with epoch %u. Probably this node was restarted between "
                      "digesting and cleaning phase of recovery.",
                      log_id_.val_,
                      sequencer_epoch.val_);
    return std::make_pair(Status::OK, Seal());
  }

  Seal effective_seal = std::max(normal_seal.value(), soft_seal.value());
  ld_check(effective_seal.valid());

  if (effective_seal.epoch.val_ > sequencer_epoch.val_ - 1) {
    return std::make_pair(Status::PREEMPTED, effective_seal);
  }

  return std::make_pair(Status::OK, Seal());
}

void PurgeCoordinator::onCleanMessage(std::unique_ptr<CLEAN_Message> clean_msg,
                                      NodeID from,
                                      Address reply_to,
                                      worker_id_t worker) {
  ld_check(clean_msg != nullptr);
  const epoch_t epoch = clean_msg->header_.epoch;
  epoch_t last_clean = parent_->getLastCleanEpoch();
  if (epoch <= last_clean) {
    // We can immediately initiate a cleaned response if we already know this
    // epoch is clean

    // If the epoch of the recovering sequencer is included in the received
    // CLEAN message, check for preemption again before sending the response.
    // It is possible that another sequencer with higher epoch has started
    // and seals the epoch. In such case, it is better to inform the sequencer
    // that sent the CLEAN regarding the preemption so that it can deactivate
    // and send redirects in a more prompt manner.
    Status status = Status::OK;
    Seal preempted_by = Seal();
    const epoch_t seq_epoch = clean_msg->header_.sequencer_epoch;
    if (seq_epoch > EPOCH_INVALID) { // running old protocol
      std::tie(status, preempted_by) = checkPreemption(seq_epoch);
    }

    sendCleanedResponse(
        status, std::move(clean_msg), reply_to, worker, preempted_by);
    return;
  }
  if (parent_->hasPermanentError()) {
    // The purge is unlikely to succeed because LocalLogStore is broken.
    // Don't bother even trying.
    sendCleanedResponse(
        E::FAILED, std::move(clean_msg), reply_to, worker, Seal());
    return;
  }

  // If the log is not in config, purging is doomed to fail. Don't start it.
  if (!logExistsInConfig()) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   2,
                   "Not processing clean message for log %lu as it does not "
                   "exist in config",
                   log_id_.val_);
    sendCleanedResponse(
        E::FAILED, std::move(clean_msg), reply_to, worker, Seal());
    return;
  }

  // Otherwise, we need to purge (or just load the last clean epoch).
  std::unique_lock<std::mutex> guard(mutex_);

  // Buffer the CLEAN.  Once the PurgeUncleanEpochs state machine finishes,
  // we will re-examine it.
  BufferedClean buf = {std::move(clean_msg), from, reply_to, worker};
  buffered_clean_.push_back(std::move(buf));

  ld_check(epoch.val_ > 0);
  epoch_t purge_to = epoch_t(epoch.val_ - 1);

  // Purging makes sure to store EpochRecoveryMetadata before
  // advancing the local LCE
  startPurge(std::move(guard), purge_to, epoch, from);
  // No longer holding lock here
}

void PurgeCoordinator::updateLastCleanInMemory(epoch_t epoch) {
  parent_->updateLastCleanEpoch(epoch);
}

void PurgeCoordinator::startBufferedMessages(
    std::vector<BufferedClean> buffered_clean,
    folly::Optional<BufferedRelease> buffered_release) {
  for (BufferedClean& clean : buffered_clean) {
    ld_check(clean.message != nullptr);
    onCleanMessage(
        std::move(clean.message), clean.from, clean.reply_to, clean.worker);
  }

  if (buffered_release.has_value()) {
    onReleaseMessage(
        buffered_release->lsn, buffered_release->from, ReleaseType::GLOBAL);
  }
}

void PurgeCoordinator::startBuffered() {
  decltype(buffered_release_) to_release;
  decltype(buffered_clean_) to_clean;

  {
    std::lock_guard<std::mutex> guard(mutex_);
    if (active_purge_ != nullptr) {
      // an active purge is already running, do nothing for now
      // since the messages will be replayed once the active purge
      // is done
      return;
    }

    to_release = std::move(buffered_release_);
    buffered_release_.reset();
    to_clean = std::move(buffered_clean_);
    buffered_clean_.clear();
  }

  startBufferedMessages(std::move(to_clean), std::move(to_release));
}

void PurgeCoordinator::onStateMachineDone() {
  decltype(buffered_release_) to_release;
  decltype(buffered_clean_) to_clean;

  {
    std::lock_guard<std::mutex> guard(mutex_);

    // Steal the buffered RELEASE and CLEAN messages (if any).
    to_release = std::move(buffered_release_);
    buffered_release_.reset();
    to_clean = std::move(buffered_clean_);
    buffered_clean_.clear();

    // Destroy the PurgeUncleanEpochs instance.  After we go out of this scope
    // and release the lock, some other thread may start a new state machine
    // in response to a new RELEASE or CLEAN message.  That is fine.
    ld_check((bool)active_purge_);
    active_purge_.reset();
  }

  // Now replay the messages outside the lock, as if the worker just received
  // them.
  startBufferedMessages(std::move(to_clean), std::move(to_release));

  // try to schedule more purging for releases enqueued earlier on the worker
  // for this shard.
  ServerWorker* worker = ServerWorker::onThisThread();
  ld_check(worker->purge_scheduler_ != nullptr);
  worker->purge_scheduler_->wakeUpMorePurgingForReleases(shard_);
}

void PurgeCoordinator::shutdown() {
  std::lock_guard<std::mutex> guard(mutex_);
  ld_check(active_purge_);
  active_purge_.reset();
}

void PurgeCoordinator::doRelease(lsn_t lsn,
                                 ReleaseType release_type,
                                 OffsetMap epoch_offsets) {
  ld_spew("log %lu releasing %s, release_type=%s",
          log_id_.val_,
          lsn_to_string(lsn).c_str(),
          release_type_to_string(release_type).c_str());

  if (!epoch_valid_or_unset(lsn_to_epoch(lsn))) {
    RATELIMIT_CRITICAL(
        std::chrono::seconds(10),
        10,
        "INTERNAL ERROR: releasing invalid epoch %u for log %lu ",
        lsn_to_epoch(lsn).val_,
        log_id_.val_);
    ld_check(false);
    return;
  }

  switch (release_type) {
    case ReleaseType::GLOBAL:
      // Global release. Update epoch offset and last-released LSN.
      if (epoch_offsets.isValid()) {
        parent_->updateEpochOffsetMap(
            std::make_pair(lsn_to_epoch(lsn), std::move(epoch_offsets)));
      }

      // releasing the lsn means that the lsn_to_epoch(lsn)-1 is clean and
      // will definitely no longer needed for recovery or purging.
      // check record cache for potential chances of evicting epochs
      if (lsn_to_epoch(lsn) > EPOCH_INVALID) {
        updateLastCleanEpochInRecordCache(epoch_t(lsn_to_epoch(lsn).val_ - 1));
      }

      if (parent_->updateLastReleasedLSN(
              lsn, LogStorageState::LastReleasedSource::RELEASE) != 0) {
        ld_check(err == E::UPTODATE);
        // LogStorageStateMap already had a last-released LSN that was >= what
        // we wanted to set.  This can happen if RELEASE messages come from
        // different sources and get processed out of order.  Since releasing
        // is cumulative, we don't need to do anything.
        return;
      }
      break;
    case ReleaseType::PER_EPOCH_DEPRECATED:
      RATELIMIT_CRITICAL(
          std::chrono::seconds(1), 10, "sending PER_EPOCH_DEPRECATED releases");
      break;
    case ReleaseType::INVALID:
      ld_check(false);
      break;
  }

  RecordID rid = {lsn_to_esn(lsn), lsn_to_epoch(lsn), log_id_};
  broadcastReleaseRequest(parent_, rid, shard_);
}

void PurgeCoordinator::updateLastCleanEpochInRecordCache(epoch_t lce) {
  RecordCache* cache = parent_->record_cache_.get();
  if (cache != nullptr) {
    cache->onLastCleanEpochAdvanced(lce);
  }
}

void PurgeCoordinator::startPurge(std::unique_lock<std::mutex> guard,
                                  epoch_t purge_to,
                                  epoch_t new_last_clean_epoch,
                                  NodeID from,
                                  ResourceBudget::Token token) {
  ServerWorker* w = ServerWorker::onThisThread();

  if (active_purge_ || !w->isAcceptingWork()) {
    return;
  }

  active_purge_ =
      std::make_shared<PurgeUncleanEpochs>(this,
                                           log_id_,
                                           shard_,
                                           parent_->getLastCleanEpoch(),
                                           purge_to,
                                           new_last_clean_epoch,
                                           from,
                                           Worker::stats());

  if (token.valid()) {
    active_purge_->setResourceToken(std::move(token));
  }

  // Register this purge with the Worker so that, in the (unlikely) event of
  // the worker shutting down, the purge can be destroyed on the worker
  // thread.
  int rv = w->activePurges().map.insert(*active_purge_);
  if (rv != 0) {
    ld_check(err == E::EXISTS);
    // another instance of PurgeUncleanEpoch is active, this should not happen
    RATELIMIT_CRITICAL(std::chrono::seconds(1),
                       10,
                       "INTERNAL ERROR: a PurgeUncleanEpochs is already active "
                       "for log %lu, purge_to: %u",
                       log_id_.val_,
                       purge_to.val_);
    ld_check(false);
    guard.unlock();
    return;
  }

  // We need to release the lock to start PurgeUncleanEpochs because it might
  // immediately call the completion callback onStateMachineDone() which needs
  // to have the lock available.  Since we are now in a consistent state, this
  // is safe to do.
  guard.unlock();

  active_purge_->start();
}

void PurgeCoordinator::sendCleanedResponse(
    Status status,
    std::unique_ptr<CLEAN_Message> clean_msg,
    Address reply_to,
    worker_id_t worker_id,
    Seal preempted_seal) {
  // We need to make sure that we are on the right worker when sending the
  // reply.  This handles a scenario like:
  // (1) CLEAN message for epoch 3 arrives at W1 from C1.  We begin purging
  // epochs 1 and 2.
  // (2) CLEAN message for epoch 2 arrives at W2 from C2.  We are already
  // purging so we buffer the CLEAN.
  // (3) Purging completes.  We are on W1 (where we started purging) and need
  // to send OK replies to C1 and C2.  Because W2 owns the socket to C2, we
  // need to ask it to send the message.
  std::unique_ptr<Request> req =
      std::make_unique<CleanedResponseRequest>(status,
                                               std::move(clean_msg),
                                               worker_id,
                                               reply_to,
                                               preempted_seal,
                                               shard_);

  Worker* w = Worker::onThisThread();
  if (worker_id == w->idx_) {
    // we are already on the correct worker, execute the request directly
    auto result = req->execute();
    if (result == Request::Execution::CONTINUE) {
      // requeset becomes self-owned
      req.release();
    }
  } else {
    // NOTE: We don't care if request posting fails. The sequencer is prepared
    // to handle lack of response anyway.
    w->processor_->postRequest(req);
  }
}

void PurgeCoordinator::onPermanentError(const char* context, Status status) {
  RATELIMIT_INFO(std::chrono::seconds(10),
                 2,
                 "%s failed with status %s for log %lu. "
                 "Entering fail-safe mode on LogStore of shard %u.",
                 context,
                 error_description(status),
                 log_id_.val_,
                 shard_);

  ServerWorker::onThisThread()
      ->processor_->sharded_storage_thread_pool_->getByIndex(shard_)
      .getLocalLogStore()
      .enterFailSafeMode(context, error_description(status));

  parent_->notePermanentError("Purging");

  // Now, let's make sure we wake-up all reach streams so they pick up the
  // information that last released will never be moved.
  RecordID rid = {ESN_INVALID, EPOCH_INVALID, log_id_};
  broadcastReleaseRequest(parent_, rid, shard_, true /* force */);
}

}} // namespace facebook::logdevice
