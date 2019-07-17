/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/RecordRebuildingBase.h"

#include <folly/Random.h>

#include "logdevice/common/CopySetSelector.h"
#include "logdevice/common/ExponentialBackoffTimer.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/stats/PerShardHistograms.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice {

RecordRebuildingBase::RecordRebuildingBase(
    lsn_t lsn,
    shard_index_t shard,
    RecordRebuildingOwner* owner,
    std::shared_ptr<ReplicationScheme> replication,
    const NodeAvailabilityChecker* node_availability)
    : nodeAvailability_(node_availability),
      lsn_(lsn),
      shard_(shard),
      owner_(owner),
      tracer_(Worker::onThisThread(false)
                  ? Worker::onThisThread()->getTraceLogger()
                  : nullptr),
      creation_time_(std::chrono::steady_clock::now()),
      replication_(std::move(replication)),
      sender_(std::make_unique<SenderProxy>()),
      refHolder_(this) {
  ld_check(replication_ != nullptr);
}

RecordRebuildingBase::~RecordRebuildingBase() {}

copyset_off_t
RecordRebuildingBase::findCopysetOffset(RebuildingCopyset& copyset,
                                        ShardID target) {
  auto it = std::find(copyset.begin(), copyset.end(), target);
  return it == copyset.end() ? -1 : std::distance(copyset.begin(), it);
}

ShardID RecordRebuildingBase::getMyShardID() const {
  return ShardID(getMyNodeIndex(), shard_);
}

node_index_t RecordRebuildingBase::getMyNodeIndex() const {
  return Worker::onThisThread()->processor_->getMyNodeID().index();
}

bool RecordRebuildingBase::isStorageShardInConfig(ShardID shard) const {
  const auto& storage_membership =
      Worker::onThisThread()->getNodesConfiguration()->getStorageMembership();
  return storage_membership->shouldReadFromShard(shard);
}

const Settings& RecordRebuildingBase::getSettings() const {
  return Worker::settings();
}

void RecordRebuildingBase::activateRetryTimer() {
  if (!retryTimer_.isAssigned()) {
    const auto& retry_timeout = owner_->getRebuildingSettings()->retry_timeout;
    retryTimer_.assign(
        [this] { onRetryTimeout(); }, retry_timeout.lo, retry_timeout.hi);
  }

  retryTimer_.activate();
}

void RecordRebuildingBase::resetRetryTimer() {
  retryTimer_.reset();
}

bool RecordRebuildingBase::isStoreTimerActive() {
  return storeTimer_.isAssigned() && storeTimer_.isActive();
}

void RecordRebuildingBase::activateStoreTimer() {
  if (!storeTimer_.isAssigned()) {
    const auto& store_timeout = owner_->getRebuildingSettings()->store_timeout;
    storeTimer_.assign(
        [this] { onStoreTimeout(); }, store_timeout.lo, store_timeout.hi);
  }

  storeTimer_.activate();
}

void RecordRebuildingBase::resetStoreTimer() {
  storeTimer_.reset();
}

void RecordRebuildingBase::deferredComplete() {
  // Start a timer with zero delay.
  deferredCompleteTimer_ = std::make_unique<Timer>([this] { onComplete(); });
  deferredCompleteTimer_->activate(std::chrono::milliseconds(0));
}

uint32_t RecordRebuildingBase::getStoreTimeoutMs() const {
  return storeTimer_.getCurrentDelay().count();
}

void RecordRebuildingBase::putAmendSelfTask(
    std::unique_ptr<AmendSelfStorageTask> task) {
  ServerWorker::onThisThread()
      ->getStorageTaskQueueForShard(getMyShardID().shard())
      ->putTask(std::move(task));
}

int RecordRebuildingBase::sendStage(StageRecipients* s,
                                    bool resend_inflight_stores) {
  bool need_retry_timer = false;
  bool need_store_timer = false;
  curStageRepliesExpected_ = 0;
  curStageRecipient_ = s;

  bool timer_previously_active = isStoreTimerActive();
  // activating timer before sending messages so STORE messages include a
  // correct STORE timeout
  if (!timer_previously_active) {
    activateStoreTimer();
  }

  SCOPE_EXIT {
    // resetting the timer if we don't actually need it
    if (!timer_previously_active &&
        (!need_store_timer || deferredStores_ != 0)) {
      resetStoreTimer();
    }
  };

  for (auto& r : s->recipients) {
    if (r.succeeded) {
      continue;
    }
    ++curStageRepliesExpected_;
    if ( // waiting for bandwidth
        r.on_bw_avail.active() ||
        // waiting for a STORED message
        (!resend_inflight_stores && r.isStoreInFlight())) {
      continue;
    }
    if (s->type == StageRecipients::Type::AMEND && r.shard_ == getMyShardID()) {
      if (amendSelfStorageTaskInFlight_) {
        // When all amends (including amendself) are executed in parallel and
        // remote amend had failed while amendself was still active, only resend
        // remote amend.
        continue;
      }
      amendSelf();
    } else {
      if (sendStore(s->type, r) == 0) {
        need_store_timer = true;
      } else if (err == E::NOSPC) {
        // Start a new wave.
        return -1;
      } else if (err == E::AGAIN) {
        need_retry_timer = true;
      } else {
        ld_check(err == E::CBREGISTERED);
        // Nothing to do until the deferred store callback is called.
      }
    }
  }
  ld_check(curStageRepliesExpected_ > 0);
  ld_check(curStageRepliesExpected_ <= s->recipients.size());
  ld_check(deferredStores_ <= s->recipients.size());
  if (need_retry_timer) {
    activateRetryTimer();
  }
  return 0;
}

std::string RecordRebuildingBase::stageDebugInfo(StageRecipients* s) const {
  std::string res;
  for (size_t i = 0; i < s->recipients.size(); ++i) {
    std::string type =
        s->type == StageRecipients::Type::AMEND ? "AMEND" : "STORE";
    res += s->recipients[i].shard_.toString();
    res += "(type=" + type;
    res += ", succeeded=" + std::to_string(s->recipients[i].succeeded);
    res += ", waiting_for_bw=" +
        std::to_string(s->recipients[i].on_bw_avail.active());
    res += ")";
    if (i != s->recipients.size() - 1) {
      res += ", ";
    }
  }
  return res;
}

int RecordRebuildingBase::sendStore(StageRecipients::Type type,
                                    RecipientNode& recipient) {
  ld_check(!recipient.on_bw_avail.active());
  recipient.on_socket_close.deactivate();

  bool amend = true;
  switch (type) {
    case StageRecipients::Type::AMEND:
      WORKER_STAT_INCR(rebuilding_amend_sent);
      break;
    case StageRecipients::Type::STORE:
      WORKER_STAT_INCR(rebuilding_store_sent);
      amend = false;
      break;
  }

  StoreChainLink dest;
  auto status = nodeAvailability_->checkNode(
      replication_->nodeset_state.get(), recipient.shard_, &dest, true);

  bool can_send = true;
  const char* unavailable_reason = "other";

  do { // while (false)
    if (status == NodeAvailabilityChecker::NodeStatus::NOT_AVAILABLE) {
      can_send = false;
      break;
    }

    const auto reason =
        replication_->nodeset_state->getNotAvailableReason(recipient.shard_);

    if (reason == NodeSetState::NotAvailableReason::NONE) {
      // Normal case - everything is fine.
      break;
    }

    unavailable_reason = NodeSetState::reasonString(reason);

    if (reason != NodeSetState::NotAvailableReason::NO_SPC) {
      can_send = false;
      break;
    }

    if (!amend) {
      // The node is out of space. Return -1 so that we will pick a different
      // copyset.
      traceEvent("RECIPIENT_NOT_AVAILABLE", unavailable_reason);
      RATELIMIT_INFO(
          std::chrono::seconds(10),
          2,
          "Can't send STORE to %s because it's out of space. Will pick a "
          "different copyset.",
          recipient.shard_.toString().c_str());
      err = E::NOSPC;
      return -1;
    }

    // Proceed with sending the amend to a node that's out of space. STORE with
    // AMEND flag is allowed even when out of space.
    RATELIMIT_INFO(std::chrono::seconds(10),
                   2,
                   "Send amend to %s even though it's out of space.",
                   recipient.shard_.toString().c_str());
  } while (false);

  if (!can_send) {
    // The node is not out of space. Rebuilding is currently designed based on
    // the premise that a node should not stay unavailable too long otherwise it
    // would be added to the rebuilding set. Do nothing and rely on the timer to
    // retry a bit later.
    traceEvent("RECIPIENT_NOT_AVAILABLE", unavailable_reason);
    RATELIMIT_INFO(
        std::chrono::seconds(10),
        2,
        "Can't send %s to %s because it's marked unavailable: %s. Will retry.",
        amend ? "AMEND" : "STORE",
        recipient.shard_.toString().c_str(),
        unavailable_reason);
    err = E::AGAIN;
    return -1;
  }

  auto message = buildStoreMessage(recipient.shard_, amend);

  ld_spew("sending STORE%s %lu%s to %s",
          amend ? " (amend)" : "",
          owner_->getLogID().val_,
          lsn_to_string(lsn_).c_str(),
          recipient.shard_.toString().c_str());
  int rv = sender_->sendMessage(std::move(message),
                                recipient.shard_.asNodeID(),
                                &recipient.on_bw_avail,
                                &recipient.on_socket_close);
  if (rv != 0) {
    if (err == E::CBREGISTERED) {
      recipient.on_bw_avail.setMessage(std::move(message), recipient.shard_);
      recipient.on_socket_close.deactivate();
      deferredStores_++;
      err = E::CBREGISTERED;
      return -1;
    } else {
      traceEvent(
          amend ? "SEND_AMEND_FAILED" : "SEND_STORE_FAILED", error_name(err));
      RATELIMIT_WARNING(std::chrono::seconds(10),
                        2,
                        "Failed to send STORE%s to %s: %s",
                        amend ? " (amend)" : "",
                        recipient.shard_.toString().c_str(),
                        error_description(err));
      err = E::AGAIN;
      return -1;
    }
  } else {
    ld_check(recipient.isStoreInFlight());
  }
  ld_check(!recipient.on_bw_avail.active());

  return 0;
}

std::unique_ptr<STORE_Message>
RecordRebuildingBase::buildStoreMessage(ShardID target_shard, bool amend) {
  RebuildingStoreChain copyset(newCopyset_.size());
  std::transform(newCopyset_.begin(),
                 newCopyset_.end(),
                 copyset.begin(),
                 [](ShardID shard) {
                   return StoreChainLink{shard, ClientID()};
                 });

  storeHeader_.timeout_ms = getStoreTimeoutMs();
  storeHeader_.sequencer_node_id = replication_->sequencer_node_id;
  STORE_flags_t add_flags = 0;
  if (amend) {
    add_flags |= STORE_Header::AMEND;
    // Just in case, clear checksum flags if we don't ship payload.
    add_flags &= ~(STORE_Header::CHECKSUM | STORE_Header::CHECKSUM_64BIT);
    add_flags |= STORE_Header::CHECKSUM_PARITY;
  }

  STORE_Extra extra;
  extra.rebuilding_version = owner_->getRestartVersion();
  extra.rebuilding_wave = rebuildingWave_;
  extra.rebuilding_id = owner_->getLogRebuildingId();
  extra.offsets_within_epoch = offsets_within_epoch_;

  copyset_off_t offset = findCopysetOffset(newCopyset_, target_shard);
  ld_check(offset >= 0 && offset < copyset.size());
  auto message = std::make_unique<STORE_Message>(
      storeHeader_,
      &copyset[0],
      offset,
      add_flags,
      extra,
      amend ? std::map<KeyType, std::string>() : optional_keys_,
      amend ? nullptr : getPayloadHolder(),
      false);

  return message;
}

void RecordRebuildingBase::sendDeferredStore(std::unique_ptr<Message> msg,
                                             ShardID shard) {
  ld_check(msg);
  ld_check(deferredStores_ > 0);

  deferredStores_--;

  auto r = findRecipient(shard);
  ld_check(r != nullptr);

  if (r->isStoreInFlight() || r->on_socket_close.active()) {
    // TODO (#T31205569): Remove this after finding and fixing the issue.
    RATELIMIT_CRITICAL(
        std::chrono::seconds(10),
        2,
        "INTERNAL ERROR: Trying to send deferred store for a recipient that "
        "already has store in flight. Log: %lu, LSN: %s, existing copyset: %s, "
        "new copyset: %s, rebuilding wave: %u, recipient: %s, stage info: %s",
        owner_->getLogID().val_,
        lsn_to_string(lsn_).c_str(),
        toString(existingCopyset_).c_str(),
        toString(newCopyset_).c_str(),
        rebuildingWave_,
        shard.toString().c_str(),
        stageDebugInfo(curStageRecipient_).c_str());
    ld_check(false);
  }

  ld_check(!r->isStoreInFlight());
  ld_check(!r->on_socket_close.active());

  int rv = sender_->sendMessage(
      std::move(msg), shard.asNodeID(), &r->on_socket_close);
  if (rv != 0) {
    activateRetryTimer();
  } else if (deferredStores_ == 0) {
    activateStoreTimer();
  }
}

void RecordRebuildingBase::onDeferredStoreCancelled(
    std::unique_ptr<Message> msg,
    ShardID shard,
    Status st) {
  ld_check(msg);
  ld_check(deferredStores_ > 0);
  deferredStores_--;

  if (st != E::OK) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   2,
                   "Deferred STORE for %lu%s on %s cancelled with %s status. "
                   "Starting retry timer",
                   owner_->getLogID().val_,
                   lsn_to_string(lsn_).c_str(),
                   shard.toString().c_str(),
                   error_name(st));

    // The socket for this recipient has been closed. Since this stage
    // cannot succeed, schedule a retry.
    activateRetryTimer();
  } else if (deferredStores_ == 0) {
    activateStoreTimer();
  }
}

RecordRebuildingBase::RecipientNode*
RecordRebuildingBase::findRecipient(ShardID shard) {
  auto it =
      std::find_if(curStageRecipient_->recipients.begin(),
                   curStageRecipient_->recipients.end(),
                   [&](const RecipientNode& r) { return r.shard_ == shard; });
  return it == curStageRecipient_->recipients.end() ? nullptr : &*it;
}

void RecordRebuildingBase::onStored(const STORED_Header& header,
                                    ShardID from,
                                    lsn_t rebuilding_version,
                                    uint32_t rebuilding_wave,
                                    log_rebuilding_id_t rebuilding_id,
                                    ServerInstanceId server_instance_id,
                                    FlushToken flush_token) {
  if (getSettings().rebuilding_dont_wait_for_flush_callbacks) {
    flush_token = FlushToken_INVALID;
  }

  ld_spew("got STORED for %lu%s from %s with status %s",
          owner_->getLogID().val_,
          lsn_to_string(lsn_).c_str(),
          from.toString().c_str(),
          error_name(header.status));
  ld_check(header.flags & STORED_Header::REBUILDING);

  if (header.wave != storeHeader_.wave) {
    RATELIMIT_WARNING(std::chrono::seconds(10),
                      2,
                      "Got STORED with wrong wave from %s: expected %u, got %u",
                      from.toString().c_str(),
                      storeHeader_.wave,
                      header.wave);
    return;
  }

  // rebuilding_id check is necessary only when LogRebuilding is restarted
  // without a change in the version.
  if (rebuilding_id != LOG_REBUILDING_ID_INVALID &&
      rebuilding_id != owner_->getLogRebuildingId()) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   2,
                   "got STORED for %lu%s from %s for an old rebuilding id "
                   "%lu. Current rebuilding id is %lu",
                   owner_->getLogID().val_,
                   lsn_to_string(lsn_).c_str(),
                   from.toString().c_str(),
                   rebuilding_id.val_,
                   owner_->getLogRebuildingId().val_);
    return;
  }

  if (rebuilding_version != LSN_INVALID &&
      rebuilding_version != owner_->getRestartVersion()) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   2,
                   "got STORED for %lu%s from %s for an old rebuilding version "
                   "%s. Current rebuilding version is %s",
                   owner_->getLogID().val_,
                   lsn_to_string(lsn_).c_str(),
                   from.toString().c_str(),
                   lsn_to_string(rebuilding_version).c_str(),
                   lsn_to_string(owner_->getRestartVersion()).c_str());
    return;
  }

  if (rebuilding_wave != 0 && rebuilding_wave != rebuildingWave_) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   2,
                   "got STORED for %lu%s from %s for an old rebuilding wave %u."
                   " Current rebuilding wave is %u",
                   owner_->getLogID().val_,
                   lsn_to_string(lsn_).c_str(),
                   from.toString().c_str(),
                   rebuilding_wave,
                   rebuildingWave_);
    return;
  }

  if (curStageRepliesExpected_ == 0) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   2,
                   "got STORED for %lu%s from %s when we weren't expecting any."
                   " possible duplicate",
                   owner_->getLogID().val_,
                   lsn_to_string(lsn_).c_str(),
                   from.toString().c_str());
    return;
  }

  auto r = findRecipient(from);
  if (r == nullptr) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   2,
                   "Got unexpected STORED from %s for %lu%s; "
                   "likely from previous stage which is normal",
                   from.toString().c_str(),
                   owner_->getLogID().val_,
                   lsn_to_string(lsn_).c_str());
    return;
  }

  if (r->succeeded ||
      // Accept a successful store reply even if we don't think there was
      // a store in flight. It's possible e.g. if a store timed out, then we
      // failed to send another store, then the first store succeeded.
      (header.status != E::OK && !r->isStoreInFlight())) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   2,
                   "Got duplicate STORED from %s for %lu%s",
                   from.toString().c_str(),
                   owner_->getLogID().val_,
                   lsn_to_string(lsn_).c_str());
    return;
  }

  bool amend = curStageRecipient_->type != StageRecipients::Type::STORE;
  if (header.status != E::OK) {
    traceEvent(
        amend ? "AMEND_FAILED" : "STORE_FAILED", error_name(header.status));
    if (header.status == E::NOSPC) {
      // Only pick another copyset if we got NOSPC. Node that's out of space
      // can still accept amends and can itself be a donor, so rebuilding
      // can make progress and we should pick another copyset and move on.
      // Node that is down in any other way will stall rebuilding, so we can
      // as well wait for it to come back.
      replication_->nodeset_state->setNotAvailableUntil(
          from,
          std::chrono::steady_clock::now() +
              getSettings().nospace_retry_interval,
          NodeSetState::NodeSetState::NotAvailableReason::NO_SPC);
      onStoreFailed();
      return;
    }

    // A note about header.status == E::REBUILDING case: it doesn't need special
    // handling, just retries. Since we never pick nodes from rebuilding set
    // into copysets, E::REBUILDING means that recipient's view of rebuilding
    // set is different from ours, i.e. at least one of us fell behind on
    // reading the event log. Just keep retrying until we catch up.

    // Will retry after a timeout.
    RATELIMIT_WARNING(std::chrono::seconds(10),
                      2,
                      "Failed to %s record %lu%s on %s: %s",
                      amend ? "amend" : "store",
                      owner_->getLogID().val_,
                      lsn_to_string(lsn_).c_str(),
                      from.toString().c_str(),
                      error_description(header.status));
    onRecipientTransientFailure(r);
    return;
  }
  if (amend) {
    WORKER_STAT_INCR(rebuilding_donor_amended_ok);
  } else {
    WORKER_STAT_INCR(rebuilding_donor_stored_ok);
  }
  onRecipientSuccess(r, server_instance_id, flush_token);
}

void RecordRebuildingBase::onStoreSent(Status st,
                                       const STORE_Header& header,
                                       ShardID to,
                                       lsn_t rebuilding_version,
                                       uint32_t rebuilding_wave) {
  ld_check(header.flags & STORE_Header::REBUILDING);
  if (st == E::OK) {
    return;
  }
  RATELIMIT_INFO(
      std::chrono::seconds(10),
      2,
      "Failed to send STORE %lu%s to %s: %s. Will retry after a timeout.",
      owner_->getLogID().val_,
      lsn_to_string(lsn_).c_str(),
      to.toString().c_str(),
      errorStrings()[st].name);

  auto r = findRecipient(to);
  if (r == nullptr || !r->isStoreInFlight() ||
      header.wave != storeHeader_.wave ||
      rebuilding_version != owner_->getRestartVersion() ||
      rebuilding_wave != rebuildingWave_) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   2,
                   "Got stale onStoreSent(). This should be rare.");
    return;
  }

  ld_check(!r->succeeded);
  onRecipientTransientFailure(r);
}

void RecordRebuildingBase::onConnectionClosed(ShardID shard) {
  auto r = findRecipient(shard);
  ld_check(r != nullptr);
  ld_check(!r->succeeded);
  ld_check(!r->on_socket_close.active()); // we're inside this callback

  RATELIMIT_INFO(
      std::chrono::seconds(10),
      2,
      "Socket to %s closed after we sent a STORE to it. Will try to reconnect "
      "after a timeout.",
      shard.toString().c_str());
  activateRetryTimer();
}

void RecordRebuildingBase::onRecipientSuccess(
    RecipientNode* r,
    ServerInstanceId server_instance_id,
    FlushToken flush_token) {
  ld_check(!r->succeeded);

  // When a timeout expires, we may retry sending the current stage
  // and thus attempt to send a duplicate STORE. However, while this
  // duplicate STORE is hung up waiting for bandwidth, we can receive a
  // STORED for the previous request that timed-out.  Since the previous
  // request is identical to the STORE we would have sent once bandwidth
  // is available, just cancel the callback and accept that this node
  // completed our request successfully.
  r->deactivateCallbacks();

  r->succeeded = true;
  r->server_instance_id = server_instance_id;
  r->flush_token = flush_token;
  ld_check(curStageRepliesExpected_ > 0);
  --curStageRepliesExpected_;
  if (curStageRepliesExpected_ == 0) {
    onStageComplete();
  }
}

void RecordRebuildingBase::onRecipientTransientFailure(RecipientNode* r) {
  r->deactivateCallbacks();
  activateRetryTimer();
}

void RecordRebuildingBase::amendSelf() {
  ld_spew("amending own copy of %lu%s",
          owner_->getLogID().val_,
          lsn_to_string(lsn_).c_str());
  ld_check(!amendSelfStorageTaskInFlight_);
  amendSelfStorageTaskInFlight_ = true;
  putAmendSelfTask(std::make_unique<AmendSelfStorageTask>(*this));
}

void RecordRebuildingBase::onAmendedSelf(Status status,
                                         FlushToken flush_token) {
  if (status != E::OK) {
    // This is almost certainly a persistent error. While waiting for it to
    // be fixed externally, let's keep retrying, we have nothing better to do.
    RATELIMIT_INFO(std::chrono::seconds(10),
                   2,
                   "Failed to amend own copy of record %lu%s: %s.",
                   owner_->getLogID().val_,
                   lsn_to_string(lsn_).c_str(),
                   error_description(status));
    traceEvent("SELF_AMEND_FAILED", error_name(status));
    activateRetryTimer();
    amendSelfStorageTaskInFlight_ = false;
    return;
  }

  auto r = findRecipient(getMyShardID());
  ld_check(r != nullptr);
  onRecipientSuccess(r, owner_->getServerInstanceId(), flush_token);
}

logid_t RecordRebuildingBase::getLogID() const {
  return owner_->getLogID();
}

lsn_t RecordRebuildingBase::getRestartVersion() const {
  return owner_->getRestartVersion();
}

bool RecordRebuildingBase::checkEveryoneStillInConfig() {
  ld_check(!newCopyset_.empty());
  for (ShardID shard : newCopyset_) {
    if (!isStorageShardInConfig(shard)) {
      return false;
    }
  }
  return true;
}

PutWriteOp RecordRebuildingBase::AmendSelfStorageTask::createWriteOp(
    const RecordRebuildingBase& owner) {
  auto record_header_flags = owner.recordFlags_ |
      LocalLogStoreRecordFormat::FLAG_AMEND |
      (owner.replication_->relocate_local_records
           ? LocalLogStoreRecordFormat::FLAG_DRAINED
           : 0) |
      (owner.getSettings().write_shard_id_in_copyset
           ? LocalLogStoreRecordFormat::FLAG_SHARD_ID
           : 0) |
      // TODO (T35832374) : remove if condition when all servers support
      // OffsetMap
      (owner.getSettings().enable_offset_map
           ? LocalLogStoreRecordFormat::FLAG_OFFSET_MAP
           : 0);

  Slice record_header = LocalLogStoreRecordFormat::formRecordHeader(
      owner.storeHeader_.timestamp,
      owner.storeHeader_.last_known_good,
      record_header_flags,
      owner.storeHeader_.wave,
      folly::Range<const ShardID*>(
          owner.newCopyset_.begin(), owner.newCopyset_.end()),
      owner.offsets_within_epoch_,
      std::map<KeyType, std::string>(),
      &recordHeaderBuf_);

  folly::Optional<lsn_t> block_starting_lsn;
  if ((owner.getSettings().write_copyset_index &&
       owner.getSettings().write_sticky_copysets_deprecated) ||
      MetaDataLog::isMetaDataLog(owner.getLogID()) ||
      configuration::InternalLogs::isInternal(owner.getLogID())) {
    // Always write CSI for internal logs. This makes enabling CSI operationally
    // easier.
    // TODO (t9002309) : sticky copysets block LSN
    block_starting_lsn = LSN_INVALID;
  }

  auto csi_flags =
      LocalLogStoreRecordFormat::formCopySetIndexFlags(record_header_flags);

  Slice csi_entry = LocalLogStoreRecordFormat::formCopySetIndexEntry(
      owner.storeHeader_.wave,
      owner.newCopyset_.begin(),
      owner.newCopyset_.size(),
      block_starting_lsn,
      csi_flags,
      &copySetIndexEntryBuf_);

  PutWriteOp res(owner.getLogID(),
                 owner.lsn_,
                 record_header,
                 Slice(),
                 owner.getMyNodeIndex(),
                 block_starting_lsn,
                 csi_entry,
                 // No need to write keys when amending copyset.
                 {},
                 owner.getSettings().rebuild_store_durability,
                 true);

  return res;
}

RecordRebuildingBase::AmendSelfStorageTask::AmendSelfStorageTask(
    const RecordRebuildingBase& owner)
    : WriteStorageTask(StorageTask::Type::REBUILDING_AMEND_SELF),
      logid(owner.getLogID()),
      lsn(owner.lsn_),
      restartVersion(owner.getRestartVersion()),
      rebuildingWave(owner.rebuildingWave_),
      writeOp_(createWriteOp(owner)),
      ref_(owner.refHolder_.ref()) {}

void RecordRebuildingBase::AmendSelfStorageTask::onDoneOrDropped(
    Status status) {
  if (!ref_) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   2,
                   "RecordRebuildingBase was destroyed before its "
                   "AmendSelfStorageTask finished. Likely aborted.");
    return;
  }

  RecordRebuildingBase* r = ref_.get();
  ld_check(r);
  ld_check(r->getLsn() == lsn);

  if (r->getSettings().rebuilding_dont_wait_for_flush_callbacks) {
    flushToken_ = FlushToken_INVALID;
  }

  if (r->getRestartVersion() != restartVersion) {
    RATELIMIT_INFO(
        std::chrono::seconds(10),
        2,
        "Discarding result of AmendSelfStorageTask because rebuilding version "
        "changed");
    return;
  }

  if (r->rebuildingWave_ != rebuildingWave) {
    // This should not happen because once we reach the last stage (amending
    // self), there is no failure mechanism to move on to another wave.
    ld_check(false);
    RATELIMIT_ERROR(
        std::chrono::seconds(10),
        2,
        "Discarding result of AmendSelfStorageTask because rebuilding wave "
        "changed");
    return;
  }

  r->onAmendedSelf(status, flushToken_);
}

void RecordRebuildingBase::AmendSelfStorageTask::onDone() {
  onDoneOrDropped(status_);
}

void RecordRebuildingBase::AmendSelfStorageTask::onDropped() {
  onDoneOrDropped(Status::DROPPED);
}

void RecordRebuildingBase::RecipientNode::ResendStoreCallback::
operator()(FlowGroup&, std::mutex&) {
  ld_check(msg_);
  record_rebuilding_->sendDeferredStore(std::move(msg_), shard_);
}

void RecordRebuildingBase::RecipientNode::ResendStoreCallback::cancelled(
    Status st) {
  ld_check(msg_);
  record_rebuilding_->onDeferredStoreCancelled(std::move(msg_), shard_, st);
}

void RecordRebuildingBase::RecipientNode::SocketClosedCallback::
operator()(Status, const Address&) {
  record_rebuilding_->onConnectionClosed(shard_);
}

}} // namespace facebook::logdevice
