/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/EpochRecovery.h"

#include <functional>

#include <folly/Conv.h>
#include <folly/CppAttributes.h>

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/AllSequencers.h"
#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/LogRecoveryRequest.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/SimpleEnumMap.h"
#include "logdevice/common/SocketCallback.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/protocol/GAP_Message.h"
#include "logdevice/common/protocol/MUTATED_Message.h"
#include "logdevice/common/stats/ServerHistograms.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

std::atomic<recovery_id_t::raw_type> EpochRecovery::next_id(1);

EpochRecovery::EpochRecovery(
    logid_t log_id,
    epoch_t epoch,
    const EpochMetaData& epoch_metadata,
    const std::shared_ptr<const NodesConfiguration>& nodes_configuration,
    std::unique_ptr<EpochRecoveryDependencies> deps,
    bool tail_optimized)
    : log_id_(log_id),
      epoch_(epoch),
      deps_(std::move(deps)),
      replication_(epoch_metadata.replication),
      id_(next_id++),
      creation_timestamp_(std::chrono::system_clock::now().time_since_epoch()),
      last_restart_timestamp_(creation_timestamp_),
      tail_optimized_(tail_optimized),
      state_(State::SEAL_OR_INACTIVE),
      recovery_set_(epoch_metadata, nodes_configuration, this),
      digest_(log_id,
              epoch,
              epoch_metadata,
              deps_->getSealEpoch(),
              nodes_configuration,
              {// write bridge record even for empty epoch if
               // settings allow _and_ log id is not a metadata log
               !MetaDataLog::isMetaDataLog(log_id_) &&
               deps_->getSettings().bridge_record_in_empty_epoch}),
      grace_period_(deps_->createTimer([this] { onGracePeriodExpired(); })),
      mutation_and_cleaning_(deps_->createTimer([this] { onTimeout(); })) {
  ld_check(log_id_ != LOGID_INVALID);
  ld_check(epoch_ != EPOCH_INVALID);
  ld_check(deps_ != nullptr);
  ld_check(deps_->getLogID() == log_id_);
}

logid_t EpochRecovery::getLogID() const {
  return log_id_;
}

bool EpochRecovery::digestComplete() const {
  if (finished_digesting_) {
    ld_check(mutation_set_size_ == 0 || mutation_and_cleaning_->isActive());
    ld_check(state_ >= State::MUTATION);
    return true;
  } else {
    ld_check(state_ <= State::DIGEST);
    return false;
  }
}

bool EpochRecovery::onSealed(ShardID from,
                             esn_t lng,
                             esn_t max_seen_esn,
                             const OffsetMap& epoch_size,
                             folly::Optional<TailRecord> tail) {
  ld_debug("SEALED for %s: from %s, lng %u, max_seen %u, "
           "epoch_size %s, tail %s.",
           identify().c_str(),
           from.toString().c_str(),
           lng.val_,
           max_seen_esn.val_,
           epoch_size.toString().c_str(),
           tail.hasValue() ? tail.value().toString().c_str() : "n/a");
  recovery_set_.transition(from, RecoveryNode::State::SEALED);

  // It's ok to update lng_ even if we already sent some START messages and
  // are continuing to build the digest. The digest processing code must
  // ignore digest records with ESNs <= lng_ at the time digest processing
  // starts.
  lng_ = std::max(lng_, lng);
  max_seen_esn_ = std::max(max_seen_esn_, max_seen_esn);

  if (epoch_size.isValid()) {
    epoch_size_map_.max(epoch_size);
  }

  uint64_t last_timestamp = 0;

  if (tail.hasValue() &&
      (!tail_record_from_sealed_.isValid() ||
       tail_record_from_sealed_.header.lsn < tail.value().header.lsn)) {
    ld_check(tail.value().containOffsetWithinEpoch());
    last_timestamp = tail.value().header.timestamp;
    tail_record_from_sealed_ = std::move(tail.value());
  }

  last_timestamp_ = std::max(last_timestamp_, last_timestamp);

  last_timestamp_from_seals_ =
      std::max(last_timestamp_from_seals_, last_timestamp);

  if (active_) {
    return onSealedOrActivated();
  }

  return false;
}

bool EpochRecovery::setNodeAuthoritativeStatus(ShardID shard,
                                               AuthoritativeStatus status) {
  AuthoritativeStatus prev = recovery_set_.getNodeAuthoritativeStatus(shard);
  if (prev == status) {
    return false;
  }

  ld_debug("Changing authoritative status of node %s from %s to %s for %s. "
           "State: {%s}",
           shard.toString().c_str(),
           toString(prev).c_str(),
           toString(status).c_str(),
           identify().c_str(),
           recoveryState().c_str());

  // Determine, based on the node's previous state, if this node could
  // be participating in mutation/cleaning.
  bool node_in_mutation_set =
      recovery_set_.nodeIsInMutationAndCleaningSet(shard);

  recovery_set_.setNodeAuthoritativeStatus(shard, status);

  // Put the node state back to SEALING. Regardless of the new authoritative
  // status, we are going to keep trying sealing the node anyway.
  if (recovery_set_.nodeIsSealed(shard)) {
    recovery_set_.transition(shard, RecoveryNode::State::SEALING);
  }

  // Payloads from this node's digest used by Mutators are freed by this
  // call. However, the Mutators referencing these records will be destroyed
  // by the restart() below since the node must have been in the
  // mutation/cleaning set for its payloads to be used. The reversed order of
  // destruction is safe since Mutators run in our same thread context.
  digest_.removeNode(shard);

  if (mutation_set_size_ > 0) {
    if (node_in_mutation_set) {
      // We are in the mutation and cleaning phase of recovery, and this node is
      // participating. Restart the recovery.
      ld_info("Node %s's authoritative status changed during the mutation "
              "and cleaning phase of recovery of %s. Restarting recovery of "
              "that epoch. State: {%s}",
              shard.toString().c_str(),
              identify().c_str(),
              recoveryState().c_str());
      ld_check(mutation_and_cleaning_->isActive());
      mutation_and_cleaning_->cancel();
      return restart();
    }
  } else if (active_ && status != AuthoritativeStatus::FULLY_AUTHORITATIVE) {
    // This state change may make it possible to start digesting if we were
    // sealing, or start mutating if we were digesting.
    return onSealedOrActivated() || onDigestMayHaveBecomeComplete();
  }
  return false;
}

void EpochRecovery::onDigestRecord(
    ShardID from,
    read_stream_id_t rsid,
    std::unique_ptr<DataRecordOwnsPayload> record) {
  ld_check(record);
  ld_check(record->logid == getLogID());
  ld_check(rsid != READ_STREAM_ID_INVALID);

  lsn_t lsn = record->attrs.lsn;
  RecordID rid = {lsn_to_esn(lsn), lsn_to_epoch(lsn), record->logid};

  if (lsn_to_epoch(lsn) != epoch_) {
    RATELIMIT_INFO(std::chrono::seconds(1),
                   10,
                   "Got an invalid or stale digest record %s from %s for "
                   "read stream %lu. Current active epoch for that log "
                   "recovery is %u. Ignoring.",
                   rid.toString().c_str(),
                   from.toString().c_str(),
                   rsid.val_,
                   epoch_.val_);
    return;
  }

  if (!recovery_set_.digestingReadStream(from, rsid)) {
    RATELIMIT_INFO(std::chrono::seconds(1),
                   10,
                   "Got an invalid or stale digest record %s from %s for "
                   "read stream %lu. Ignoring.",
                   rid.toString().c_str(),
                   from.toString().c_str(),
                   rsid.val_);
    return;
  }

  if (digestComplete()) {
    ld_debug("Got a stale digest record %s from %s for "
             "read stream %lu. EpochRecovery machine for epoch %u is "
             "no longer accepting digest records. Ignoring.",
             rid.toString().c_str(),
             from.toString().c_str(),
             rsid.val_,
             epoch_.val_);
    return;
  }

  if (record->invalid_checksum_) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Got a digest record %s (flags %s) with bad checksum "
                    "from %s for read stream %lu. Current active epoch "
                    "recovery: %s. Reporting as a gap.",
                    rid.toString().c_str(),
                    RECORD_Message::flagsToString(record->flags_).c_str(),
                    from.toString().c_str(),
                    rsid.val_,
                    identify().c_str());

    STAT_INCR(deps_->getStats(), epoch_recovery_digest_checksum_fail);
    // issuing a gap instead of shipping a record with an invalid checksum
    GAP_Header gap_header = {getLogID(),
                             rsid,
                             lsn,
                             lsn,
                             GapReason::CHECKSUM_FAIL,
                             GAP_Header::DIGEST,
                             from.shard()};
    onDigestGap(from, gap_header);
    return;
  }

  digest_.onRecord(from, std::move(record));
  // ownership of record was transferred to digest_

  if (lsn_to_esn(lsn) == ESN_MAX) {
    onDigestStreamEndReached(from);
  }
}

void EpochRecovery::onDigestGap(ShardID from, const GAP_Header& gap) {
  ld_check(gap.flags & GAP_Header::DIGEST);
  ld_check(lsn_to_epoch(gap.start_lsn) == lsn_to_epoch(gap.end_lsn));

  if (!recovery_set_.digestingReadStream(from, gap.read_stream_id)) {
    RATELIMIT_INFO(std::chrono::seconds(1),
                   10,
                   "Got an invalid or stale digest gap %s from %s for "
                   "read stream %lu. Ignoring.",
                   gap.identify().c_str(),
                   from.toString().c_str(),
                   gap.read_stream_id.val_);
    return;
  }

  if (lsn_to_epoch(gap.start_lsn) != epoch_) {
    RATELIMIT_INFO(std::chrono::seconds(1),
                   10,
                   "Got an invalid digest gap %s from %s for "
                   "read stream %lu. The gap must be completely within "
                   "epoch %u, which is currently being recovered."
                   "Ignoring.",
                   gap.identify().c_str(),
                   from.toString().c_str(),
                   gap.read_stream_id.val_,
                   epoch_.val_);
    return;
  }

  if (digestComplete()) {
    ld_debug("Got a stale digest gap %s from %s for "
             "read stream %lu. EpochRecovery machine for %s is "
             "no longer accepting digest gaps. Ignoring.",
             gap.identify().c_str(),
             from.toString().c_str(),
             gap.read_stream_id.val_,
             identify().c_str());
    return;
  }

  // gaps messages are only used to detect the end of the epoch
  if (lsn_to_esn(gap.end_lsn) == ESN_MAX) {
    onDigestStreamEndReached(from);
  }
}

void EpochRecovery::onMutated(ShardID from, const MUTATED_Header& header) {
  ld_check(header.rid.logid == getLogID());

  if (epoch_ != header.rid.epoch) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "PROTOCOL ERROR: Got a MUTATED message %s from %s for an "
                    "invalid epoch. Current active epoch for that log recovery "
                    "is %u. Ignoring",
                    header.rid.toString().c_str(),
                    from.toString().c_str(),
                    epoch_.val_);
    return;
  }

  auto it = mutators_.find(header.rid.esn);
  if (it == mutators_.end()) {
    ld_debug("Got a MUTATED message %s from %s, but mutator for that ESN "
             "doesn't exist. Ignoring.",
             header.rid.toString().c_str(),
             from.toString().c_str());
    return;
  }

  ld_check(it->second != nullptr);
  it->second->onStored(from, header);
}

void EpochRecovery::onStoreSent(ShardID to,
                                const STORE_Header& store,
                                Status status) {
  ld_check(store.rid.logid == getLogID());

  if (epoch_ != store.rid.epoch) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "STORE_Message::onSent() for %s forwarded "
                    "to an EpochRecovery object for a different epoch %u. "
                    "Ignoring.",
                    store.rid.toString().c_str(),
                    epoch_.val_);
    return;
  }

  auto it = mutators_.find(store.rid.esn);
  if (it == mutators_.end()) {
    ld_debug("Mutator for record %s doesn't exist. Ignoring.",
             store.rid.toString().c_str());
    return;
  }

  ld_check(it->second != nullptr);
  it->second->onMessageSent(to, status, store);
}

bool EpochRecovery::onDigestComplete() {
  ld_check(!digestComplete());
  ld_check(!mutation_and_cleaning_->isActive());
  ld_check(!grace_period_->isActive());
  ld_check(state_ == State::DIGEST);

  // make one final effort for checking shards in DIGESTED are eligible for
  // mutation
  auto digested_shards =
      recovery_set_.getNodesInState(RecoveryNode::State::DIGESTED);
  for (const auto& digested_shard : digested_shards) {
    if (deps_->canMutateShard(digested_shard)) {
      recovery_set_.transition(digested_shard, RecoveryNode::State::MUTATABLE);
    }
  }

  if (lng_ != ESN_INVALID && last_timestamp_from_seals_ == 0) {
    // There used to be a bug that would cause this code path to be hit.
    // Now this is just a paranoid check.
    last_timestamp_ = RecordTimestamp::now().toMilliseconds().count();
    ld_error(
        "All SEALED messages contained zero last_timestamp for a nonempty "
        "epoch (recovery: %s). This is unexpected, please investigate. Using "
        "current time instead. Log: %lu, lng: %u.",
        identify().c_str(),
        getLogID().val_,
        lng_.val_);
  }

  // Record the fact that we have selected a mutation set and it won't get
  // changed until this epoch recovery is completed or restarted. This
  // EpochRecovery machine is about to transition into the mutation and
  // cleaning phase. This also prevents nodes that are still in DIGESTING or
  // less to enter DIGESTED, and prevent more nodes to become MUTATABLE.
  finished_digesting_ = true;
  mutation_set_size_ =
      recovery_set_.countShardsInState(RecoveryNode::State::MUTATABLE);
  ld_check(mutation_set_size_ <= recovery_set_.size());

  // Expect to be in one of these states:
  // 1/ We have a non authoritative f-majority digest because too many nodes are
  //    rebuilding. This is an emergency situation during which we need to
  //    finish recovery since rebuilding depends on it. Recovery won't plug
  //    holes in order to ensure DATALOSS gaps are reported by readers, even
  //    though some legit holes may be reported as DATALOSS;
  // 2/ We have a complete authoritative f-majority (all the nodes in the
  //    nodeset either participated or are empty) digest;
  // 3/ We have an incomplete authoritative f-majority digest and we are able to
  //    replicate in the mutation set picked.

  const auto fmajority_result = recovery_set_.fmajorityForNodesInStateOrHigher(
      RecoveryNode::State::DIGESTED);

  ld_check(
      fmajority_result == FmajorityResult::NON_AUTHORITATIVE ||
      fmajority_result == FmajorityResult::AUTHORITATIVE_COMPLETE ||
      (fmajority_result == FmajorityResult::AUTHORITATIVE_INCOMPLETE &&
       recovery_set_.canReplicateNodesInState(RecoveryNode::State::MUTATABLE)));

  startMutationAndCleaningTimer();

  // calculating the digest stage latency
  ld_check_ne(
      last_digesting_start_, std::chrono::steady_clock::time_point::min());
  HISTOGRAM_ADD(deps_->getStats(),
                log_recovery_digesting,
                usec_since(last_digesting_start_));
  last_mutation_start_ = std::chrono::steady_clock::now();

  // A corner case: there are no storage nodes available to participate in
  // recovery, and nothing to recover. Consider recovery done.
  if (mutation_set_size_ == 0) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "All nodes have AuthoritativeStatus equal to "
                    "UNDERREPLICATION or AUTHORITATIVE_EMPTY. There is "
                    "nothing to recover");
    // use the tail record of previous epoch as the new tail
    final_tail_record_ = tail_record_before_this_epoch_;
    advanceLCE();
    // Return false because advanceLCE() should not synchronously destroy `this`
    // because it destroys `this` only if EpochStore::setLastCleanEpoch
    // completes with E::STALE and E::OK which should not hapen synchronously.
    return false;
  }

  return startMutations();
}

bool EpochRecovery::startMutations() {
  // must have picked a mutation set
  ld_check(mutation_set_size_ > 0);
  // Mutation can only happen after digest
  ld_check(state_ == State::DIGEST);

  // DIGEST -> MUTATION
  state_ = State::MUTATION;

  const auto fmajority_result = recovery_set_.fmajorityForNodesInStateOrHigher(
      RecoveryNode::State::DIGESTED);
  ld_check(fmajority_result != FmajorityResult::NONE);

  std::set<ShardID> mutation_set =
      recovery_set_.getNodesInState(RecoveryNode::State::MUTATABLE);
  ld_check(mutation_set.size() <= recovery_set_.size());
  ld_check(mutation_set.size() == mutation_set_size_);

  //// Perform final processing on the Digest received:

  // 1) discard entries whose esn < lng_. For entry == lng (if exist), it is
  // not trimmed until the tail record is decided.
  if (lng_ >= ESN_MIN) {
    digest_.trim(esn_t(lng_.val_ - 1));
  }

  // 2) Filter out record info from the digest belonging to storage shards in
  // the following situations:
  //     a. nodes sent us some digest records but then didn't complete the
  //        digest.
  //     b. nodes finished digested but not eligible for mutation and did not
  //        make it to the mutation set.
  // In both cases, we need to remove these nodes from digest. This is needed to
  // make sure that the mutation will eventually store epoch recovery results
  // r-replicated on mutation set without any conflicts. Therefore it is safe
  // for purging to delete records on nodes not included in the mutation set.
  digest_.filter(mutation_set);

  // 3) finalize the digest by
  //    a. applying bridge record received and compute the bridge esn for the
  //       epoch. The tail record of the epoch is also determined.
  //    b. recompute offsets within the epoch

  bridge_esn_ = digest_.applyBridgeRecords(lng_, &tail_esn_);
  ld_check(bridge_esn_ >= lng_ || bridge_esn_ == ESN_INVALID);
  ld_check(tail_esn_ >= lng_ || tail_esn_ == ESN_INVALID);

  int rv;
  if (lng_ == ESN_INVALID || lng_ >= digest_start_esn_) {
    // lng is zero or in the digest, Digest can compute OffsetMap to the end of
    // epoch by itself
    rv = digest_.recomputeOffsetsWithinEpoch(lng_);
  } else {
    // lng must be exactly one esn before the digest start esn
    ld_check(tail_record_from_sealed_.isValid());
    ld_check(tail_record_from_sealed_.containOffsetWithinEpoch());
    ld_check(digest_start_esn_ > ESN_INVALID);
    ld_check(lsn_to_esn(tail_record_from_sealed_.header.lsn).val_ ==
             digest_start_esn_.val_ - 1);
    rv = digest_.recomputeOffsetsWithinEpoch(
        lsn_to_esn(tail_record_from_sealed_.header.lsn),
        tail_record_from_sealed_.offsets_map_);
  }

  if (rv != 0 && deps_->getSettings().byte_offsets) {
    // only bump the counter if byte_offset feature enable in settings
    STAT_INCR(deps_->getStats(), recompute_byteoffset_failed);
  }

  updateEpochTailRecord();
  ld_check(final_tail_record_.isValid());
  ld_check(!final_tail_record_.containOffsetWithinEpoch());

  // 4) trim entries whose esn == lng_ finally
  digest_.trim(lng_);

  ld_info("Log %lu epoch %u final digest before mutation: consensus LNG %u, "
          "start esn: %u, %lu entries, first esn: %u, last esn: %u, "
          "bridge esn: %u, tail esn: %u, mutation set fmajority results: %s, "
          "mutation set: %s, last timestamp: %s, last timestamp from "
          "seals: %s. Tail record before this epoch: %s, Tail record from "
          "sealed: %s, final tail record: %s. Recovery set state when mutation "
          "started: {%s}.",
          log_id_.val_,
          epoch_.val_,
          lng_.val_,
          digest_start_esn_.val_,
          digest_.size(),
          digest_.getFirstEsn().val_,
          digest_.getLastEsn().val_,
          bridge_esn_.val_,
          tail_esn_.val_,
          fmajorityResultString(fmajority_result),
          toString(mutation_set).c_str(),
          format_time(std::chrono::milliseconds(last_timestamp_)).c_str(),
          format_time(std::chrono::milliseconds(last_timestamp_from_seals_))
              .c_str(),
          tail_record_before_this_epoch_.toString().c_str(),
          tail_record_from_sealed_.toString().c_str(),
          final_tail_record_.toString().c_str(),
          recoveryState().c_str());

  if (bridge_esn_ == ESN_INVALID && digest_.empty()) {
    // there is nothing to mutated because:
    // 1) digest is empty; AND
    // 2) there is no need to insert bridge record (e.g., last record is
    //    ESN_MAX or epoch is also empty)
    advanceToCleaning();
    return false;
  }

  return mutateEpoch(mutation_set, fmajority_result);
}

void EpochRecovery::updateEpochTailRecord() {
  // must have finished the digest phase
  ld_check(mutation_set_size_ > 0);
  ld_check(digest_start_esn_ != ESN_INVALID);
  ld_check(tail_record_before_this_epoch_.isValid());
  ld_check(!tail_record_before_this_epoch_.containOffsetWithinEpoch());

  if (tail_esn_ == ESN_INVALID) {
    // epoch is empty, tail record of the previous epoch gets carried over
    final_tail_record_ = tail_record_before_this_epoch_;
    return;
  }

  auto gen_tail_record_for_dataloss = [&]() {
    uint32_t flags = TailRecordHeader::CHECKSUM_PARITY | TailRecordHeader::GAP |
        TailRecordHeader::OFFSET_WITHIN_EPOCH;
    // TODO (T35832374) : remove if condition when all servers support OffsetMap
    if (deps_->getSettings().enable_offset_map) {
      flags |= TailRecordHeader::OFFSET_MAP;
    }
    return TailRecord(
        {getLogID(),
         compose_lsn(epoch_, lng_),
         /*timestamp*/ 0,
         {BYTE_OFFSET_INVALID /* deprecated, OffsetMap used instead*/},
         flags,
         {}},
        OffsetMap(),
        std::shared_ptr<PayloadHolder>());
  };

  if (tail_esn_ < digest_start_esn_) {
    // tail record is outside of the digest, this can only happen when
    // tail record is already fetched in the seal phase and it is the same as
    // lng so digest is started at lng + 1
    if (!tail_record_from_sealed_.isValid() ||
        tail_record_from_sealed_.header.lsn != compose_lsn(epoch_, tail_esn_)) {
      RATELIMIT_CRITICAL(
          std::chrono::seconds(10),
          10,
          "INTERNAL ERROR: tail record at esn %u for %s is "
          "smaller than digest start esn %u (lng %u) and it is "
          "different from tail record (%s) collected in SEALED "
          "replies!",
          tail_esn_.val_,
          identify().c_str(),
          digest_start_esn_.val_,
          lng_.val_,
          lsn_to_string(tail_record_from_sealed_.header.lsn).c_str());
      ld_check(false);
      return;
    }

    // tail record is the one collected in SEALED replies
    final_tail_record_ = tail_record_from_sealed_;
  } else {
    // tail record is in the digest, find the entry and construct a tail record
    // from it
    const Digest::Entry* tail_entry = digest_.findEntry(tail_esn_);
    if (tail_entry == nullptr) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      10,
                      "Potential DATALOSS alert: tail record at esn %u for "
                      "%s not found in digest entries! digest start esn %u, "
                      "lng %u.",
                      tail_esn_.val_,
                      identify().c_str(),
                      digest_start_esn_.val_,
                      lng_.val_);

      final_tail_record_ = gen_tail_record_for_dataloss();
      STAT_INCR(deps_->getStats(), epoch_recovery_tail_record_not_in_digest);
    } else {
      if (tail_entry->isHolePlug()) {
        RATELIMIT_CRITICAL(std::chrono::seconds(10),
                           10,
                           "INTERNAL ERROR: Tail record at esn %u for %s "
                           "is a hole/bridge in digest entries! digest start "
                           "esn %u, lng %u. This could be caused by a bug or "
                           "data corruption.",
                           tail_esn_.val_,
                           identify().c_str(),
                           digest_start_esn_.val_,
                           lng_.val_);
        dd_assert(false, "tail record is a hole plug");
        // consider it a dataloss at the tail
        final_tail_record_ = gen_tail_record_for_dataloss();
        STAT_INCR(deps_->getStats(), epoch_recovery_tail_record_hole_plug);
      } else {
        OffsetMap offsets_within_epoch;
        if ((tail_entry->record->flags_ &
             RECORD_Header::INCLUDE_OFFSET_WITHIN_EPOCH) &&
            tail_entry->record->extra_metadata_ != nullptr) {
          offsets_within_epoch =
              tail_entry->record->extra_metadata_->offsets_within_epoch;
        }

        TailRecordHeader::flags_t flags = TailRecordHeader::OFFSET_WITHIN_EPOCH;
        // TODO (T35832374) : remove if condition when all servers support
        // OffsetMap
        if (deps_->getSettings().enable_offset_map) {
          flags |= TailRecordHeader::OFFSET_MAP;
        }
        std::shared_ptr<PayloadHolder> ph;
        if (tail_optimized_) {
          // TODO T9899761: use PayloadHolder when deserializing RECORD to avoid
          // this copy
          auto dup_payload = tail_entry->getPayload().dup();
          ph = std::make_shared<PayloadHolder>(
              dup_payload.data(), dup_payload.size());
          flags |= TailRecordHeader::HAS_PAYLOAD;
          // TODO T31241526: support checksum flags in tail record
          flags |= TailRecordHeader::CHECKSUM_PARITY;
        } else {
          flags |= TailRecordHeader::CHECKSUM_PARITY;
        }
        final_tail_record_.reset(
            {tail_entry->record->logid,
             tail_entry->record->attrs.lsn,
             static_cast<uint64_t>(tail_entry->record->attrs.timestamp.count()),
             {BYTE_OFFSET_INVALID /* deprecated, offsets_within_epoch used instead */},
             flags,
             {}},
            std::move(ph),
            std::move(offsets_within_epoch));
      }
    }
  }

  ld_check(final_tail_record_.containOffsetWithinEpoch());

  // if the tail record happen to have an offset within epoch associated with
  // it, use that as the epoch size, otherwise, use the maximum epoch size
  // reported in SEALED replies as an approximation
  if (!final_tail_record_.offsets_map_.isValid()) {
    if (epoch_size_map_.isValid()) {
      final_tail_record_.offsets_map_ = epoch_size_map_;
    } else {
      final_tail_record_.offsets_map_ = OffsetMap::fromLegacy(0);
    }
  }

  // the epoch_size_map_ got from sealed reply may not be accurate,
  // we need to correct epoch_size_map_ to be the same as the one in
  // final_tail_record_, so that later CLEAN message can contain the correct
  // OffsetMap
  epoch_size_map_ = final_tail_record_.offsets_map_;

  // the finalized tail record should contain byte offset instead of offset
  // within epoch. Its byte offset should be the sum of the epoch size (offset
  // within epoch of the tail record) and the accumulative byte offset from the
  // previous epoch
  auto& prev_epoch_offsets_map = tail_record_before_this_epoch_.offsets_map_;
  final_tail_record_.offsets_map_ = OffsetMap::mergeOffsets(
      std::move(final_tail_record_.offsets_map_), prev_epoch_offsets_map);
  final_tail_record_.header.flags &= ~TailRecordHeader::OFFSET_WITHIN_EPOCH;
  ld_check(!final_tail_record_.containOffsetWithinEpoch());

  if (!tail_optimized_) {
    // do not keep the payload if the log is not tail optimized
    final_tail_record_.removePayload();
  }
}

bool EpochRecovery::mutateEpoch(const std::set<ShardID>& mutation_set,
                                FmajorityResult fmajority_result) {
  /// properties of the mutation set

  // If false, we are in emergency non-authoritative mode because too many
  // nodes are rebuilding, we need to finish recovery because rebuilding depends
  // on it. We may also not have enough nodes to fully re-replicate. Holes and
  // bridge record won't be plugged in such mode.
  const bool is_authoritative =
      fmajority_result != FmajorityResult::NON_AUTHORITATIVE;

  // If false, we may have an authoritative set but not enough nodes are
  // available for full replication. This can happen if there are too many EMPTY
  // or REBUILDING nodes.
  const bool can_fully_replicate =
      recovery_set_.canReplicateNodesInState(RecoveryNode::State::MUTATABLE);

  // does the digest contain all fully-authoritative nodes in the recovery set?
  // note here we take advantage of the fact that the digest set is already
  // fmajority in the mutation phase
  const bool is_complete_set =
      (fmajority_result != FmajorityResult::NONE &&
       fmajority_result != FmajorityResult::AUTHORITATIVE_INCOMPLETE);

  /// ESN range to be mutated: [LNG + 1, max(bridge_esn_, digest_.getLastEsn())]

  // otherwise we wouldn't be here but directly advance to digesting
  ld_check(lng_ < ESN_MAX);
  const esn_t mutation_first = esn_t(lng_.val_ + 1);
  const esn_t mutation_last = std::max(bridge_esn_, digest_.getLastEsn());

  // Determine timestamp for records to be mutated. For the majority of the
  // cases, existing timestamps are used for all mutations. If some ESN is
  // missing from the digest, we'll use the timestamp of a record preceding it
  // (or, if we need to plug some holes before digest_.begin(), timestamp of
  // the first record in the digest is used). This is done to ensure that
  // timestamps are monotonically increasing. The other case is that the
  // digest collected is empty but we need to plug a bridge record. In such
  // case, we use the last_timestamp_ collected in the seal phase as the
  // timestamp of the bridge record.
  uint64_t mutation_timestamp = digest_.empty()
      ? last_timestamp_
      : digest_.begin()->second.record->attrs.timestamp.count();

  esn_t cur = mutation_first;
  Digest::iterator it = digest_.begin();
  // note that it may move forward as cur advances in the loop.
  // upcast to 64-bit integer to avoid overflows
  for (uint64_t cur_esn = static_cast<uint64_t>(mutation_first.val_);
       cur_esn <= static_cast<uint64_t>(mutation_last.val_);
       ++cur_esn) {
    ld_check(cur_esn <= ESN_MAX.val_);
    cur = esn_t(static_cast<esn_t::raw_type>(cur_esn));

    // TODO T31241526: add support for checksums
    STORE_flags_t mutation_flags =
        (STORE_Header::RECOVERY | STORE_Header::WRITTEN_BY_RECOVERY |
         STORE_Header::CHECKSUM_PARITY);
    if (cur == bridge_esn_ && is_authoritative) {
      // Important: do not plug bridge record if digest is non-authoritative.
      // We do not want to hide true data losses.
      mutation_flags |= STORE_Header::BRIDGE;
    }

    DataRecordOwnsPayload* record = nullptr;
    Payload payload;
    std::set<ShardID> successfully_stored;
    std::set<ShardID> amend_metadata;
    std::set<ShardID> conflict_copies;

    if (it == digest_.end() || it->first > cur) {
      // if the current esn past the end of digest, it must be a bridge record.
      ld_check(it != digest_.end() || cur == bridge_esn_);

      // esn is not in digest, insert a hole to plug it
      ld_debug("esn %u needs a hole to be plugged for %s.",
               cur.val_,
               identify().c_str());

      if (!is_authoritative) {
        // The digest is not authoritative. We cannot tell whether the LSN was
        // acknowledged to the writer as accepted for delivery. Do not plug the
        // hole. Readers will report data loss.
        // Note: Here we do not even plug the bridge record because plugging
        // it may hide true data loss.
        STAT_INCR(deps_->getStats(), num_holes_not_plugged);
        continue;
      }

      // Digest is authoritative for this hole. Store hole plugs. No amend
      // or overwrites needs to be done.
      mutation_flags |= STORE_Header::HOLE;
      // empty payload
      ld_check(payload.data() == nullptr && payload.size() == 0);
      ++num_holes_plugged_;

      if (cur != bridge_esn_) {
        // exclude bridge gaps for hole plugs
        STAT_INCR(deps_->getStats(), num_hole_plugs);
      }
    } else if (it->first == cur) {
      // there is a digest entry with ESN == cur, perform mutation according to
      // the entry
      Digest::Entry& dentry = it->second;
      ld_check(dentry.record != nullptr);
      record = dentry.record.get();

      // adjust mutation timestamp
      mutation_timestamp = dentry.record->attrs.timestamp.count();
      if (!digest_.needsMutation(dentry,
                                 &successfully_stored,
                                 &amend_metadata,
                                 &conflict_copies,
                                 is_complete_set)) {
        ld_debug("esn %u does not need mutation for %s.",
                 cur.val_,
                 identify().c_str());
        ++it;
        continue;
      }

      ld_debug("esn %u needs mutation for %s.", cur.val_, identify().c_str());

      if (dentry.isHolePlug()) {
        mutation_flags |= STORE_Header::HOLE;
        ld_check(payload.data() == nullptr &&
                 payload.size() == 0); // empty payload

        // plug needs to be stored on all storage nodes that contain a data
        // record for this ESN
        conflict_copies.size() > 0 ? ++num_holes_conflict_
                                   : ++num_holes_re_replicate_;
      } else {
        // this is a normal record, can never be a hole or bridge
        ld_check(!(mutation_flags & STORE_Header::HOLE));
        ld_check(!(mutation_flags & STORE_Header::BRIDGE));
        payload = dentry.getPayload();

        // note that with failure domain requirements, existing copyset size may
        // be equal or larger than replication factor but the digest entry still
        // need mutation
        ++num_records_re_replicate_;
      }
      ++it;
    } else { // it->first < cur
      // it->first should never behind cur as cur are contiguous, keep
      // increasing and digest are ordered and trimmed using lng_
      ld_check(false);
    }

    if (cur == bridge_esn_) {
      STAT_INCR(deps_->getStats(), num_bridge_records);
    }

    if (!can_fully_replicate) {
      // nodes in the mutation set cannot fully replicate the record, Mutator
      // will do best-of-effort re-replication. This will cause
      // underreplication, bump stats to keep track of that.
      if (MetaDataLog::isMetaDataLog(getLogID())) {
        STAT_INCR(deps_->getStats(),
                  epoch_recovery_record_underreplication_metadatalog);
      } else {
        STAT_INCR(
            deps_->getStats(), epoch_recovery_record_underreplication_datalog);
      }

      RATELIMIT_WARNING(
          std::chrono::seconds(10),
          10,
          "Epoch recovery %s doesn't have enough nodes to replicate mutation "
          "results but it already got all FULLY_AUTHORITATIVE shards. This is "
          "best it can do, moving forward with potential under-replicated "
          "records. Recovery set state: {%s}.",
          identify().c_str(),
          recoveryState().c_str());
    }

    // creating a mutator for the esn

    // Note that here we actaully ignore all nodes in successfully stored and
    // instead treat them as records that needs to be amended. The reason is
    // that we want to guarantee a R-replicated copyset so that rebuilding
    // can work correctly. Specifically, even for nodes that are in
    // "successfully_stored", which is considered OK for epoch recovery, the
    // records may still have a different copyset from what we are going to
    // store. This is bad for rebuilding because of the copyset divergence.
    // So instead, we just amend these nodes anyway.
    for (ShardID n : successfully_stored) {
      amend_metadata.insert(n);
    }
    successfully_stored.clear();

    StorageSet mutation_nodeset;
    mutation_nodeset.reserve(mutation_set.size());
    for (const auto& shard : mutation_set) {
      mutation_nodeset.push_back(shard);
    }

    auto mutation_header =
        createMutationHeader(cur, mutation_timestamp, mutation_flags, record);
    auto res =
        mutators_.emplace(cur,
                          deps_->createMutator(mutation_header.first,
                                               mutation_header.second,
                                               payload,
                                               std::move(mutation_nodeset),
                                               replication_,
                                               std::move(amend_metadata),
                                               std::move(conflict_copies),
                                               this));
    ld_check(res.second);
  }

  if (mutators_.size() == 0) {
    // no mutations needed
    advanceToCleaning();
    return false;
  }

  // Tell the first few and last few mutators to report more debug information
  // when done.
  {
    size_t i = 0;
    for (auto jt = mutators_.begin(); jt != mutators_.end() && i < 2;
         ++jt, ++i) {
      jt->second->printDebugTraceWhenComplete();
    }
    i = 0;
    for (auto jt = mutators_.rbegin(); jt != mutators_.rend() && i < 2;
         ++jt, ++i) {
      jt->second->printDebugTraceWhenComplete();
    }
  }

  const recovery_id_t recovery_id = id_;
  // start all mutators, note that it is possible some may synchronously finish
  for (auto mit = mutators_.begin(); mit != mutators_.end();) {
    ld_check(mit->second != nullptr);
    auto& mutator = *mit->second;
    mit++;
    mutator.start();
    STAT_INCR(deps_->getStats(), mutation_started);
    // Mutator can synchronously restart this EpochRecovery (for example, if
    // one of the nodes in the mutation set is now no longer in the config),
    // thus destroying the current digest and invalidating `mit'.
    if (id_ != recovery_id) {
      RATELIMIT_INFO(std::chrono::seconds(1),
                     10,
                     "Aborting mutations for epoch %u of log %lu because "
                     "the EpochRecovery state machine was restarted.",
                     epoch_.val_,
                     getLogID().val_);
      return true;
    }
  }

  return false;
}

std::pair<STORE_Header, STORE_Extra>
EpochRecovery::createMutationHeader(esn_t esn,
                                    uint64_t timestamp,
                                    STORE_flags_t flags,
                                    DataRecordOwnsPayload* record) const {
  OffsetMap offsets_within_epoch;
  NodeID my_node_id = deps_->getMyNodeID();

  if (record != nullptr) {
    if (record->flags_ & RECORD_Header::BUFFERED_WRITER_BLOB) {
      flags |= STORE_Header::BUFFERED_WRITER_BLOB;
    }
    if (record->flags_ & RECORD_Header::INCLUDE_OFFSET_WITHIN_EPOCH) {
      flags |= STORE_Header::OFFSET_WITHIN_EPOCH;
      // TODO (T35832374) : remove if condition when all servers support
      // OffsetMap
      if (deps_->getSettings().enable_offset_map) {
        flags |= STORE_Header::OFFSET_MAP;
      }
      if (record->extra_metadata_) {
        offsets_within_epoch = record->extra_metadata_->offsets_within_epoch;
      }
    }
  }

  STORE_Extra extra;
  extra.recovery_id = id_;
  extra.recovery_epoch = deps_->getSealEpoch();
  extra.offsets_within_epoch = std::move(offsets_within_epoch);

  return std::make_pair(
      STORE_Header{
          RecordID(esn, epoch_, log_id_),
          timestamp,
          lng_,
          0, // wave will be overwritten by Mutator
          flags,
          0, // nsync, not used
          0, // copyset_offset will be overwritten by Mutator
          0, // copyset_size will be overwritten by Mutator
          static_cast<uint32_t>(
              std::chrono::duration_cast<std::chrono::milliseconds>(
                  deps_->getSettings().recovery_timeout)
                  .count()), // mutation timeout
          my_node_id,        // sequencer node id
      },
      std::move(extra));
}

bool EpochRecovery::restart() {
  // restart() is expected to only be called in the mutation and
  // cleaning phase of recovery
  ld_check_gt(mutation_set_size_, 0);
  ld_check(finished_digesting_);
  ld_check_in(state_, ({State::MUTATION, State::CLEAN, State::ADVANCE_LCE}));

  ld_check(!grace_period_->isActive());
  ld_check(!mutation_and_cleaning_->isActive());

  id_ = recovery_id_t(next_id++);

  state_ = State::SEAL_OR_INACTIVE;
  digest_start_esn_ = ESN_INVALID;
  bridge_esn_ = ESN_INVALID;
  tail_esn_ = ESN_INVALID;
  final_tail_record_.reset();
  mutators_.clear();
  digest_.clear();
  absent_nodes_.clear();
  mutation_set_size_ = 0;
  finished_digesting_ = false;

  // Note: STOP messages are sent to nodes in DIGESTING to terminate
  // digest read streams
  recovery_set_.resetToSealed();

  // reset debug stats
  last_restart_timestamp_ = std::chrono::system_clock::now().time_since_epoch();
  last_digesting_start_ = last_mutation_start_ = last_cleaning_start_ =
      std::chrono::steady_clock::time_point::min();

  num_holes_plugged_ = 0;
  num_holes_re_replicate_ = 0;
  num_holes_conflict_ = 0;
  num_records_re_replicate_ = 0;
  ++num_restarts_;

  return onSealedOrActivated();
}

void EpochRecovery::startMutationAndCleaningTimer() {
  std::chrono::milliseconds recovery_timeout{
      deps_->getSettings().recovery_timeout};

  ld_check(recovery_timeout > std::chrono::milliseconds::zero());
  mutation_and_cleaning_->activate(recovery_timeout);
}

void EpochRecovery::onTimeout() {
  ld_check(!grace_period_->isActive());
  // timer_ is started when grace_period_ fires or right after it was cancelled

  int n_clean = recovery_set_.countShardsInState(RecoveryNode::State::CLEAN);

  if (n_clean < mutation_set_size_) {
    STAT_INCR(deps_->getStats(), recovery_mutation_and_cleaning_timeouts);

    auto describe_mutators = [&] {
      std::stringstream ss;
      ss << "{";
      bool first = true;
      for (auto& m : mutators_) {
        if (!first) {
          ss << ", ";
        }
        first = false;
        ss << "e" << m.first.val() << ": {" << m.second->describeState() << "}";
      }
      ss << "}";
      return ss.str();
    };

    ld_warning("Recovery timeout expired for %s. Restarting recovery "
               "for that epoch. State: %s, epoch recovery start: %s, last "
               "restart: %s, nodes: %s, running mutators: %s",
               identify().c_str(),
               toString(state_).c_str(),
               RecordTimestamp(creation_timestamp_).toString().c_str(),
               RecordTimestamp(last_restart_timestamp_).toString().c_str(),
               recoveryState().c_str(),
               describe_mutators().c_str());

    restart();
  } else {
    ld_warning("Recovery timeout expired for %s. Epoch is fully recovered. "
               "Will try again to update the last clean epoch value for log "
               "in the epoch store",
               identify().c_str());
    // must be already in ADVANCE_LCE state
    ld_check(state_ == State::ADVANCE_LCE);
    startMutationAndCleaningTimer();
    advanceLCE();
  }
}

void EpochRecovery::onNodeStateChanged(ShardID shard, RecoveryNode::State to) {
  recovery_set_.onNodeStateChanged(shard, to);
}

void EpochRecovery::activate(const TailRecord& prev_tail_record) {
  ld_check(!active_);

  active_ = true;

  ld_check(prev_tail_record.isValid());
  ld_check(!prev_tail_record.containOffsetWithinEpoch());

  tail_record_before_this_epoch_ = prev_tail_record;
  last_timestamp_ =
      std::max(last_timestamp_, prev_tail_record.header.timestamp);

  activation_time_ = std::chrono::steady_clock::now();

  onSealedOrActivated();
}

std::string EpochRecovery::identify() const {
  return "epoch " + folly::to<std::string>(epoch_.val_) + " of log " +
      folly::to<std::string>(log_id_.val_);
}

bool EpochRecovery::onSealedOrActivated() {
  ld_check(active_);

  // Digest threshold is the number of sealed nodes at which we begin
  // sending START messages requesting records and gaps for the
  // digest, between lng_ and the end of epoch. The lower we set it,
  // the more records we may potentially receive and have to add to
  // the digest. The higher we set it, the longer we may have to wait
  // before asking for the digest records. The smallest possible value
  // is 1.  The largest is N, the size of nodeset. The minimum number
  // of nodes we need in order to complete recovery seems like a good
  // compromise. To complete recovery, the subset of nodes must both
  // be a `f-majority' subset and be able to replicate records/plugs
  // satisfying the replication requirements for the epoch.

  // We have to take into account nodes in states higher than SEALED
  // when deciding whether to request digest streams from those
  // currently in SEALED. This is because the sealing of some nodes
  // may be delayed until after an fmajority gets sealed, transitions
  // to DIGESTING, and we start a grace period. While in grace period
  // we may have fewer than digest_threshold nodes in SEALED (most
  // nodes are in DIGESTING), yet if some straggeler gets sealed in that
  // period, we still want to start receiving a digest stream from it.

  const auto fmajority_sealed = recovery_set_.fmajorityForNodesInStateOrHigher(
      RecoveryNode::State::SEALED);

  if (fmajority_sealed == FmajorityResult::NONE) {
    // We need more nodes to chime in.
    return false;
  }

  // We should start building the digest if:
  // * there are no more nodes we can read from (AUTHORITATIVE_COMPLETE or
  //   NON_AUTHORITATIVE), OR
  // * the subset of nodes is authoritative for the f-majority property, and
  //   they satisfy the requirement for replicating a record/hole for mutation.
  const bool can_build_digest =
      fmajority_sealed != FmajorityResult::AUTHORITATIVE_INCOMPLETE ||
      recovery_set_.canReplicateNodesInStateOrHigher(
          RecoveryNode::State::SEALED);

  if (!can_build_digest || digestComplete()) {
    // do nothing if too few nodes have been sealed, or if we are done
    // building the digest.
    return false;
  }

  if (digest_start_esn_ == ESN_INVALID) {
    // digest has never been started for this round of epoch recovery, start
    // the digest by first determining the digest_start_esn_

    // SEAL_OR_INACTIVE -> DIGEST
    ld_check(state_ == State::SEAL_OR_INACTIVE);
    state_ = State::DIGEST;

    // starting a new round of digest
    digest_start_esn_ = computeDigestStart();
    digest_.setDigestStartEsn(digest_start_esn_);
    ld_check(digest_start_esn_ != ESN_INVALID);

    if (last_digesting_start_ == std::chrono::steady_clock::time_point::min()) {
      last_digesting_start_ = std::chrono::steady_clock::now();
    }
  } else {
    // digest has already been started on some nodes, use the same
    // digest_start_esn
    ld_check(state_ == State::DIGEST);
  }

  // start digesting on nodes in SEALED
  int n_matching __attribute__((__unused__)) =
      recovery_set_.advanceMatching(RecoveryNode::State::DIGESTING);

  return onDigestMayHaveBecomeComplete();
}

esn_t EpochRecovery::computeDigestStart() const {
  if (lng_ == ESN_INVALID) {
    return ESN_MIN;
  }

  if (!tail_record_from_sealed_.isValid() ||
      tail_record_from_sealed_.header.lsn < compose_lsn(epoch_, lng_) ||
      (tail_optimized_ && !tail_record_from_sealed_.hasPayload())) {
    // we don't have a tail record at lng in the seal phase, or do not have
    // the payload of tail record for a tail optimized log, need to fetch the
    // tail record in the digest phase, starting with the last known good esn
    return lng_;
  }

  // we already have the tail record at lng_, start digesting from
  // lng_ + 1
  return lng_ == ESN_MAX ? ESN_MAX : esn_t(lng_.val_ + 1);
}

void EpochRecovery::onDigestStreamStarted(ShardID from,
                                          read_stream_id_t rsid,
                                          lsn_t last_known_good_lsn,
                                          Status status) {
  ld_assert(recovery_set_.digestingReadStream(from, rsid));

  if (status == Status::OK) {
    ld_debug("Started digest read stream %lu to %s for %s, optional LNG %s.",
             rsid.val_,
             from.toString().c_str(),
             identify().c_str(),
             lsn_to_string(last_known_good_lsn).c_str());

    if (last_known_good_lsn != LSN_INVALID) {
      if (lsn_to_epoch(last_known_good_lsn) != epoch_) {
        RATELIMIT_ERROR(std::chrono::seconds(10),
                        10,
                        "Received LNG update %s from %s but it is in a "
                        "different epoch for %s.",
                        lsn_to_string(last_known_good_lsn).c_str(),
                        from.toString().c_str(),
                        identify().c_str());
      } else {
        esn_t lng_received = lsn_to_esn(last_known_good_lsn);
        if (lng_received > lng_) {
          ld_debug("Updating lng from %u to %u by receiving STARTED from %s "
                   "for %s.",
                   lng_.val_,
                   lng_received.val_,
                   from.toString().c_str(),
                   identify().c_str());
          lng_ = lng_received;
          STAT_INCR(deps_->getStats(), lng_update_digest_started);
        }
      }
    }

    return;
  }

  // This node should be known as FULLY_AUTHORITATIVE. Indeed, we are here
  // because we could seal this node. If a node's authoritative state becomes !=
  // FULLY_AUTHORITATIVE, the node is put back into SEALING state.
  ld_assert(recovery_set_.getNodeAuthoritativeStatus(from) ==
            AuthoritativeStatus::FULLY_AUTHORITATIVE);

  if (status == Status::REBUILDING) {
    // We are probably lagging behind reading the event log. Hopefully we can
    // complete recovery without this node. Worst case, recovery will stall
    // until we catch up reading the event log.
    RATELIMIT_WARNING(
        std::chrono::seconds(1),
        10,
        "Node %s sent a STARTED message for %s with "
        "E::REBUILDING but event log says the node is FULLY_AUTHORITATIVE. "
        "Ignoring this reply. We will keep retrying until what the node says "
        "matches what the event log says.",
        from.toString().c_str(),
        identify().c_str());
    return;
  }

  ld_info("Failed to start digest read stream %lu to %s for %s: %s. ",
          rsid.val_,
          from.toString().c_str(),
          identify().c_str(),
          error_description(status));

  if (status == E::AGAIN) {
    // Transient error. Resend START.
    recovery_set_.resendMessage(from);
  }
}

void EpochRecovery::onDigestStreamEndReached(ShardID from) {
  // this function is only called when we get a digest RECORD or GAP
  // If digest is complete, we must ignore that RECORD or GAP message.
  ld_check(!digestComplete());

  recovery_set_.transition(from, RecoveryNode::State::DIGESTED);

  if (deps_->canMutateShard(from)) {
    // If the node is considered as eligible for mutation at this point of time,
    // transition its state to State::MUTATABLE so that it will be included in
    // the mutation and cleaning set. If our local view is stale and the node
    // is not mutatable, the mutation will be rejected in later stage and if
    // epoch recovery has to mutate on the node, it will get stuck and restarted
    // after a timeout.
    recovery_set_.transition(from, RecoveryNode::State::MUTATABLE);
  }

  onDigestMayHaveBecomeComplete();
}

/**
 * RecoverySet::fmajorityForNodesInState() may return:
 *
 * - NONE: we need more names to participare;
 * - NON_AUTHORITATIVE: we heard from all the non rebuilding and non empty nodes
 *   and too many nodes are rebuilding. The digest is complete but recovery will
 *   be non authoritative ie it won;t plug holes because these holes may be for
 *   records that were acknowledged as appended;
 * - AUTHORITATIVE_COMPLETE: We heard from all the non empty non rebuilding
 *   nodes and we have an authoritative f-majority. Recovery will plus holes.
 * - AUTHORITATIVE_INCOMPLETE: We heard from an f-majority of non empty nodes.
 *   When we get this result, we activate a timer to give a bit more time for
 *   more nodes to chime in, which will reduce the amount of gap/copy conflicts.
 *   Mutation will start when the grace timer expires or all nodes chime in
 *   (AUTHORITATIVE_COMPLETE).
 */
bool EpochRecovery::onDigestMayHaveBecomeComplete(bool grace_period_expired) {
  ld_check(active_);
  if (digestComplete()) {
    // onDigestComplete() was already called, nothing to do here.
    return false;
  }

  // note: this function might get called even in SEAL state, e.g., on
  // authoritative status changes
  ld_check(state_ <= State::DIGEST);

  // checking if the subset of shards that completed digest (i.e., state in
  // {DIGESTED, MUTATABLE} satisfies f-majority property of the epoch
  const auto fmajority_result = recovery_set_.fmajorityForNodesInStateOrHigher(
      RecoveryNode::State::DIGESTED);

  switch (fmajority_result) {
    case FmajorityResult::NONE:
      grace_period_->cancel();
      return false;
    case FmajorityResult::NON_AUTHORITATIVE:
      // Too many nodes are rebuilding, we'll do a non-authoritative recovery,
      // i.e. we won't plug holes.
      RATELIMIT_INFO(
          std::chrono::seconds(10),
          10,
          "Doing a non-authoritative recovery of %s because %lu nodes are "
          "rebuilding. This recovery won't plug holes. State: {%s}",
          identify().c_str(),
          recovery_set_.numShardsInAuthoritativeStatus(
              AuthoritativeStatus::UNDERREPLICATION),
          recoveryState().c_str());
      STAT_INCR(deps_->getStats(), non_auth_recovery_epochs);
      FOLLY_FALLTHROUGH;
    case FmajorityResult::AUTHORITATIVE_COMPLETE:
      ld_debug("Got a full recovery digest for %s. State: {%s}",
               identify().c_str(),
               recoveryState().c_str());
      grace_period_->cancel();

      // NOTE: epoch recovery has already completed digest for all
      // FULLY_AUTHORITATIVE storage shards and this is the best it can do.
      // But it is possible that the set of MUTATABLE nodes cannot satisfy the
      // replication property. This might be caused by unsafe operations such
      // as draining too many nodes or removing too many nodes from the config.
      // In such case epoch recovery will move forward to perform best effort
      // replication to all remaining shards and accept the under-replication.
      // see comments of can_fully_replicate in MutateEpoch().
      return onDigestComplete();
    case FmajorityResult::AUTHORITATIVE_INCOMPLETE: {
      // We have built a minimal digest based on just an F-majority of
      // nodes in the epoch's nodeset.

      if (!recovery_set_.canReplicateNodesInState(
              RecoveryNode::State::MUTATABLE)) {
        // We don't have enough shards to replicate to in the current mutation
        // set candidates. Let's wait for more shards to chime in.
        RATELIMIT_INFO(std::chrono::seconds(10),
                       10,
                       "We don't have enough shards to replicate to for "
                       "recovery of %s. Waiting for more shards to chime in. "
                       "State: {%s}",
                       identify().c_str(),
                       recoveryState().c_str());
        grace_period_->cancel();
        return false;
      }

      // Digest is from authoritative f-majority and mutatable nodes contains
      // a valid copyset. We can be sure that no holes in it have been
      // acknowledged to the writers as accepted for delivery, given that
      // there is no silent under-replication. We can start mutations now,
      // but we will get fewer gap/copy conflicts if we wait for the rest of
      // the nodeset to send us their digest records during the grace period.
      // This will also reduce the chance of epoch recovery getting wrong
      // results in case of silent under-replication, as a best effort
      // mitigation.

      if (grace_period_->isActive()) {
        // already in a grace period
        ld_check(!grace_period_expired);
        ld_debug("Grace period timer for %s is already active. State: {%s}",
                 identify().c_str(),
                 recoveryState().c_str());
        return false;
      }

      auto grace_period = deps_->getSettings().recovery_grace_period;
      if (!grace_period_expired &&
          grace_period > std::chrono::milliseconds::zero()) {
        grace_period_->activate(grace_period);
      } else {
        RATELIMIT_INFO(std::chrono::seconds(10),
                       10,
                       "Completing an authoritative partial digest for %s "
                       "because grace period with timeout of %ldms expired. "
                       "State: {%s}",
                       identify().c_str(),
                       grace_period.count(),
                       recoveryState().c_str());
        return onDigestComplete();
      }
    }
  }

  return false;
}

void EpochRecovery::onGracePeriodExpired() {
  ld_debug("Grace period timer expired for %s. State: {%s}",
           identify().c_str(),
           recoveryState().c_str());
  // We are here because onDigestMayHaveBecomeComplete() got an incomplete
  // authoritative majority and decided to give more time for some nodes to
  // chime in. Calling onDigestMayHaveBecomeComplete() again now that the grace
  // period expired.
  onDigestMayHaveBecomeComplete(true /* grace_period_expired */);
}

void EpochRecovery::onMutationComplete(esn_t esn, Status st, ShardID shard) {
  auto it = mutators_.find(esn);
  ld_check(it != mutators_.end());
  ld_check(it->second != nullptr);
  Seal preempted_seal = it->second->getPreemptedSeal();
  mutators_.erase(it);
  ld_check(digestComplete());

  switch (st) {
    case E::NOTINCONFIG:
      ld_check(shard.isValid());
      deps_->onShardRemovedFromConfig(shard);
      return;

    case E::PREEMPTED:
      deps_->onEpochRecovered(
          epoch_, TailRecord(), Status::PREEMPTED, preempted_seal);
      // this EpochRecovery object no longer exists
      return;

    default:
      ld_check(st == E::OK);
  }

  if (mutators_.size() == 0) {
    advanceToCleaning();
  }
}

esn_t EpochRecovery::getLastDigestEsn() const {
  return std::max(lng_, digest_.getLastEsn());
}

void EpochRecovery::advanceToCleaning() {
  ld_check(last_mutation_start_ !=
           std::chrono::steady_clock::time_point::min());

  // MUTATION -> CLEANING
  ld_check(state_ == State::MUTATION);
  state_ = State::CLEAN;

  HISTOGRAM_ADD(deps_->getStats(),
                log_recovery_mutation,
                usec_since(last_mutation_start_));

  deps_->noteMutationsCompleted(*this);

  last_cleaning_start_ = std::chrono::steady_clock::now();

  auto absent_set = recovery_set_.getNodes([](const RecoveryNode& n) {
    return n.getState() != RecoveryNode::State::MUTATABLE;
  });

  ld_check(absent_nodes_.empty());
  absent_nodes_.assign(absent_set.begin(), absent_set.end());

  // all mutations are done. Move on to cleaning.
  int n_matching = recovery_set_.advanceMatching(RecoveryNode::State::CLEANING);

  ld_check(n_matching > 0);
  // It is possible that RecoverySet::advanceMatching() above may restart the
  // EpochRecovery state machine if a node is replaced. In such case,
  // mutation_set_size_ is reset back to 0
  ld_check(n_matching == mutation_set_size_ || mutation_set_size_ == 0);
}

void EpochRecovery::onCleaned(ShardID from, Status st, Seal preempted_seal) {
  if (!recovery_set_.expectingCleaned(from)) {
    return;
  }

  if (st == Status::PREEMPTED && preempted_seal.valid()) {
    deps_->onEpochRecovered(
        epoch_, TailRecord(), Status::PREEMPTED, preempted_seal);
    // this EpochRecovery object no longer exists
    return;
  }

  if (st != Status::OK) {
    ld_info("Got a CLEANED message from %s for %s with failure code %s. "
            "Will re-run epoch recovery after a timeout. State: {%s}",
            from.toString().c_str(),
            identify().c_str(),
            error_name(st),
            recoveryState().c_str());
    return;
  }

  ld_check(mutation_set_size_ > 0);
  ld_check(mutation_set_size_ <= recovery_set_.size());

  recovery_set_.transition(from, RecoveryNode::State::CLEAN);

  int n_clean = recovery_set_.countShardsInState(RecoveryNode::State::CLEAN);

  if (n_clean < mutation_set_size_) {
    return;
  }

  // All nodes in the mutation set have moved to state CLEAN. We are
  // done. Register that fact in our EpochStore.
  ld_check(last_cleaning_start_ !=
           std::chrono::steady_clock::time_point::min());
  HISTOGRAM_ADD(deps_->getStats(),
                log_recovery_cleaning,
                usec_since(last_cleaning_start_));

  advanceLCE();
}

void EpochRecovery::advanceLCE() {
  // three possible cases:
  // 1) after digest completes it found that there are no node can participate
  //    in recovery (i.e., due to authoritativeness or nodes removed from
  //    config, see onDigestComplete()), consider recovery done and advance lce;
  // 2) in CLEAN phase, the state machine receives CLEANED reply from all nodes
  //    in the mutation and cleaning set;
  // 3) epoch recovery timed out in ADVANCE_LCE state. Since the epoch is
  //    already recovered, advanceLCE() is retried without restarting recovery.
  ld_check(state_ == State::DIGEST || state_ == State::CLEAN ||
           state_ == State::ADVANCE_LCE);
  state_ = State::ADVANCE_LCE;

  ld_check(final_tail_record_.isValid());
  ld_check(!final_tail_record_.containOffsetWithinEpoch());
  const epoch_t recovering_epoch = epoch_;
  const recovery_id_t recovery_id = id_;

  TailRecord epoch_tail = final_tail_record_;
  if (epoch_tail.hasPayload() &&
      epoch_tail.getPayloadSlice().size > TAIL_RECORD_INLINE_LIMIT) {
    ld_check(tail_optimized_);
    // payload is too large to put into the epoch store. discard it.
    epoch_tail.removePayload();
  }

  deps_->setLastCleanEpoch(
      getLogID(),
      epoch_,
      epoch_tail,
      [recovering_epoch, recovery_id](Status status,
                                      logid_t log_id,
                                      epoch_t lce,
                                      TailRecord tail_record_from_epoch_store) {
        // it's OK to use Worker here, in unit tests we will call
        // onLastCleanEpochUpdated on the state machine directly
        EpochRecovery* active_recovery =
            Worker::onThisThread()->findActiveEpochRecovery(log_id);
        if (!active_recovery || active_recovery->epoch_ != recovering_epoch ||
            active_recovery->id_ != recovery_id) {
          ld_info("EpochStore reply for setLastCleanEpoch() initiated by "
                  "EpochRecovery machine (id %lu) for epoch %u of log %lu "
                  "is stale. That EpochRecovery machine is no longer running. "
                  "Ignoring.",
                  recovery_id.val_,
                  recovering_epoch.val_,
                  log_id.val_);
          return;
        }
        active_recovery->onLastCleanEpochUpdated(
            status, lce, std::move(tail_record_from_epoch_store));
      });
}

void EpochRecovery::onLastCleanEpochUpdated(Status st,
                                            epoch_t lce,
                                            TailRecord tail_from_epoch_store) {
  auto update_epoch_recovery_latency = [this]() {
    HISTOGRAM_ADD(
        deps_->getStats(), log_recovery_epoch, usec_since(activation_time_));
    HISTOGRAM_ADD(
        deps_->getStats(), log_recovery_epoch_restarts, num_restarts_);
  };

  switch (st) {
    case E::STALE:
      ld_check(epoch_ <= lce);
      RATELIMIT_INFO(std::chrono::seconds(1),
                     10,
                     "The last clean epoch (LCE) "
                     "recorded in our epoch store for log %lu is %u, which is "
                     "the same or higher than epoch %u being recovered by this "
                     "EpochRecovery machine. This should be rare.",
                     getLogID().val_,
                     lce.val_,
                     epoch_.val_);
      update_epoch_recovery_latency();

      // it is OK to use final_tail_record_ even if another sequencer has
      // already recovered this epoch. The reason is that after the mutation
      // stage the state of the epoch should become immutable and the consensus
      // is already established, and the tail record computed by the other
      // sequencer must be the same as the final_tail_record_.
      // TODO T22445654: detect sequencer preemption
      deps_->onEpochRecovered(epoch_, final_tail_record_, Status::OK);
      // this EpochRecovery object no longer exists here
      return;

    case E::OK:
      ld_check(epoch_ == lce);
      ld_check(tail_from_epoch_store.header.lsn ==
               final_tail_record_.header.lsn);
      update_epoch_recovery_latency();
      deps_->onEpochRecovered(
          epoch_,
          // use final_tail_record_ as it is more likely to contain payload
          final_tail_record_,
          Status::OK);
      // this EpochRecovery object no longer exists here
      return;

    case E::ACCESS:
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      2,
                      "EpochStore denied EpochRecovery for %s access to "
                      "set LCE. Will retry.",
                      identify().c_str());
      break;

    case E::BADMSG:
      RATELIMIT_CRITICAL(std::chrono::seconds(10),
                         2,
                         "METADATA CORRUPTION: EpochStore LCE record for %s "
                         "is corrupted.",
                         identify().c_str());
      break;

    case E::CONNFAILED:
      RATELIMIT_WARNING(
          std::chrono::seconds(10),
          2,
          "Lost connection to EpochStore while setting LCE for %s. "
          "Will retry.",
          identify().c_str());
      break;

    case E::INTERNAL:
      RATELIMIT_CRITICAL(std::chrono::seconds(10),
                         2,
                         "INTERNAL ERROR while setting LCE for %s. Will retry.",
                         identify().c_str());
      break;

    case E::NOTFOUND:
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      2,
                      "Failed to update LCE for %s because the log is not "
                      "provisioned in the EpochStore. Will retry.",
                      identify().c_str());
      break;

    case E::AGAIN:
      RATELIMIT_INFO(std::chrono::seconds(10),
                     2,
                     "Advancing LCE for %s failed because another sequencer "
                     "simultaneously tried to update it. Will retry.",
                     identify().c_str());
      break;

    default:
      RATELIMIT_CRITICAL(
          std::chrono::seconds(10),
          2,
          "INTERNAL ERROR: unexpected status %s while setting LCE "
          "for %s. Will retry.",
          error_name(st),
          identify().c_str());
  }

  // if we did not call onEpochRecovered() above, the mutation_and_cleaning_
  // timer will retry setting the LCE after timeout
}

std::string EpochRecovery::recoveryState() const {
  std::string nodes_info;

  auto add = [&](char marker, RecoveryNode::State state) {
    const auto& set = recovery_set_.getNodesInState(state);
    if (set.empty()) {
      return;
    }
    if (!nodes_info.empty()) {
      nodes_info += ' ';
    }
    nodes_info += marker;
    nodes_info += ':';

    for (auto it = set.begin(); it != set.end(); ++it) {
      if (it != set.begin()) {
        nodes_info += ',';
      }
      nodes_info += it->toString();
      const auto st = recovery_set_.getNodeAuthoritativeStatus(*it);
      if (st == AuthoritativeStatus::UNDERREPLICATION) {
        nodes_info += "(UR)";
      } else if (st == AuthoritativeStatus::AUTHORITATIVE_EMPTY) {
        nodes_info += "(AE)";
      }
    }
  };

  add('s', RecoveryNode::State::SEALING);
  add('S', RecoveryNode::State::SEALED);
  add('d', RecoveryNode::State::DIGESTING);
  add('D', RecoveryNode::State::DIGESTED);
  add('m', RecoveryNode::State::MUTATABLE);
  add('c', RecoveryNode::State::CLEANING);
  add('C', RecoveryNode::State::CLEAN);

  return nodes_info + " " + toString(getState());
}

void EpochRecovery::getDebugInfo(InfoRecoveriesTable& table) const {
  auto created = std::chrono::duration_cast<std::chrono::milliseconds>(
      creation_timestamp_);
  auto restarted = std::chrono::duration_cast<std::chrono::milliseconds>(
      last_restart_timestamp_);

  const auto fmajority_result = recovery_set_.fmajorityForNodesInStateOrHigher(
      RecoveryNode::State::DIGESTED);

  const bool digest_fmajority = fmajority_result != FmajorityResult::NONE;

  const bool mutation_set_replication =
      recovery_set_.canReplicateNodesInStateOrHigher(
          RecoveryNode::State::MUTATABLE);

  const bool digest_authoritative =
      fmajority_result == FmajorityResult::AUTHORITATIVE_COMPLETE ||
      fmajority_result == FmajorityResult::AUTHORITATIVE_INCOMPLETE;

  table.next()
      .set<0>(log_id_)
      .set<1>(epoch_)
      .set<2>(isActive() ? "ACTIVE" : "INACTIVE")
      .set<3>(lng_)
      .set<4>(digest_.size())
      .set<5>(digest_fmajority)
      .set<6>(mutation_set_replication)
      .set<7>(digest_authoritative)
      .set<8>(num_holes_plugged_)
      .set<9>(num_holes_re_replicate_)
      .set<10>(num_holes_conflict_)
      .set<11>(num_records_re_replicate_)
      .set<12>(mutators_.size())
      .set<13>(recoveryState())
      .set<14>(created)
      .set<15>(restarted)
      .set<16>(num_restarts_);
}

////// Dependencies ////////////

EpochRecoveryDependencies::EpochRecoveryDependencies(LogRecoveryRequest* driver)
    : sender_(std::make_unique<SenderProxy>()), driver_(driver) {}

EpochRecoveryDependencies::~EpochRecoveryDependencies() {}

epoch_t EpochRecoveryDependencies::getSealEpoch() const {
  return driver_->getSealEpoch();
}

epoch_t EpochRecoveryDependencies::getLogRecoveryNextEpoch() const {
  return driver_->getEpoch();
}

void EpochRecoveryDependencies::onEpochRecovered(epoch_t epoch,
                                                 TailRecord epoch_tail,
                                                 Status status,
                                                 Seal seal) {
  return driver_->onEpochRecovered(epoch, std::move(epoch_tail), status, seal);
}

void EpochRecoveryDependencies::onShardRemovedFromConfig(ShardID shard) {
  return driver_->onShardRemovedFromConfig(shard);
}

bool EpochRecoveryDependencies::canMutateShard(ShardID shard) const {
  auto rebuilding_set =
      Worker::onThisThread()->processor_->rebuilding_set_.get();

  // the shard is not writable if it is in the current rebuilding set.
  // (note: we do not count time-range rebuilding)
  return !rebuilding_set ||
      (rebuilding_set->isRebuildingFullShard(shard.node(), shard.shard()) ==
       folly::none);
}

NodeID EpochRecoveryDependencies::getMyNodeID() const {
  return Worker::onThisThread()->processor_->getMyNodeID();
}

read_stream_id_t EpochRecoveryDependencies::issueReadStreamID() {
  return Worker::onThisThread()->processor_->issueReadStreamID();
}

void EpochRecoveryDependencies::noteMutationsCompleted(
    const EpochRecovery& erm) {
  return driver_->noteMutationsCompleted(erm);
}

std::unique_ptr<BackoffTimer> EpochRecoveryDependencies::createBackoffTimer(
    const chrono_expbackoff_t<std::chrono::milliseconds>& backoff,
    std::function<void()> callback) {
  auto timer =
      std::make_unique<ExponentialBackoffTimer>(std::move(callback), backoff);
  return std::move(timer);
}

std::unique_ptr<Timer>
EpochRecoveryDependencies::createTimer(std::function<void()> cb) {
  auto timer = std::make_unique<Timer>(cb);
  return timer;
}

int EpochRecoveryDependencies::registerOnSocketClosed(const Address& addr,
                                                      SocketCallback& cb) {
  return Worker::onThisThread()->sender().registerOnSocketClosed(addr, cb);
}

int EpochRecoveryDependencies::setLastCleanEpoch(logid_t logid,
                                                 epoch_t lce,
                                                 const TailRecord& tail_record,
                                                 EpochStore::CompletionLCE cf) {
  EpochStore& epoch_store =
      Worker::onThisThread()->processor_->allSequencers().getEpochStore();

  return epoch_store.setLastCleanEpoch(logid, lce, tail_record, std::move(cf));
}

std::unique_ptr<Mutator>
EpochRecoveryDependencies::createMutator(const STORE_Header& header,
                                         const STORE_Extra& extra,
                                         Payload payload,
                                         StorageSet mutation_set,
                                         ReplicationProperty replication,
                                         std::set<ShardID> amend_metadata,
                                         std::set<ShardID> conflict_copies,
                                         EpochRecovery* epoch_recovery) {
  return std::make_unique<Mutator>(header,
                                   extra,
                                   payload,
                                   std::move(mutation_set),
                                   std::move(replication),
                                   std::move(amend_metadata),
                                   std::move(conflict_copies),
                                   epoch_recovery);
}

const Settings& EpochRecoveryDependencies::getSettings() const {
  return Worker::settings();
}

StatsHolder* EpochRecoveryDependencies::getStats() const {
  return Worker::stats();
}

logid_t EpochRecoveryDependencies::getLogID() const {
  return driver_->getLogID();
}

namespace {
SimpleEnumMap<EpochRecovery::State, const char*>
    state_names({{EpochRecovery::State::SEAL_OR_INACTIVE, "SEAL_OR_INACTIVE"},
                 {EpochRecovery::State::DIGEST, "DIGEST"},
                 {EpochRecovery::State::MUTATION, "MUTATION"},
                 {EpochRecovery::State::CLEAN, "CLEAN"},
                 {EpochRecovery::State::ADVANCE_LCE, "ADVANCE_LCE"}});
}

std::ostream& operator<<(std::ostream& os, EpochRecovery::State state) {
  return os << state_names[state];
}
}} // namespace facebook::logdevice
