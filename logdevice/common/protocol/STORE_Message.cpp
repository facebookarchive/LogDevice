/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/STORE_Message.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <random>
#include <unistd.h>
#include <vector>

#include <folly/Memory.h>

#include "logdevice/common/Appender.h"
#include "logdevice/common/EpochRecovery.h"
#include "logdevice/common/RebuildingTypes.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/MUTATED_Message.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"
#include "logdevice/common/protocol/STORED_Message.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

// gcc 4.8 does not yet implement std::is_trivially_copyable. Uncomment this
// when we are on a version of gcc that does.
//
// static_assert(std::is_trivially_copyable<StoreChainLink>::value,
//              "StoreChainLink must be trivially copyable because we use "
//              "memcpy() on its objects");

STORE_Message::STORE_Message(const STORE_Header& header,
                             const StoreChainLink copyset[],
                             copyset_off_t copyset_offset,
                             STORE_flags_t flags,
                             STORE_Extra extra,
                             std::map<KeyType, std::string> optional_keys,
                             std::shared_ptr<PayloadHolder> payload,
                             bool appender_context,
                             std::string e2e_tracing_context)
    : Message(MessageType::STORE, calcTrafficClass(header)),
      header_(header),
      extra_(extra),
      appender_context_(appender_context),
      payload_(std::move(payload)),
      copyset_(header.copyset_size),
      optional_keys_(std::move(optional_keys)),
      my_pos_in_copyset_(-1),
      e2e_tracing_context_(std::move(e2e_tracing_context)) {
  ld_check(header_.copyset_size > 0);
  ld_check(header_.copyset_size <= COPYSET_SIZE_MAX);
  ld_check(copyset_offset >= 0);
  ld_check(copyset_offset < COPYSET_SIZE_MAX);

  if (flags & STORE_Header::RECOVERY) {
    ld_check(extra_.recovery_id.val_);
    ld_check(extra_.recovery_epoch != EPOCH_INVALID);
  }

  // in debug mode assert that all node indexes in copyset[] are distinct
  if (folly::kIsDebug) {
    std::vector<ShardID> indexes(header.copyset_size);
    std::transform(copyset,
                   copyset + header.copyset_size,
                   indexes.begin(),
                   [](const StoreChainLink& c) { return c.destination; });
    std::sort(indexes.begin(), indexes.end());
    ld_assert(std::unique(indexes.begin(), indexes.end()) == indexes.end());
  }

  memcpy(copyset_.begin(), copyset, sizeof(copyset[0]) * header.copyset_size);
  header_.flags |= flags;
  if (header_.flags & STORE_Header::STICKY_COPYSET) {
    ld_check(false); // this shouldn't be set before we have the LSN
    header_.flags &= ~STORE_Header::STICKY_COPYSET;
  }

  header_.copyset_offset = copyset_offset;
}

STORE_Message::STORE_Message(const STORE_Header& header,
                             std::shared_ptr<PayloadHolder>&& payload)
    : Message(MessageType::STORE, calcTrafficClass(header)),
      header_(header),
      payload_(std::move(payload)),
      copyset_(header.copyset_size),
      my_pos_in_copyset_(-1) {
  ld_check(payload_);
}

bool STORE_Message::cancelled() const {
  if (appender_context_) {
    // TODO: Is this check worth it?  This is likely executing very quickly
    // after the Appender created the STORE_Message, unlikely it's gone by now.
    ld_check(my_pos_in_copyset_ < 0);
    const Appender* a =
        Worker::onThisThread()->activeAppenders().map.find(header_.rid);
    if (!a) {
      // Appender is gone. Cancel the message.
      return true;
    }
  }
  return false;
}
void STORE_Message::serialize(ProtocolWriter& writer) const {
  // Assert that the expectation documented in the header file about
  // `first_amendable_offset' is satisfied.  This is not in the constructor
  // because the forwarding codepath on storage nodes modifies and sends an
  // existing STORE_Message.
  if ((header_.flags & STORE_Header::CHAIN) &&
      header_.copyset_offset >= extra_.first_amendable_offset) {
    ld_check(header_.flags & STORE_Header::AMEND);
    ld_check(header_.wave > 1);
  }

  ld_check(!payload_ || payload_->valid());

  writer.write(header_);

  if (header_.flags & STORE_Header::RECOVERY) {
    writer.write(extra_.recovery_id);
    writer.write(extra_.recovery_epoch);
  }

  if (header_.flags & STORE_Header::REBUILDING) {
    writer.write(extra_.rebuilding_version);
    writer.write(extra_.rebuilding_wave);
    writer.write(extra_.rebuilding_id);
  }

  if (header_.flags & STORE_Header::OFFSET_WITHIN_EPOCH) {
    if (header_.flags & STORE_Header::OFFSET_MAP) {
      extra_.offsets_within_epoch.serialize(writer);
    } else {
      writer.write(extra_.offsets_within_epoch.getCounter(BYTE_OFFSET));
    }
  }

  if (header_.wave > 1) {
    writer.write(extra_.first_amendable_offset);
  }

  writer.writeVector(copyset_);
  ld_check(header_.copyset_size == copyset_.size());

  if (header_.flags & STORE_Header::STICKY_COPYSET) {
    writer.write(block_starting_lsn_);
  }

  if (header_.flags & STORE_Header::CUSTOM_KEY) {
    // Replacing it with write/readLengthPrefixedVector breaks existing
    // MessageSerializationTest test infrastructure.
    uint8_t optional_keys_length = optional_keys_.size();
    writer.write(optional_keys_length);
    for (const auto& key_pair : optional_keys_) {
      uint8_t type = static_cast<uint8_t>(key_pair.first);
      ld_check(type <= std::numeric_limits<uint8_t>::max());
      writer.write(type);
      uint16_t length = key_pair.second.size();
      writer.write(length);
      writer.writeVector(key_pair.second);
    }
  }

  if (header_.flags & STORE_Header::E2E_TRACING_ON) {
    ld_check(e2e_tracing_context_.size() < MAX_E2E_TRACING_CONTEXT_SIZE);
    if (e2e_tracing_context_.size() < MAX_E2E_TRACING_CONTEXT_SIZE) {
      // only serialize the information when its size reasonable
      writer.writeLengthPrefixedVector(e2e_tracing_context_);
    } else {
      writer.writeLengthPrefixedVector(std::string(""));
    }
  }

  if (payload_ && !(header_.flags & STORE_Header::AMEND)) {
    payload_->serialize(writer);
  }
}

MessageReadResult STORE_Message::deserialize(ProtocolReader& reader) {
  return deserialize(reader, Worker::settings().max_payload_inline);
}

MessageReadResult STORE_Message::deserialize(ProtocolReader& reader,
                                             size_t max_payload_inline) {
  STORE_Header hdr;
  STORE_Extra extra;

  reader.read(&hdr);

  if (hdr.flags & STORE_Header::RECOVERY) {
    reader.read(&extra.recovery_id);
    reader.read(&extra.recovery_epoch);
  }

  if (hdr.flags & STORE_Header::REBUILDING) {
    reader.read(&extra.rebuilding_version);
    reader.read(&extra.rebuilding_wave);
    reader.read(&extra.rebuilding_id);
  }

  if (hdr.flags & STORE_Header::OFFSET_WITHIN_EPOCH) {
    if (hdr.flags & STORE_Header::OFFSET_MAP) {
      extra.offsets_within_epoch.deserialize(reader, false /*not used */);
    } else {
      uint64_t offset_within_epoch;
      reader.read(&offset_within_epoch);
      extra.offsets_within_epoch.setCounter(BYTE_OFFSET, offset_within_epoch);
    }
  }

  if (hdr.wave > 1) {
    reader.read(&extra.first_amendable_offset);
  }

  if (reader.ok() &&
      (hdr.copyset_size <= 0 || hdr.copyset_size > COPYSET_SIZE_MAX)) {
    ld_error("Bad STORE message: copyset_size (%hhu) is not in the allowed "
             "range [1..%zu]",
             hdr.copyset_size,
             COPYSET_SIZE_MAX);
    return reader.errorResult(E::BADMSG);
  }

  folly::small_vector<StoreChainLink, 6> copyset;
  reader.readVector(&copyset, hdr.copyset_size);
  ld_check(!reader.ok() || copyset.size() == hdr.copyset_size);

  lsn_t block_starting_lsn = LSN_INVALID;
  std::map<KeyType, std::string> optional_keys;
  if (hdr.flags & STORE_Header::STICKY_COPYSET) {
    reader.read(&block_starting_lsn);
  }

  if (hdr.flags & STORE_Header::CUSTOM_KEY) {
    uint8_t optional_keys_length;
    reader.read(&optional_keys_length);
    for (uint8_t i = 0; i < optional_keys_length; ++i) {
      if (!reader.ok()) {
        break;
      }
      uint8_t type;
      std::string str;
      uint16_t length;
      reader.read(&type);
      reader.read(&length);
      reader.readVector(&str, length);
      auto keytype = static_cast<KeyType>(type);
      optional_keys.insert(std::make_pair(keytype, str));
    }
  }

  std::string tracing_context;

  if (hdr.flags & STORE_Header::E2E_TRACING_ON) {
    reader.readLengthPrefixedVector(&tracing_context);
  }

  const size_t payload_size = reader.bytesRemaining();
  auto payload = PayloadHolder::deserialize(
      reader,
      payload_size,
      /*zero_copy*/ payload_size > max_payload_inline);

  auto payload_holder = std::make_shared<PayloadHolder>(std::move(payload));

  return reader.result([&] {
    // No, you can't replace this with make_unique. The constructor is private.
    std::unique_ptr<STORE_Message> m(
        new STORE_Message(hdr, std::move(payload_holder)));
    m->copyset_ = std::move(copyset);
    m->block_starting_lsn_ = block_starting_lsn;
    m->extra_ = std::move(extra);
    m->optional_keys_ = std::move(optional_keys);
    m->e2e_tracing_context_ = std::move(tracing_context);
    return m;
  });
}

void STORE_Message::onSentCommon(Status st, const Address& to) const {
  ld_check(!to.isClientAddress());

  ld_check(header_.copyset_offset < copyset_.size());
  shard_index_t shard_idx =
      copyset_[header_.copyset_offset].destination.shard();
  ShardID shard(to.id_.node_.index(), shard_idx);

  if (header_.flags & STORE_Header::RECOVERY) {
    EpochRecovery* recovery =
        Worker::onThisThread()->findActiveEpochRecovery(header_.rid.logid);
    if (!recovery || recovery->id_ != extra_.recovery_id) {
      ld_debug("No active EpochRecovery machine with id %lu for log %lu found",
               extra_.recovery_id.val_,
               header_.rid.logid.val_);
      return;
    }

    recovery->onStoreSent(shard, header_, st);
    return;
  }

  // message is being sent by an Appender

  Appender* appender{
      // Appender that sent this message
      Worker::onThisThread()->activeAppenders().map.find(header_.rid)};

  if (!appender) {
    // Appender had received enough STORED replies and retired by
    // the time this STORE was passed to TCP. This can happen if we
    // sent into an unconnected Socket and the handshake took too long.
    ld_debug("Appender for record %s sent to %s not found",
             header_.rid.toString().c_str(),
             Sender::describeConnection(to).c_str());
    return;
  }

  ld_assert(header_.rid == Appender::KeyExtractor()(*appender));

  appender->onCopySent(st, shard, header_);
}

void STORE_Message::sendReply(Status status,
                              Seal seal,
                              ShardID rebuildingRecipient) const {
  ld_check(reply_to_.valid());

  // If status is PREEMPTED then a valid preemptor seal must be provided
  ld_check(status != E::PREEMPTED || seal.valid());
  // If status is REBUILDING then a valid rebuilding recipient must be provided
  ld_check(status != E::REBUILDING || rebuildingRecipient.isValid());

  ld_check(header_.copyset_offset < copyset_.size());
  shard_index_t shard = copyset_[header_.copyset_offset].destination.shard();

  if (header_.flags & STORE_Header::RECOVERY) {
    // this STORE is for a mutation during recovery, reply with MUTATED,
    // TODO T28121050: include rebuildignRecipient in MUTATED replies.
    MUTATED_Message::createAndSend(
        MUTATED_Header{
            extra_.recovery_id, header_.rid, status, seal, shard, header_.wave},
        reply_to_);

  } else {
    STORED_flags_t flags = 0;
    if (header_.flags & STORE_Header::REBUILDING) {
      flags |= STORED_Header::REBUILDING;
    }
    if (soft_preempted_only_) {
      // only apply the flag in appends but not recovery
      flags |= STORED_Header::PREMPTED_BY_SOFT_SEAL_ONLY;
    }

    STORED_Message::createAndSend(
        STORED_Header{
            header_.rid, header_.wave, status, seal.seq_node, flags, shard},
        reply_to_,
        extra_.rebuilding_version,
        extra_.rebuilding_wave,
        extra_.rebuilding_id,
        FlushToken_INVALID,
        rebuildingRecipient);
  }
}

void STORE_Message::onForwardingFailure(Status st) const {
  ld_check(my_pos_in_copyset_ >= 0);
  ld_check(my_pos_in_copyset_ + 1 < header_.copyset_size);
  ld_check(header_.flags & STORE_Header::CHAIN);

  ShardID next_dest = copyset_[my_pos_in_copyset_ + 1].destination;

  RATELIMIT_INFO(std::chrono::seconds(10),
                 2,
                 "Failed to forward a STORE message for record %s "
                 "(wave %u) to link #%d (%s) in the chain : %s",
                 header_.rid.toString().c_str(),
                 header_.wave,
                 my_pos_in_copyset_ + 1,
                 next_dest.toString().c_str(),
                 error_description(st));

  WORKER_STAT_INCR(store_forwarding_failed);

  // Send a STORED(E::FORWARD) back to the Appender.
  // Asserting that this STORE was not for recovery here because we don't
  // support E::FORWARD in MUTATED message and chaining should never be used in
  // mutations anyway.
  ld_check(!(header_.flags & STORE_Header::RECOVERY));
  if (!(header_.flags & STORE_Header::RECOVERY)) {
    sendReply(E::FORWARD);
  }
}

std::string STORE_Message::printableCopyset() {
  std::string res;

  res.reserve(80);
  res = "[";

  for (copyset_off_t i = 0; i < header_.copyset_size; i++) {
    res += copyset_[i].destination.toString();
    if (i < header_.copyset_size - 1) {
      res += ",";
    }
  }

  res += "]";

  return res;
}

/* static */
std::pair<STORE_Message::PreemptResult, Seal>
STORE_Message::checkIfPreempted(const RecordID& record_id,
                                epoch_t recovery_epoch,
                                Seal normal_seal,
                                Seal soft_seal,
                                bool drain) {
  auto preempted_by_seal = [&](Seal seal) {
    if (recovery_epoch != EPOCH_INVALID) {
      // In order to prevent concurrent mutations by sequencers performing
      // recovery on the same log, a mutation must fail if it doesn't come
      // from a sequencer which was the last to seal the log.
      return recovery_epoch < seal.epoch;
    }

    return record_id.epoch <= seal.epoch;
  };

  PreemptResult result = PreemptResult::NOT_PREEMPTED;
  Seal preempted_by;

  if (preempted_by_seal(normal_seal)) {
    result = PreemptResult::PREEMPTED_NORMAL;
    preempted_by = normal_seal;
  }

  // if the store is for draining purpose (w/ DRAINING set in flag),
  // ignore soft seals
  if (!drain && preempted_by_seal(soft_seal)) {
    if (result == PreemptResult::NOT_PREEMPTED) {
      result = PreemptResult::PREEMPTED_SOFT_ONLY;
    }

    // use the seq node from the seal with higher preemption epoch
    if (soft_seal.epoch > normal_seal.epoch) {
      preempted_by = soft_seal;
    }
  }

  return std::make_pair(result, preempted_by);
}

void STORE_Message::setBlockStartingLSN(lsn_t lsn) {
  block_starting_lsn_ = lsn;
  if (lsn == LSN_INVALID) {
    header_.flags &= ~STORE_Header::STICKY_COPYSET;
  } else {
    header_.flags |= STORE_Header::STICKY_COPYSET;
  }
}

std::string StoreChainLink::toString() const {
  return destination.toString() + origin.toString();
}

std::string STORE_Message::flagsToString(STORE_flags_t flags) {
  std::string s;

#define FLAG(f)                  \
  if (flags & STORE_Header::f) { \
    if (!s.empty()) {            \
      s += "|";                  \
    }                            \
    s += #f;                     \
  }

  FLAG(CHAIN)
  FLAG(SYNC)
  FLAG(CHECKSUM)
  FLAG(CHECKSUM_64BIT)
  FLAG(CHECKSUM_PARITY)
  FLAG(RECOVERY)
  FLAG(HOLE)
  FLAG(BUFFERED_WRITER_BLOB)
  FLAG(REBUILDING)
  FLAG(AMEND)
  FLAG(DRAINING)
  FLAG(STICKY_COPYSET)
  FLAG(OFFSET_WITHIN_EPOCH)
  FLAG(WRITTEN_BY_RECOVERY)
  FLAG(CUSTOM_KEY)
  FLAG(BRIDGE)
  FLAG(EPOCH_BEGIN)
  FLAG(DRAINED)

#undef FLAG

  return s;
}

std::vector<std::pair<std::string, folly::dynamic>>
STORE_Message::getDebugInfo() const {
  std::vector<std::pair<std::string, folly::dynamic>> res;

  auto add = [&](const char* key, folly::dynamic val) {
    res.emplace_back(key, std::move(val));
  };

  add("log_id", header_.rid.logid.val());
  add("lsn", lsn_to_string(header_.rid.lsn()));
  add("timestamp", header_.timestamp);
  add("last_known_good", header_.last_known_good.val());
  add("wave", header_.wave);
  add("flags", flagsToString(header_.flags));
  add("nsync", header_.nsync);
  add("copyset_offset", header_.copyset_offset);
  add("copyset_size", header_.copyset_size);
  add("timeout_ms", header_.timeout_ms);
  add("sequencer_node_id", header_.sequencer_node_id.toString());
  if (extra_.recovery_id.val()) {
    add("recovery_id", extra_.recovery_id.val());
  }
  if (extra_.recovery_epoch.val()) {
    add("recovery_epoch", extra_.recovery_epoch.val());
  }
  if (extra_.rebuilding_version != LSN_INVALID) {
    add("rebuilding_version", extra_.rebuilding_version);
    add("rebuilding_wave", extra_.rebuilding_wave);
  }
  if (extra_.rebuilding_id != LOG_REBUILDING_ID_INVALID) {
    add("rebuilding_id", extra_.rebuilding_id.val());
  }
  add("offsets_within_epoch", extra_.offsets_within_epoch.toString().c_str());
  if (extra_.first_amendable_offset != COPYSET_SIZE_MAX) {
    add("first_amendable_offset", extra_.first_amendable_offset);
  }
  add("payload_size", payload_ ? payload_->size() : 0);
  add("copyset", toString(copyset_));
  if (block_starting_lsn_ != LSN_INVALID) {
    add("block_starting_lsn", lsn_to_string(block_starting_lsn_));
  }
  if (!optional_keys_.empty()) {
    folly::dynamic map{folly::dynamic::object()};
    for (auto& kv : optional_keys_) {
      map[toString(kv.first)] = kv.second;
    }
    add("optional_keys", std::move(map));
  }
  add("soft_preempted_only", soft_preempted_only_);

  return res;
}

}} // namespace facebook::logdevice
