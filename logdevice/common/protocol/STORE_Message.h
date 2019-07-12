/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <array>
#include <cstdlib>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include <folly/Optional.h>
#include <folly/small_vector.h>

#include "logdevice/common/ClientID.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/OffsetMap.h"
#include "logdevice/common/PayloadHolder.h"
#include "logdevice/common/RecordID.h"
#include "logdevice/common/Seal.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/configuration/TrafficClass.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/settings/Durability.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Record.h"

namespace facebook { namespace logdevice {

// a StoreChainLink describes one link in a message delivery chain
struct StoreChainLink {
  // Shard to which a STORE is to be delivered.
  ShardID destination;

  // id of the "client" connection on the destination node whose remote end
  // (from the point of view of that node) is the sender of this message.
  // The destination server uses this ClientID to send a reply to the message
  // directly to the sender.
  ClientID origin;

  bool operator==(const StoreChainLink& other) const {
    return destination == other.destination && origin == other.origin;
  }

  std::string toString() const;
};

struct STORE_Header {
  RecordID rid; // unique ID of record being stored, includes log id and LSN
  uint64_t timestamp; // Record timestamp in milliseconds since 1970.

  // highest esn in this record's epoch such that at the time this message
  // was originally sent by a sequencer all records with this and lower ESNs
  // in this epoch were known to the sequencer to be fully stored on R nodes
  esn_t last_known_good;

  // Appender sends STORE messages in waves, generating a new copyset
  // for each wave. All STORE messages in the same wave share the same
  // copyset. This is the id of wave to which this STORE belongs
  uint32_t wave;

  STORE_flags_t flags; // a bitset of flag constants defined below

  copyset_size_t nsync; // if chain-sending, number of copies that must be
                        // synced to disk before acknowledging
  copyset_off_t copyset_offset; // Offset of the recipient of this message in
                                // the copyset.  Incremented when forwarding.
  copyset_size_t copyset_size;  // total number of node ids in the copyset
                                // to follow this header

  uint32_t timeout_ms; // store timeout in milliseconds
                       // the store node should skip handling this
                       // message if it hasn't started to store it
                       // timeout_ms milliseconds after received it.
                       // 0 means no timeout

  // ID of the _latest_ sequencer node for the log.
  // (Not necessarily the node that ran sequencer for this record's epoch.)
  // Used for sealing the log.
  // Can be invalid in the unlikely corner case of metadata log records written
  // by rebuilding after the log was removed from config.
  NodeID sequencer_node_id;

  // followed by an array of .copyset_size StoreChainLink structs. If CHAIN
  // flag is not set in .flags, StoreChainLink::origin fields of those structs
  // are ignored

  // if set in .flags, the node that will receive this message is
  // instructed to forward it to the next node in the replication
  // list. Otherwise destination will do no forwarding.
  static const STORE_flags_t CHAIN = 1 << 0; //=1

  // If set in .flags, the storage node should sync the write before
  // replying.
  static const STORE_flags_t SYNC = 1 << 1; //=2

  // If set in .flags, the payload is prefixed with a checksum.  The length of
  // the checksum depends on the CHECKSUM_64BIT flag; if it is set, the
  // checksum is 64 bits, otherwise 32 bits.
  static const STORE_flags_t CHECKSUM = 1 << 2;       //=4
  static const STORE_flags_t CHECKSUM_64BIT = 1 << 3; //=8
  // XNOR of the other two checksum bits.  Ensures that single-bit corruptions
  // in the checksum bits (or flags getting zeroed out) get detected.
  static const STORE_flags_t CHECKSUM_PARITY = 1 << 4; //=16

  // This message is a part of log recovery procedure and is to
  // perform a mutation of a previously sealed epoch. When this flag
  // is set, the header is immediately followed by a 4-byte epoch
  // number of the epoch through which the recovery procedure has
  // sealed the log.  When the recipient does its preemption check on
  // the STORE, it must use that epoch number instead of rid.epoch.

  // Indicates that this STORE message was sent by recovery, and the wave is
  // repurposed to be the `sequencer epoch' of the log recovery.
  // Not to be confused with WRITTEN_BY_RECOVERY, which means that this record
  // was *at some point* written by recovery, but the current STORE message
  // is not necessarily from recovery.
  // Mutually exclusive with REBUILDING.
  static const STORE_flags_t RECOVERY = 1 << 5; //=32

  // The record being stored is a plug for a hole in the numbering sequence.
  // There is no payload. RECOVERY must also be set.
  static const STORE_flags_t HOLE = 1 << 6; //=64

  // Record contains a blob composed by BufferedWriter
  static const STORE_flags_t BUFFERED_WRITER_BLOB = 1 << 7; //=128

  // The record is being re-replicated to this node as part of rebuilding.
  static const STORE_flags_t REBUILDING = 1 << 8; //=256

  // Amend metadata (e.g., copyset, flags) of existing record.
  // The message doesn't contain payload.
  static const STORE_flags_t AMEND = 1 << 9; //=512

  // The store is issued by a sequencer draining its appenders during
  // graceful reactivation/migration, and should not be preempted by
  // soft seals
  static const STORE_flags_t DRAINING = 1 << 10; //=1024

  // The record is part of a sticky copyset block.
  // Record contains the starting LSN of the block, or LSN_INVALID if this
  // record is out-of-block (equivalent to not having STICKY_COPYSET flag).
  static const STORE_flags_t STICKY_COPYSET = 1 << 11; //=2048

  // The message has information about how many bytes have been written to the
  // epoch so far.
  static const STORE_flags_t OFFSET_WITHIN_EPOCH = 1 << 12; //=4096

  // The record is written or overwritten by epoch recovery. Not to be confused
  // with RECOVERY. WRITTEN_BY_RECOVERY means that the record was at some point
  // written by recovery, but the current STORE message doesn't necessarily
  // originate from recovery. The current STORE message is either from recovery
  // (in which case RECOVERY flag is set too) or from rebuilding (REBUILDING
  // flag set) which passed the flag through from its local log store.
  static const STORE_flags_t WRITTEN_BY_RECOVERY = 1 << 13; //=8192

  // The message contains a custom key provided by the client for this record.
  static const STORE_flags_t CUSTOM_KEY = 1 << 14; //=16384

  // The record being stored is a special hole indicating the end of an epoch.
  // There is no payload. WRITTEN_BY_RECOVERY and HOLE must also be set.
  static const STORE_flags_t BRIDGE = 1 << 15; //=32768

  // The record being stored is the first record in the epoch. It is safe to
  // assume that no record is stored before the ESN of this record in the
  // epoch of the record.
  static const STORE_flags_t EPOCH_BEGIN = 1 << 16; //=65536

  // used to know weather we should expect e2e tracing information or not
  static const STORE_flags_t E2E_TRACING_ON = 1u << 17; //=131072

  // used to know if contains OffsetMap or not
  static const STORE_flags_t OFFSET_MAP = 1u << 18; //=262144

  // NOTE: Reserved to match the same bit in RECORD_Header. Should never
  //       be set on a STORE. Used for asserts.
  static const STORE_flags_t DRAINED = 1u << 19; //=524288

  // Please update STORE_Message::flagsToString() when adding flags.
} __attribute__((__packed__));

/**
 * Optional extra information that may be passed along with STORE_Message
 * such as rebuilding, recovery or byte offset.
 */
struct STORE_Extra {
  // Number which uniquely identifies the recovery machine responsible for
  // sending this message. Used only if STORE_Header::RECOVERY flag is set.
  recovery_id_t recovery_id = recovery_id_t(0);

  // An epoch number through which the recovery procedure has sealed the log.
  // Used only if STORE_Header::RECOVERY flag is set, in which case it is
  // guaranteed to be a valid epoch number.
  epoch_t recovery_epoch = EPOCH_INVALID;

  // If this STORE message is for rebuilding (REBUILDING flag is set), the
  // rebuilding version is sent along. This version will be passed back in the
  // STORED reply so that the RecordRebuilding state machine can detect that it
  // is a stale response in case the rebuilding version changed.
  lsn_t rebuilding_version = LSN_INVALID;
  uint32_t rebuilding_wave = 0;

  // If this STORE message is for rebuilding, the rebuilding_id which uniquely
  // identifies a run of the LogRebuilding state machine where this store
  // originated from, is sent along. This version will be passed back in the
  // STORED reply.
  log_rebuilding_id_t rebuilding_id = LOG_REBUILDING_ID_INVALID;

  // If this STORE message include BYTE_OFFSET (BYTE_OFFSET flag is
  // set), the amount of bytes were written in epoch (to which this message
  // belongs) before record of this message is going to be sent along. Not all
  // STORE message will have this info. This info is sent along with store
  // header every X bytes being appended. If OFFSET_MAP flag is set, other
  // counters will be included. Some of these counters are present in
  // counter_type_t others are specified by logdevice clients.
  OffsetMap offsets_within_epoch;

  // Serialized when wave > 1.
  // If flags.CHAIN is set and copyset_offset >= first_amendable_offset, the
  // AMEND flag is expected to be set.  The STORE is sent without the payload
  // because all remaining nodes in the copyset already have the record.
  // Storage nodes should maintain this implication when forwarding (note that
  // `copyset_offset' increments so the condition can become true later in the
  // chain).
  copyset_size_t first_amendable_offset = COPYSET_SIZE_MAX;

  bool operator==(const STORE_Extra& other) const {
    auto as_tuple = [](const STORE_Extra& r) {
      return std::tie(r.recovery_id,
                      r.recovery_epoch,
                      r.rebuilding_version,
                      r.rebuilding_wave,
                      r.rebuilding_id,
                      r.offsets_within_epoch);
    };
    return as_tuple(*this) == as_tuple(other);
  }
};

class STORE_Message : public Message {
 public:
  /**
   * Appender and Mutator use this constructor when composing STORE messages to
   * send to destinations.
   *
   * @param header     header to copy into header_. The .flags field is
   *                   ignored and is instead copied from the _flags_ parameter.
   * @param copyset    the copyset of the wave of STORE messages to which
   *                   this message belongs. Its size is in the header.
   * @param copyset_offset  copied into header_.copyset_offset
   * @param additional_flags  OR-d into set header_.flags
   * @param extra      optional extra infrmation to be passed along with message
   * @param optional_keys      keys provided by client on appending a record
   * @param payload           payload of the STORE message
   * @param appender_context  if true, double-check that the Appender still
   *                          exists before serializing over the wire
   * @param write_sticky_copysets  used in tests to serialize() to run on a
   *                               non-worker thread. TODO (#37280475): remove.
   */
  STORE_Message(const STORE_Header& header,
                const StoreChainLink copyset[],
                copyset_off_t copyset_offset,
                STORE_flags_t additional_flags,
                STORE_Extra extra,
                std::map<KeyType, std::string> optional_keys,
                std::shared_ptr<PayloadHolder> payload,
                bool appender_context = false,
                std::string e2e_tracing_context = "");

  // Movable but not copyable
  STORE_Message(STORE_Message&&) = default;
  STORE_Message& operator=(STORE_Message&&) = default;
  STORE_Message(const STORE_Message&) = delete;
  STORE_Message& operator=(const STORE_Message&) = delete;

  int8_t getExecutorPriority() const override {
    return header_.flags & (STORE_Header::REBUILDING | STORE_Header::AMEND)
        ? folly::Executor::LO_PRI
        : folly::Executor::HI_PRI;
  }

  static TrafficClass calcTrafficClass(const STORE_Header& header) {
    TrafficClass tc;

    if (header.flags & STORE_Header::RECOVERY) {
      tc = TrafficClass::RECOVERY;
    } else if (header.flags & STORE_Header::REBUILDING) {
      tc = TrafficClass::REBUILD;
    } else {
      tc = TrafficClass::APPEND;
    }
    return tc;
  }

  // see Message.h
  bool cancelled() const override;
  void serialize(ProtocolWriter& writer) const override;

  // The onSent() logic is a bit different on client and server. This method
  // is the part that is shared by both. The server-specific part lives in
  // server/STORE_onSent.cpp.
  // (Normal clients don't currently send STORE messages, but meta-fixer tool
  //  does.)
  void onSentCommon(Status st, const Address& to) const;

  void onSent(Status, const Address& /* to */) const override {
    // Handler lives either in STORE_onSent() (server)
    // or onSentCommon() (client). This should never get called.
    std::abort();
  }
  Disposition onReceived(const Address&) override {
    // Receipt handler lives in StoreStateMachine::onReceived(); this should
    // never get called.
    std::abort();
  }
  static Message::deserializer_t deserialize;
  // Overload of deserialize that does not need to run on a Worker thread.
  static MessageReadResult deserialize(ProtocolReader&,
                                       size_t max_payload_inline);

  /**
   * @return a human-readable representation of copyset_
   */
  std::string printableCopyset();

  // see docs in checkIfPreempted()
  enum class PreemptResult {
    NOT_PREEMPTED,
    PREEMPTED_NORMAL,
    PREEMPTED_SOFT_ONLY
  };

  /**
   * A helper method used to check if the storage node shouldn't process a
   * STORE message because the log was sealed up to a certain epoch.
   *
   * @param record_id       record being stored
   * @param recovery_epoch  if STORE is part of the recovery process (RECOVERY
   *                        flag is set in the header), the sequencer sending
   *                        this STORE message performs recovery up to this
   *                        epoch
   * @param normal_seal     Seal value from the SEAL messages sent by log
   *                        recovery
   * @param soft_seal       Seal value obtained from STORE messages
   * @param drain           indicate if the STORE is for sequencer draining, if
   *                        true, soft seals will be ignored
   *
   * @return                a pair of <PreemptResult, Seal>, in which
   *                        PreemptResult can be:
   *                          NOT_PREEMTPED       STORE is not preempted
   *                          PREEMPTED_NORMAL    STORE is preempted by a normal
   *                                              seal or both normal and soft
   *                                              seals
   *                          PREEMPTED_SOFT_ONLY STORE is preempted only by
   *                                              soft seal
   *                        if PreemptResult is PREEMPTED_NORMAL or
   *                        PREEMPTED_SOFT_ONLY, Seal will set to
   *                        the Seal that effectively preempts the
   *                        STORE. otherwise Seal will be invalid.
   */
  static std::pair<STORE_Message::PreemptResult, Seal>
  checkIfPreempted(const RecordID& record_id,
                   epoch_t recovery_epoch,
                   Seal normal_seal,
                   Seal soft_seal,
                   bool drain = false);

  const STORE_Header& getHeader() const {
    return header_;
  }

  const STORE_Extra& getExtra() const {
    return extra_;
  }

  const folly::small_vector<StoreChainLink, 6>& getCopyset() const {
    return copyset_;
  }

  const PayloadHolder* getPayloadHolder() const {
    return payload_.get();
  }

  /**
   * A method to set the first LSN of the block that the record belongs to.
   *
   * @param lsn     the block starting LSN
   */
  void setBlockStartingLSN(lsn_t lsn);

  /**
   * Pretty-prints flags.
   */
  static std::string flagsToString(STORE_flags_t flags);

  virtual std::vector<std::pair<std::string, folly::dynamic>>
  getDebugInfo() const override;

 private:
  /**
   * deserialize() uses this constructor when it reads a STORE message from
   * the input buffer of a bufferevent.
   */
  STORE_Message(const STORE_Header& header,
                std::shared_ptr<PayloadHolder>&& payload);

  // Convenience wrapper for STORED_Message::createAndSend
  void sendReply(Status status,
                 Seal seal = Seal(),
                 ShardID rebuildingRecipient = ShardID()) const;

  /**
   * Called when a storage node fails to forward this message along
   * a delivery chain.
   *
   * @param  st  error code reported by Sender::sendMessage() or
   *         STORE_Message::onSent()
   */
  void onForwardingFailure(Status st) const;

  STORE_Header header_;

  STORE_Extra extra_;

  // If true, STORE_Message::cancelled() checks that the Appender still
  // exists before allowing the message to be serialized onto the wire
  bool appender_context_{false};

  // Nullptr means empty payload.  The payload is also not serialized when the
  // AMEND flag is set, so on the receiving side it will always be empty for
  // amends.
  std::shared_ptr<PayloadHolder> payload_;

  // identities of all nodes on which this record is stored
  folly::small_vector<StoreChainLink, 6> copyset_;

  // The starting LSN of the block to which the record belongs (issued by the
  // sticky copyset selector), or LSN_INVALID if this is a single record.
  // This is essentially unused until block CSI is implemented.
  lsn_t block_starting_lsn_ = LSN_INVALID;

  // The (optional) keys provided by the client in the append() operation.
  // See @Record.h for details
  std::map<KeyType, std::string> optional_keys_;

  // This field is used only if the message was received from the
  // wire. StoreStateMachine::onReceived() sets this to the offset in
  // copyset_[] of the node that received the message (and may need to forward
  // it down the chain). If we are in the sequencer context and the object was
  // not received, but rather was created locally by an Appender, this field
  // is set to -1.
  copyset_off_t my_pos_in_copyset_;

  // Where to send a reply once the write is done.  Calculated by
  // StoreStateMachine::onReceived() and used in sendReply().
  ClientID reply_to_;

  // if true, indicate that the STORE is preempted by only by a soft seal
  bool soft_preempted_only_{false};

  // tracing context containing information about the parent context of future
  // spans references
  std::string e2e_tracing_context_;

  friend class StoreStateMachine;
  friend void STORE_onSent(const STORE_Message& msg,
                           Status st,
                           const Address& to,
                           const SteadyTimestamp enqueue_time);
  friend class MutatorTest;
  friend class AppenderTest;
  friend class TestAppender;
  friend class MessageSerializationTest;
  friend class E2ETracingSerializationTest;
};

}} // namespace facebook::logdevice
