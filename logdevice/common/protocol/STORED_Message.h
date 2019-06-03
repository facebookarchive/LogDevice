/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstdint>

#include "logdevice/common/ClientID.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/RecordID.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/configuration/TrafficClass.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

// type of STORED_Header::flags field
typedef uint8_t STORED_flags_t;

struct STORED_Header {
  // unique ID of record that we are replying about
  RecordID rid;

  // id of the "wave" of STORE messages send by this record's Appender
  // to which the STORE message we are replying to belongs
  uint32_t wave;

  // OK           if record copy was successfully stored on this node
  // DISABLED     if local log store is not accepting writes or there is some
  //              persistent error
  // DROPPED      if the request was dropped
  // PREEMPTED    if rid.epoch was already sealed by some sequencer
  // NOSPC        if the partition of local log store is out of disk space
  // NOTINCONFIG  if the node id that the node that got the STORE believes to
  //              be its current identifier in the cluster did not
  //              appear in the copyset of the STORE. This status is reported
  //              only if STORE was sent directly rather than through a chain.
  // SHUTDOWN     storage node is shutting down and not accepting any more work
  // NOTSTORAGE   this is not a storage node
  // FORWARD      this node received a chained STORE but was not able to forward
  //              the STORE to the next node in the chain. The Appender can
  //              assume that all nodes in the chain after this node will not
  //              successfully store a copy. When chaining is enabled, the
  //              Appender may receive two STORED messages from a node to which
  //              it sent a STORE, one with E::FORWARD and another one for the
  //              store operation it performs locally.
  // REBUILDING   copyset intersects with recipient node's view of rebuilding
  //              set.
  //              If there are multiple such nodes, it may take multiple waves
  //              for the sequencer to blacklist them all. This check is
  //              disabled for records/holes stored by recovery
  //              (STORE_Message::WRITTEN_BY_RECOVERY flag);
  //              TODO (#10343616): bypassing the check for recovery stores is
  //                                not fully correct.
  // CHECKSUM_MISMATCH
  //              if the STORE failed checksum verification, either this or
  //              sequencer node seem to have corrupted it, likely due to bad
  //              hardware or bug. Appender will determine which node caused
  //              this. It's also possible that the STORE was malformed due to
  //              a bug, in which case this node will be blamed as corrupting
  //              stores and be graylisted. Either case will set off an alarm
  //              and should be investigated by the oncall.
  Status status;

  // when status is PREEMPTED, this will be set to the ID of the most recent
  // sequencer that sealed this log
  NodeID redirect;

  // a bitset of flag constants defined below
  STORED_flags_t flags;

  shard_index_t shard;

  // size of the header in message given the protocol version
  static size_t headerSize(uint16_t proto) {
    return sizeof(STORED_Header);
  }

  // the write was part of a batch that was synced
  static const STORED_flags_t SYNCED = 1ul << 0; //=1
  // worker's storage task queue is overloaded
  // @see PerWorkerStorageTaskQueue::isOverloaded()
  static const STORED_flags_t OVERLOADED = 1ul << 1; //=2
  // This flag is ignored. It used to tell whether this record can be amended
  // later. Now everything is amendable.
  static const STORED_flags_t AMENDABLE_DEPRECATED = 1ul << 2; //=4
  // the record is a part of rebuilding
  // (REBUILDING flag was set in STORE_Header)
  static const STORED_flags_t REBUILDING = 1ul << 3; //=8
  // the store is preempted ONLY by a soft seal
  static const STORED_flags_t PREMPTED_BY_SOFT_SEAL_ONLY = 1ul << 4; //=16
  // the local log store's partition crossed low-watermark
  static const STORED_flags_t LOW_WATERMARK_NOSPC = 1ul << 5; //=32
} __attribute__((__packed__));

class STORED_Message : public Message {
 public:
  static TrafficClass calcTrafficClass(const STORED_Header& header) {
    return (header.flags & STORED_Header::REBUILDING) ? TrafficClass::REBUILD
                                                      : TrafficClass::APPEND;
  }

  explicit STORED_Message(const STORED_Header& header,
                          lsn_t rebuilding_version,
                          uint32_t rebuilding_wave,
                          log_rebuilding_id_t rebuilding_id,
                          FlushToken flushToken,
                          ServerInstanceId serverInstanceId,
                          ShardID rebuildingRecipient = ShardID());

  void serialize(ProtocolWriter&) const override;

  // The onReceived() logic is a bit different on client and server. This method
  // is the part that is shared by both. The server-specific part lives in
  // server/STORED_onReceived.cpp.
  // (Normal clients don't currently send STORE messages, but meta-fixer tool
  //  does.)
  Disposition onReceivedCommon(const Address& from);

  Disposition onReceived(const Address& /* from */) override {
    // Handler lives either in STORED_onReceived() (server)
    // or onReceivedCommon() (client). This should never get called.
    std::abort();
  }

  int8_t getExecutorPriority() const override {
    return header_.flags & STORED_Header::REBUILDING ? folly::Executor::LO_PRI
                                                     : folly::Executor::HI_PRI;
  }

  static Message::deserializer_t deserialize;

  /**
   * Creates a STORED message and sends it to the specified client.  Logs an
   * error on failure.
   *
   * Must be called on a worker thread.  If the client is not owned by the
   * current Worker, a Request is sent to the responsible Worker, which will
   * send the message.
   */
  static void createAndSend(const STORED_Header& header,
                            ClientID send_to,
                            lsn_t rebuilding_version,
                            uint32_t rebuilding_wave,
                            log_rebuilding_id_t rebuilding_id,
                            FlushToken flushToken = FlushToken_INVALID,
                            ShardID rebuildingRecipient = ShardID());

  STORED_Header header_;

  // If the STORE message was for rebuilding (REBUILDING flag was set), the
  // rebuilding version is sent back along so that the RecordRebuilding state
  // machine can detect that it is a stale response in case the rebuilding
  // version changed.
  lsn_t rebuilding_version_;
  uint32_t rebuilding_wave_;
  // If the STORE message was for rebuilding (REBUILDING flag was set), the
  // rebuilding id is sent back along so that the RecordRebuilding state
  // machine can detect that it is a stale response in case the rebuilding
  // id changed. (Rebuilding id changes everytime LogRebuilding state machine
  // is restarted.)
  log_rebuilding_id_t rebuilding_id_;
  // If the STORE message was for rebuilding (REBUILDING flag was set), the
  // flushToken the write was attributed to is sent so that LogRebuilding
  // state machine can act on a write being stored durably.
  FlushToken flushToken_;
  ServerInstanceId serverInstanceId_;

  // If header_.status == E::REBUILDING, this field is populated with the
  // recipient in the copyset that is in the rebuilding set.
  ShardID rebuildingRecipient_;

  virtual std::vector<std::pair<std::string, folly::dynamic>>
  getDebugInfo() const override;

 private:
  /**
   * Calls Appender::onReply() once (at most).  Helper function.
   */
  static Message::Disposition handleOneMessage(const STORED_Header& header,
                                               ShardID from,
                                               ShardID rebuildingRecipient);

  friend Disposition STORED_onReceived(STORED_Message* msg,
                                       const Address& from);
};

}} // namespace facebook::logdevice
