/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Optional.h>

#include "logdevice/common/Request.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

using APPENDED_flags_t = uint16_t;

struct APPENDED_Header {
  request_id_t rqid; // echoed request id from the corresponding APPEND
  lsn_t lsn;         // on success, the LSN that was assigned to record
                     // on success, the timestamp that was assigned to record
  RecordTimestamp timestamp;

  // E:OK           if record was successfully appended
  // E::NOSEQUENCER if logdeviced that we sent APPEND to is not running a
  //                sequencer for that log
  // E::NOSPC       cluster is out of disk space, record cannot be delivered
  // E::OVERLOADED  cluster is overloaded, record cannot be delivered
  // E::PREEMPTED   the sequencer that received APPEND was preempted for that
  //                log by some other sequencer (see `redirect')
  // E::SHUTDOWN    logdeviced is shutting down and is no longer accepting work
  // E::SEQNOBUFS   if the sequencer (1) reached the maximum size of the sliding
  //                window of Appenders for this log, or (2) attempted to buffer
  //                a pending Appender and hit the max pending appenders limit.
  // E::SEQSYSLIMIT if the sequencer is out of file descriptors, ephemeral
  //                ports, hit the heap size limit, or can't route packets to
  //                some nodes
  // E::NOTREADY    if the node is being brought up and is not running
  //                sequencers at the time; clients should select another node
  // E::REBUILDING  the node is rebuilding and doesn't run sequencers
  // E::DISABLED    too many storage nodes are in persistent error state,
  //                server is not taking appends
  // E::INTERNAL    unexpected error detected, check server logs
  // E::BADPAYLOAD  payload data does not correspond with checksum bits, or
  //                payload does not contain valid epoch metadata (for
  //                metadata log), or
  //                payload contained BufferedWriter blob which could not be
  //                decoded
  // E::ACCESS      Client does not have the required permissions
  //
  //      Starting with Compatibility::APPENDED_NOTINSERVERCONFIG_STATUS
  //
  // E::NOTINSERVERCONFIG  the logid is not in the configuration of the
  //                       sequencer node

  NodeID redirect; // when status is PREEMPTED, indicates which sequencer
                   // should receive appends for the log

  Status status;

  APPENDED_flags_t flags;

  // NOTE: Fixed-size header may be followed by optional fields depending on
  // flags.

  // If set, the header is followed by uint32_t `seq_batching_offset'.
  static const APPENDED_flags_t INCLUDES_SEQ_BATCHING_OFFSET = 1;
  // If set, the record has not been successfully stored on any storage node
  // before failing with preempted.
  static const APPENDED_flags_t NOT_REPLICATED = 2;
  // If set, the `redirect' node appears to be dead from the Failure Detector
  // perspective. This is used to give a hint to the client. This should really
  // only happen if the appender has started and the sequencer realizes it has
  // been previously preempted (received a STORED with E::PREEMPTED) but the
  // preemptor doesn't seem to be alive. In that case clients need to retry the
  // append rather than follow the redirect.
  static const APPENDED_flags_t REDIRECT_NOT_ALIVE = 4;
};

static_assert(sizeof(APPENDED_Header) ==
                  (sizeof(request_id_t) + sizeof(lsn_t) + sizeof(uint64_t) +
                   sizeof(NodeID) + sizeof(Status) + sizeof(APPENDED_flags_t)),
              "APPENDED_Header has unexpected compiler inserted padding");

class APPENDED_Message : public Message {
 public:
  explicit APPENDED_Message(const APPENDED_Header& header)
      : Message(MessageType::APPENDED, TrafficClass::APPEND), header_(header) {}

  // see Message.h
  void serialize(ProtocolWriter& writer) const override;
  Disposition onReceived(const Address& from) override;

  int8_t getExecutorPriority() const override {
    return folly::Executor::HI_PRI;
  }

  static Message::deserializer_t deserialize;

  virtual std::vector<std::pair<std::string, folly::dynamic>>
  getDebugInfo() const override;

  const APPENDED_Header header_;
  // If the append was batched by the sequencer, this is the offset of the
  // append within the batch.
  folly::Optional<uint32_t> seq_batching_offset;
  // This is a hack.  At time of writing the above offset was only consumed by
  // Contest (to match up appends that get batched with the right reads).  To
  // avoid bloating the append API with the offset, we just stash it into a
  // thread-local that can be read by the Contest-supporting part of loadtest.
  static __thread uint32_t last_seq_batching_offset;
};

}} // namespace facebook::logdevice
