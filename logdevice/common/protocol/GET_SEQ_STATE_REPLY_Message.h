/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstddef>
#include <cstdint>

#include "logdevice/common/EpochMetaDataMap.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/TailRecord.h"
#include "logdevice/common/configuration/TrafficClass.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/protocol/MessageType.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/LogTailAttributes.h"
#include "logdevice/include/types.h"

/**
 * @file This is a reply to GET_SEQ_STATE. It contains last released LSN for a
 *       log, as well as the next sequence number that will be assigned to a
 *       record. Since GET_SEQ_STATE may be sent to the wrong node, this reply
 *       also includes the status, as well as an optional redirect.
 */

namespace facebook { namespace logdevice {

class ProtocolWriter;
struct Address;

// the type of GET_SEQ_STATE_REPLY_Header::flags
typedef uint32_t GET_SEQ_STATE_REPLY_flags_t;

struct GET_SEQ_STATE_REPLY_Header {
  logid_t log_id;
  lsn_t last_released_lsn;
  lsn_t next_lsn;

  // A bitset of flag constants defined below.
  GET_SEQ_STATE_REPLY_flags_t flags;

  // if set in .flags, the reply will contain sequencer tail attributes
  static const GET_SEQ_STATE_REPLY_flags_t INCLUDES_TAIL_ATTRIBUTES = 1 << 0;

  // if set in .flags, the reply will contain current epoch offset
  static const GET_SEQ_STATE_REPLY_flags_t INCLUDES_EPOCH_OFFSET = 1 << 1;

  // if set in .flags, the reply will contain historical metadata
  static const GET_SEQ_STATE_REPLY_flags_t INCLUDES_HISTORICAL_METADATA = 1
      << 2;

  // if set in .flags, the reply will contain tail record (with payload for
  // tail optimized log
  static const GET_SEQ_STATE_REPLY_flags_t INCLUDES_TAIL_RECORD = 1 << 3;

  // if set in .flags, if the reply status is E::OK, it will contain a bool
  // indicating whether the log is empty.
  static const GET_SEQ_STATE_REPLY_flags_t INCLUDES_IS_LOG_EMPTY = 1 << 4;
} __attribute__((__packed__));

class GET_SEQ_STATE_REPLY_Message : public Message {
 public:
  explicit GET_SEQ_STATE_REPLY_Message(const GET_SEQ_STATE_REPLY_Header& hdr)
      : Message(MessageType::GET_SEQ_STATE_REPLY, TrafficClass::RECOVERY),
        header_(hdr) {}

  // implementation of the Message interface
  void serialize(ProtocolWriter&) const override;
  static Message::deserializer_t deserialize;
  Disposition onReceived(const Address& from) override;

  // header included in every message.
  GET_SEQ_STATE_REPLY_Header header_;

  // id of the original request; included if proto >= 8
  request_id_t request_id_{request_id_t(-1)};

  // Status of the GET_SEQ_STATE operation. Possible values are:
  //    OK            The node has an active sequencer for the log
  //    REDIRECTED    A different node is running a sequencer for this log
  //    PREEMPTED     This sequencer was preempted for this log by some other
  //                  sequencer
  //    AGAIN         The node is responsible for this log, but the sequencer
  //                  is not ready yet(e.g. recovery is not complete)
  //    NOSEQUENCER   Sequencer for the log doesn't exist yet
  //    NOTREADY      The node is not ready e.g. it is still coming up or it is
  //                  not a sequencer node.
  //    SHUTDOWN      The node is not accepting work, possibly because it is
  //                  shutting down.
  //    REBUILDING    The node is rebuilding local log storage.
  //    FAILED        Sequencer activation failed for some other reason
  //                  e.g. due to E::SYSLIMIT, E::NOBUFS, E::TOOMANY(too many
  //                  activations).
  //    NOTFOUND      The log ID is not in config.
  // next_lsn and last_released_lsn will be set by the sender
  // only if status is E::OK.
  // Otherwise, these values will be LSN_INVALID
  Status status_{E::OK};

  // node that runs the sequencer for the log; valid only if status is either
  // E::REDIRECTED or E::PREEMPTED; included if proto >= 8
  NodeID redirect_{};

  LogTailAttributes tail_attributes_;

  OffsetMap epoch_offsets_;

  std::shared_ptr<const EpochMetaDataMap> metadata_map_;

  std::shared_ptr<TailRecord> tail_record_;

  bool is_log_empty_{false};

  virtual std::vector<std::pair<std::string, folly::dynamic>>
  getDebugInfo() const override;
};

}} // namespace facebook::logdevice
