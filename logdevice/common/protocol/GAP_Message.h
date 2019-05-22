/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>

#include "logdevice/common/protocol/FixedSizeMessage.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/EnumMap.h"
#include "logdevice/include/Record.h"

namespace facebook { namespace logdevice {

/**
 * @file GAP is the message sent by a storage node to clients to report that
 *       the node doesn't have any records in the interval [start_lsn, end_lsn].
 *
 *       Note that clients assume that no records with sequence numbers smaller
 *       than start_lsn will be delievered after this message.
 */

/**
 * Reason for the gap, from a storage node's perspective.
 */
enum class GapReason : uint8_t {
  // Log trimmed up to the specified lsn
  TRIM = 0,
  // There are no records in the specified range
  NO_RECORDS = 1,
  // Record failed checksum
  CHECKSUM_FAIL = 2,
  // No longer used but keeping the value reserved to avoid confusing old
  // clients
  EPOCH_NOT_CLEAN_DEPRECATED = 3,
  // Equivalent to NO_RECORDS, but the storage node knows that it may
  // be missing records near the current read position.
  UNDER_REPLICATED = 4,
  // Supported when protocol >= Compatibility::SERVER_CAN_FILTER
  // ServerReadStream send this GAP when records are filtered out on the
  // server-side.[Experimental Feature]
  FILTERED_OUT = 5,
  // The number of valid GapReason constants.
  MAX,
  INVALID
};

typedef uint8_t GAP_flags_t;

extern EnumMap<GapReason, std::string> gap_reason_names;

struct GAP_Header {
  logid_t log_id;
  read_stream_id_t read_stream_id;
  lsn_t start_lsn;
  lsn_t end_lsn;
  GapReason reason;
  GAP_flags_t flags;
  shard_index_t shard;

  // This is a digest GAP message. It is a part of a read stream that
  // was created by a START message with the DIGEST flag set. Digest
  // gaps are used in epoch recovery.
  static const GAP_flags_t DIGEST = 1 << 5; //=32

  /**
   * @return a human-readable string with the GAP's log id, lsn range,
   *         and the reason for the gap.
   */
  std::string identify() const;
} __attribute((packed));

class GAP_Message : public Message {
 public:
  // identifies the origin of the gap
  enum class Source { LOCAL_LOG_STORE, CACHED_DIGEST };

  explicit GAP_Message(const GAP_Header&,
                       TrafficClass,
                       Source source = Source::LOCAL_LOG_STORE);

  // see Message.h
  void serialize(ProtocolWriter&) const override;
  Disposition onReceived(const Address& from) override;
  static Message::deserializer_t deserialize;

  const GAP_Header& getHeader() const {
    return header_;
  }

  std::string identify() const {
    return header_.identify();
  }

  GAP_Header header_;

  // used only on the sending end
  Source source_;

  virtual std::vector<std::pair<std::string, folly::dynamic>>
  getDebugInfo() const override;
};

}} // namespace facebook::logdevice
