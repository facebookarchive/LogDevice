/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <vector>

#include "logdevice/common/OffsetMap.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/Seal.h"
#include "logdevice/common/TailRecord.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * @file  SEALED is a message sent by storage threads as a reply to
 *        sequencer's SEAL. It's a variable-sized message containing, in
 *        addition to the 'status' field a list of last known good values for
 *        all epochs in the range <last_clean_epoch, seal_epoch] (@see
 *        SEAL_Header).
 */

struct SEALED_Header {
  // id of the log this SEALED message is for
  logid_t log_id;

  // epoch of the original SEAL message; used with `log_id' to identify the
  // LogRecovery request which sent the SEAL message
  epoch_t seal_epoch;

  // result of the sealing operation
  Status status;

  lsn_t authoritative_since_deprecated = LSN_OLDEST; // Deprecated and unused.

  // number of lsn_t values following the header.
  // An additional this many uint64_t's follow,
  // forming the epoch_offset_map list.
  uint32_t lng_list_size;

  // shard that was sealed.
  shard_index_t shard;

  // since Compatibility::TAIL_RECORD_IN_SEALED

  // number of tail records attached to the message. There will be one
  // TailRecord for each epoch in the sealed range which has at least one
  // per-epoch released records. default to -1
  // (e.g., protocol < Compatibility::TAIL_RECORD_IN_SEALED)
  int32_t num_tail_records;

  // since Compatibility::TRIM_POINT_IN_SEALED
  lsn_t trim_point;

  // Return the expected size of the header given a protocol version.
  static size_t getExpectedSize(uint16_t proto);

} __attribute__((__packed__));

class SEALED_Message : public Message {
 public:
  SEALED_Message(const SEALED_Header& header,
                 std::vector<lsn_t> lng,
                 Seal seal,
                 std::vector<uint64_t> last_timestamp,
                 std::vector<OffsetMap> epoch_offset_map,
                 std::vector<lsn_t> max_seen_lsn,
                 std::vector<TailRecord> tail_records)
      : Message(MessageType::SEALED, TrafficClass::RECOVERY),
        header_(header),
        epoch_lng_(std::move(lng)),
        seal_(seal),
        last_timestamp_(std::move(last_timestamp)),
        epoch_offset_map_(std::move(epoch_offset_map)),
        max_seen_lsn_(std::move(max_seen_lsn)),
        tail_records_(std::move(tail_records)) {
    header_.lng_list_size = epoch_lng_.size();
    if (header.num_tail_records != -1 && tail_records.size() == 0) {
      // for old protocol
      header_.num_tail_records = tail_records_.size();
    }
  }

  void serialize(ProtocolWriter&) const override;
  static Message::deserializer_t deserialize;

  Disposition onReceived(const Address& from) override;

  void onSent(Status status, const Address& to) const override;

  /**
   * A helper method that constructs and sends a SEALED message with the given
   * arguments.
   */
  static void createAndSend(
      const Address& to,
      logid_t log_id,
      shard_index_t shard_idx,
      epoch_t seal_epoch,
      Status status,
      lsn_t trim_point = LSN_INVALID,
      std::vector<lsn_t> lng_list = std::vector<lsn_t>(),
      Seal seal = Seal(),
      std::vector<OffsetMap> epoch_offset_map = std::vector<OffsetMap>(),
      std::vector<uint64_t> last_timestamp = std::vector<uint64_t>(),
      std::vector<lsn_t> max_seen_lsn = std::vector<lsn_t>(),
      std::vector<TailRecord> tail_records = std::vector<TailRecord>());

  std::string toString() const;

  SEALED_Header header_;
  std::vector<lsn_t> epoch_lng_;

  // If status is PREEMPTED, contains storage node's seal record for the log,
  // including the identity of the sequencer that sealed it. Present only if
  // protocol version is >= 7.
  Seal seal_;

  // Node's view of timestamp of the last appended record.
  std::vector<uint64_t> last_timestamp_;

  // Node's view of amount of data written in seal_epoch.
  std::vector<OffsetMap> epoch_offset_map_;

  // since Compatibility::TAIL_RECORD_IN_SEALED
  // Node's view of maximum stored lsn of the epoch
  std::vector<lsn_t> max_seen_lsn_;

  // vector of tail records for each epoch that has a valid tail
  std::vector<TailRecord> tail_records_;
};

}} // namespace facebook::logdevice
