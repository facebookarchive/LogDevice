/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Optional.h>
#include <folly/small_vector.h>

#include "logdevice/common/CopySet.h"
#include "logdevice/common/ReadStreamAttributes.h"
#include "logdevice/common/SCDCopysetReordering.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/protocol/FixedSizeMessage.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Record.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * @file START is sent by readers to request that a server start delivering
 *       records.
 */

typedef uint32_t START_flags_t;

struct START_Header {
  logid_t log_id;                  // which log to deliver records from
  read_stream_id_t read_stream_id; // client-issued read stream ID
  lsn_t start_lsn;                 // LSN to start delivering from (inclusive)
  lsn_t until_lsn;                 // LSN to deliver to (inclusive)
  lsn_t window_high;               // see ServerReadStream::window_high
  START_flags_t flags;             // bitset of flag constants defined below
  node_index_t required_node_in_copyset; // deprecated.

  filter_version_t filter_version;
  // Number of nodes that are filtered out.
  // Set to 0 if single copy delivery is not enabled.
  copyset_size_t num_filtered_out;

  // Replication factor this client expects for records.
  // This value helps the storage node figure out that the copy it has is
  // actually an extra so it can ship it if SCD is active.
  // Set to 0 if replication is unknown.
  uint8_t replication;

  SCDCopysetReordering scd_copyset_reordering;

  shard_index_t shard;

  // The list of known down nodes follows the header...

  // If set in .flags, indicates that RECORD messages from the server should
  // include extra metadata (see RECORD_Message.h).
  static const START_flags_t INCLUDE_EXTRA_METADATA = 1u << 0; //=1

  // If set in .flags, indicates that when the server reads records for this
  // read stream, it should avoid putting them in caches.  This usually goes
  // hand in hand with the INCLUDE_EXTRA_METADATA flag as we don't want to
  // thrash caches on donor nodes when rebuilding.
  static const START_flags_t DIRECT = 1u << 1; //=2

  // If set in .flags, indicates that the storage node should send all records
  // that it has within the specified range, regardless of whether they were
  // released or not.
  static const START_flags_t IGNORE_RELEASED_STATUS = 1u << 2; //=4

  // If set in .flags, indicates that the storage node should deliver records
  // without the payload
  static const START_flags_t NO_PAYLOAD = 1u << 3; //=8

  // If set in .flags, indicates that the storage node should deliver only data
  // contained in copy set index. If this is not avilable for the log it will
  // return all data. Also use NO_PAYLOAD to fall back to records without
  // payload.
  static const START_flags_t CSI_DATA_ONLY = 1u << 4; //=16

  // If set in .flags, indicates that this read stream will be used for
  // building a record digest. It is a part of an epoch recovery procedure.
  static const START_flags_t DIGEST = 1u << 5; //=32

  // Enable single copy delivery. The storage node will only send a record if it
  // is the primary recipient for that record.
  // @see doc/single-copy-delivery.md for more information about scd.
  static const START_flags_t SINGLE_COPY_DELIVERY = 1u << 7; //=128

  // If set in .flags, the storage node should, in place of payload, send
  // 8 bytes: 4-byte payload length followed by 4-byte payload checksum.
  static const START_flags_t PAYLOAD_HASH_ONLY = 1u << 8; //=256

  // Request approximate amount of data written to log. Byte offset will be
  // send along with record once it is available on storage nodes.
  static const START_flags_t INCLUDE_BYTE_OFFSET = 1u << 9; //=512

  // Enable server-side filtering; This will allow records to be filtered
  // before sending to client.
  static const START_flags_t INCLUDE_FILTER = 1u << 10; //=1024

  // Enable local single copy delivery. The storage node will only send a record
  // if it is the primary recipient (left-most in copyset) of the record in the
  // client's region.
  static const START_flags_t LOCAL_SCD_ENABLED = 1u << 11; //=2048
} __attribute__((__packed__));

class START_Message : public Message {
 public:
  /**
   * Construct a START message.
   * @param filtered_out If single copy delivery is enabled
   *                     (START_Header::SINGLE_COPY_DELIVERY) flag is set,
   *                     filtered_out list to be sent to storage nodes. The size
   *                     of this filtered_out must be equal to
   *                     header.num_filtered_out
   *                     Must be empty if single copy delivery is not enabled.
   *
   * @param attrs      Structure containing parameters that alter
   *                   the behavior of the read stream. In
   *                   particular it is used to pass filters
   *                   for the server-side filtering  experimental feature.
   *
   * @param client_session_id Session id of client that initiated the request.
   *                          Can be used to seed copyset shuffling
   *                          (if SCD is enabled).
   */
  explicit START_Message(const START_Header& header,
                         small_shardset_t filtered_out = small_shardset_t{},
                         const ReadStreamAttributes* attrs = nullptr,
                         std::string client_session_id = "");

  // Not publicly copyable or movable.  There is a private copy constructor.
  START_Message(START_Message&&) noexcept = delete;
  START_Message& operator=(const START_Message&) = delete;
  START_Message& operator=(START_Message&&) = delete;
  // see Message.h
  void serialize(ProtocolWriter&) const override;
  Disposition onReceived(const Address&) override {
    // Receipt handler lives in server/START_onReceived.cpp; this should never
    // get called.
    std::abort();
  }
  void onSent(Status st, const Address& to) const override;
  bool warnAboutOldProtocol() const override {
    // We have highly sophisticated handling for protocol versions
    return false;
  }
  bool allowUnencrypted() const override;
  static Message::deserializer_t deserialize;

  // `proto_' only populated when receiving
  uint16_t proto_;
  START_Header header_;
  // A list of shards that are considered either down or too slow by the client.
  // Other storage shards will send records that those shards in this list were
  // supposed to send.
  small_shardset_t filtered_out_;
  // server-side filtering parameters
  ReadStreamAttributes attrs_;

  std::string client_session_id_; // session id of client that created stream
  uint64_t csid_hash_pt1 = 0;     // Session id hash, pt1
  uint64_t csid_hash_pt2 = 0;     // Session id hash, pt2

  virtual std::vector<std::pair<std::string, folly::dynamic>>
  getDebugInfo() const override;
};

}} // namespace facebook::logdevice
