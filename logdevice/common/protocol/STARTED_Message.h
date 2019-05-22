/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/CopySet.h"
#include "logdevice/common/protocol/FixedSizeMessage.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * @file STARTED is sent by a storage node to a reader in response to a START
 * message.  Although the name suggests that reading has started successfully
 * (which is typical), this is not necessarily the case; STARTED is also sent
 * by the server to communicate an error.
 */

struct STARTED_Header {
  // Id of the log for which the recipient attempted to start a read stream.
  // Currently only used during log recovery.
  logid_t log_id;

  // client-issued read stream ID
  read_stream_id_t read_stream_id;

  // - OK: Reading started successfully, client should expect to see some
  //   records.
  // - NOTSTORAGE: Server is not a storage node.  This is a persistent error,
  //   there is no point in retrying unless the server restarts (socket
  //   closes).
  // - FAILED: Persistent error, no point in retrying unless the socket
  //   closes.
  // - AGAIN, NOTREADY, SYSLIMIT: Transient error, reader should retry.
  // - REBUILDING: Server is in rebuilding state. No point in retrying unless
  //   the socket closes.
  // - SHUTDOWN: Server is shutting down. No point in retrying unless the socket
  //   closes.
  Status status;

  // The server sends the filter_version that was present in the START message
  // back to the client because when ClientReadStream rewinds the stream by
  // sending a new START message with a bumped filter_version, it discards
  // everything we send until STARTED with the matching filter_version is
  // received. @see filter_version_ in ServerReadStream for a detailed
  // explaination of a race condition this fixes.
  filter_version_t filter_version;

  // 1) If the read stream is not used for rebuilding or recovery, this is
  // the last released lsn at the moment of receving the START message.
  // Currently used for optimizing metadata fetching during reading. It is
  // a best effort: if last_released_lsn is not in memory, this will be
  // LSN_INVALID.
  //
  // 2) If the read stream is used for recovery (digest), and the stream is
  // served from record cache (CachedDigest), this will be the last known
  // good LSN of the digesting epoch when START is received, this is used
  // by epoch recovery with a stale LNG to update its LNG and not treating
  // records not delivered by record cache missing as they are actually evicted
  // by LNG updates.
  //
  // 3) for all other cases, this is LSN_INVALID
  lsn_t last_released_lsn;

  shard_index_t shard;
} __attribute__((__packed__));

class STARTED_Message : public Message {
 public:
  // identifies the origin of the started response.
  enum class Source { LOCAL_LOG_STORE, CACHED_DIGEST };

  explicit STARTED_Message(const STARTED_Header&,
                           TrafficClass,
                           Source source = Source::LOCAL_LOG_STORE,
                           lsn_t starting_read_ptr = LSN_INVALID);

  STARTED_Message(STARTED_Message&&) noexcept = delete;
  STARTED_Message& operator=(const STARTED_Message&) = delete;
  STARTED_Message& operator=(STARTED_Message&&) = delete;

  // see Message.h
  void serialize(ProtocolWriter&) const override;
  Disposition onReceived(const Address& from) override;
  static Message::deserializer_t deserialize;

  STARTED_Header header_;

  // Not serialized and only used when sending the message.
  Source source_;
  lsn_t starting_read_ptr_;

  std::string toString() const;

  virtual std::vector<std::pair<std::string, folly::dynamic>>
  getDebugInfo() const override;
};

}} // namespace facebook::logdevice
