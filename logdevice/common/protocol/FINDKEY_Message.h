/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Optional.h>

#include "logdevice/common/RecordID.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/protocol/FixedSizeMessage.h"

namespace facebook { namespace logdevice {

/**
 * @file Message sent by the client library to a storage node while processing
 * an application's call to the findTime() or findKey() API.  Asks for the
 * storage node's perspective on what the answer is, based on the contents of
 * its local log store.
 *
 * See FindTimeRequest for a detailed explanation of the algorithm and
 * protocol.
 */

typedef uint8_t FINDKEY_flags_t;

// In the case of findKey(), the header is followed by the key.
struct FINDKEY_Header {
  request_id_t client_rqid; // id of FindTimeRequest to deliver reply to
  logid_t log_id;
  int64_t timestamp;

  FINDKEY_flags_t flags; // bitset of flags.

  // With this flag turned on findTime() (or findKey()) will perform faster but
  // will return approximate result lsn which may be smaller than biggest lsn
  // for which timestamp (or key) is <= given timestamp (or key). Please read
  // FindTimeRequest.h for more information.
  static const FINDKEY_flags_t APPROXIMATE = 1 << 0; //=1
  // With this flag turned on, the findKey() request will be performed instead,
  // for the target key provided in the message.
  static const FINDKEY_flags_t USER_KEY = 1 << 1; //=2

  // Only search within the following range.
  lsn_t hint_lo;
  lsn_t hint_hi;

  int64_t timeout_ms; // FindTime timeout in milliseconds.
                      // The Node should skip handling this
                      // message if it hasn't started to execute it
                      // timeout_ms milliseconds after receiving it.
                      // 0 means no timeout.

  shard_index_t shard; // Shard on which to look for this log.

  // Return the expected size of the header given a protocol version.
  static size_t getExpectedSize(uint16_t proto);

} __attribute__((__packed__));

class FINDKEY_Message : public Message {
 public:
  explicit FINDKEY_Message(const FINDKEY_Header& header,
                           folly::Optional<std::string> key)
      : Message(MessageType::FINDKEY, TrafficClass::READ_BACKLOG),
        header_(header),
        key_(std::move(key)) {}

  void serialize(ProtocolWriter&) const override;
  static Message::deserializer_t deserialize;

  Disposition onReceived(const Address&) override {
    // Receipt handler lives in server/FINDKEY_onReceived.cpp; this should
    // never get called.
    std::abort();
  }
  void onSent(Status st, const Address& to) const override;

  int8_t getExecutorPriority() const override {
    return folly::Executor::LO_PRI;
  }

  FINDKEY_Header header_;

  // The key provided by the client.
  folly::Optional<std::string> key_;
};

}} // namespace facebook::logdevice
