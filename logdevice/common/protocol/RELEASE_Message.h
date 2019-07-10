/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstdint>
#include <string>

#include "logdevice/common/RecordID.h"
#include "logdevice/common/configuration/TrafficClass.h"
#include "logdevice/common/protocol/FixedSizeMessage.h"

namespace facebook { namespace logdevice {

enum class ReleaseType : uint8_t { GLOBAL, PER_EPOCH, INVALID };

struct RELEASE_Header {
  // All records in epoch rid.epoch of log rid.logid with ESNs up to and
  // including rid.esn are eligible for delivery to readers.
  // If GLOBAL release, all records of epochs < rid.epoch are also eligible for
  // delivery (global last released lsn has moved).
  RecordID rid;

  // Type of release event.
  ReleaseType release_type;

  // shard this RELEASE is for. May not be provided if the protocol is too old.
  shard_index_t shard;

  // size of the header in message given the protocol version
  static size_t headerSize(uint16_t proto) {
    return sizeof(RELEASE_Header);
  }
} __attribute__((__packed__));

class RELEASE_Message : public Message {
 public:
  explicit RELEASE_Message(const RELEASE_Header& header);

  RELEASE_Message(const RELEASE_Message&) noexcept = delete;
  RELEASE_Message(RELEASE_Message&&) noexcept = delete;
  RELEASE_Message& operator=(const RELEASE_Message&) = delete;
  RELEASE_Message& operator=(RELEASE_Message&&) = delete;

  // see Message.h
  void serialize(ProtocolWriter&) const override;
  Disposition onReceived(const Address&) override {
    // Receipt handler lives in PurgeCoordinator::onReceived(); this should
    // never get called.
    std::abort();
  }

  void onSent(Status st, const Address& to) const override;
  static Message::deserializer_t deserialize;

  bool warnAboutOldProtocol() const override;

  const RELEASE_Header& getHeader() const {
    return header_;
  }

  virtual std::vector<std::pair<std::string, folly::dynamic>>
  getDebugInfo() const override;

  RELEASE_Header header_;
};

const std::string& release_type_to_string(ReleaseType release_type);

}} // namespace facebook::logdevice
