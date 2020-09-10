/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/thrift/compat/ThriftMessageSerializer.h"

#include <cstddef>

#include <folly/Optional.h>
#include <folly/io/Cursor.h>

#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/MessageDeserializers.h"
#include "logdevice/common/protocol/MessageTypeNames.h"
#include "logdevice/common/protocol/ProtocolHeader.h"
#include "logdevice/common/protocol/ProtocolReader.h"

namespace facebook { namespace logdevice {

namespace {
constexpr size_t kHeaderSize = sizeof(ProtocolHeader);
} // namespace

std::unique_ptr<thrift::Message>
ThriftMessageSerializer::toThrift(const Message& message,
                                  uint16_t protocol) const {
  // Checksumming is deprecated, so always pass false when serializing to Thrift
  std::unique_ptr<folly::IOBuf> serialized =
      message.serialize(protocol, /*checksum_enabled*/ false);
  if (!serialized) {
    // err is set during serialization
    return nullptr;
  }
  auto result = std::make_unique<thrift::Message>();
  thrift::LegacyMessageWrapper wrapper;
  wrapper.set_payload(std::move(serialized));
  result->set_legacyMessage(std::move(wrapper));
  return result;
}

std::unique_ptr<Message>
ThriftMessageSerializer::fromThrift(thrift::Message&& source,
                                    uint16_t protocol) {
  ld_check(source.getType() == thrift::Message::Type::legacyMessage);
  thrift::LegacyMessageWrapper wrapper = source.move_legacyMessage();
  std::unique_ptr<folly::IOBuf> payload = std::move(wrapper).get_payload();
  // Read and validate header
  folly::Optional<ProtocolHeader> header = readHeader(payload.get());
  if (!header) {
    err = E::BADMSG;
    return nullptr;
  }
  if (!validateHeader(*header)) {
    err = E::BADMSG;
    return nullptr;
  }
  // Read and validate message body
  ProtocolReader reader(header->type, std::move(payload), protocol);
  auto result = readMessage(*header, reader);
  if (!result) {
    onError(*header, protocol);
    return nullptr;
  }
  return result;
}

folly::Optional<ProtocolHeader>
ThriftMessageSerializer::readHeader(folly::IOBuf* source) {
  ld_check(source);
  folly::io::Cursor cursor(source);
  if (!cursor.canAdvance(kHeaderSize)) {
    ld_error("PROTOCOL ERROR: Ran out of bytes while trying to read header, "
             "context: %s. Tried to read %zu more but only %zu available.",
             context_.c_str(),
             kHeaderSize,
             cursor.totalLength());
    err = E::BADMSG;
    return folly::none;
  }
  ProtocolHeader header;
  cursor.pull(&header, kHeaderSize);
  source->trimStart(kHeaderSize);
  return header;
}

bool ThriftMessageSerializer::validateHeader(const ProtocolHeader& header) {
  size_t min_size = kHeaderSize - sizeof(ProtocolHeader::cksum);

  if (header.len <= min_size) {
    ld_error("PROTOCOL ERROR: got message length %u, expected at least %zu "
             "bytes given sizeof(ProtocolHeader)=%zu, context: %s",
             header.len,
             min_size + 1,
             kHeaderSize,
             context_.c_str());
    err = E::BADMSG;
    return false;
  }

  size_t size = ProtocolHeader::bytesNeeded(header.type, 0 /* unused */);

  if (header.len > Message::MAX_LEN + size) {
    err = E::BADMSG;
    ld_error("PROTOCOL ERROR: got invalid message length %u for msg:%s. "
             "Expected at most %u bytes. min_protohdr_bytes:%zu, context: %s",
             header.len,
             messageTypeNames()[header.type].c_str(),
             Message::MAX_LEN,
             min_size,
             context_.c_str());
    return false;
  }
  return true;
}

std::unique_ptr<Message>
ThriftMessageSerializer::readMessage(const ProtocolHeader& header,
                                     ProtocolReader& reader) {
  Message::deserializer_t* deserializer = messageDeserializers[header.type];
  if (deserializer == nullptr) {
    ld_error("PROTOCOL ERROR: got an unknown message type '%c' (%d), "
             "context: %s.",
             int(header.type),
             int(header.type),
             context_.c_str());
    err = E::BADMSG;
    return nullptr;
  }
  return deserializer(reader).msg;
}

void ThriftMessageSerializer::onError(const ProtocolHeader& header,
                                      uint16_t protocol) {
  switch (err) {
    case E::TOOBIG:
      RATELIMIT_ERROR(
          std::chrono::seconds(1),
          1,
          "PROTOCOL ERROR: message of type %s is too large: %u bytes, "
          "context: %s.",
          messageTypeNames()[header.type].c_str(),
          header.len,
          context_.c_str());
      err = E::BADMSG;
      break;

    case E::BADMSG:
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      1,
                      "PROTOCOL ERROR: message of type %s has invalid format. "
                      "proto_:%hu, context: %s.",
                      messageTypeNames()[header.type].c_str(),
                      protocol,
                      context_.c_str());
      err = E::BADMSG;
      break;

    case E::INTERNAL:
      RATELIMIT_CRITICAL(
          std::chrono::seconds(1),
          1,
          "INTERNAL ERROR while deserializing a message of type %s, "
          "context: %s.",
          messageTypeNames()[header.type].c_str(),
          context_.c_str());
      break;

    case E::NOTSUPPORTED:
      RATELIMIT_CRITICAL(
          std::chrono::seconds(1),
          1,
          "INTERNAL ERROR: deserializer for message type %d (%s) not "
          "implemented, context: %s.",
          int(header.type),
          messageTypeNames()[header.type].c_str(),
          context_.c_str());
      ld_check(false);
      err = E::INTERNAL;
      break;

    default:
      RATELIMIT_CRITICAL(
          std::chrono::seconds(1),
          1,
          "INTERNAL ERROR: unexpected error code %d (%s) from "
          "deserializer for message type %s received, context: %s.",
          static_cast<int>(err),
          error_name(err),
          messageTypeNames()[header.type].c_str(),
          context_.c_str());
      err = E::INTERNAL;
      break;
  }
}

}} // namespace facebook::logdevice
