/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/Message.h"

#include <memory>

#include <folly/io/IOBuf.h>

#include "logdevice/common/Address.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/MessageTypeNames.h"
#include "logdevice/common/protocol/ProtocolWriter.h"

namespace facebook { namespace logdevice {

void Message::onSent(Status st,
                     const Address& to,
                     const SteadyTimestamp enqueue_time) const {
  ld_log(st == E::OK ? dbg::Level::SPEW : dbg::Level::DEBUG,
         ": message=%s st=%s to=%s msg_size=%zu enqueue_time=%s",
         messageTypeNames()[type_].c_str(),
         error_name(st),
         to.toString().c_str(),
         // Sender::describeConnection(to).c_str(),
         size(),
         toString(enqueue_time).c_str());
  // Support legacy message handlers.
  onSent(st, to);
}

size_t Message::size(uint16_t proto) const {
  ProtocolWriter writer(type_, static_cast<folly::IOBuf*>(nullptr), proto);
  serialize(writer);
  ssize_t size = writer.result();
  ld_check(size >= 0);
  return ProtocolHeader::bytesNeeded(type_, proto) + size;
}

std::unique_ptr<folly::IOBuf> Message::serialize(uint16_t protocol,
                                                 bool checksum_enabled) const {
  const bool compute_checksum =
      ProtocolHeader::needChecksumInHeader(type_, protocol) && checksum_enabled;

  const size_t protohdr_bytes = ProtocolHeader::bytesNeeded(type_, protocol);
  auto io_buf = folly::IOBuf::create(IOBUF_ALLOCATION_UNIT);
  if (protohdr_bytes > IOBUF_ALLOCATION_UNIT) {
    ld_check(0);
    err = E::INTERNAL;
    return nullptr;
  }
  io_buf->advance(protohdr_bytes);

  ProtocolWriter writer(type_, io_buf.get(), protocol);

  serialize(writer);
  ssize_t bodylen = writer.result();
  if (bodylen <= 0) { // unlikely
    RATELIMIT_CRITICAL(std::chrono::seconds(1),
                       2,
                       "INTERNAL ERROR: Failed to serialize a message of "
                       "type %s",
                       messageTypeNames()[type_].c_str());
    ld_check(0);
    err = E::INTERNAL;
    return nullptr;
  }

  ProtocolHeader protohdr;
  protohdr.cksum = compute_checksum ? writer.computeChecksum() : 0;
  protohdr.type = type_;
  io_buf->prepend(protohdr_bytes);
  protohdr.len = io_buf->computeChainDataLength();

  memcpy(static_cast<void*>(io_buf->writableData()), &protohdr, protohdr_bytes);
  return io_buf;
}

}} // namespace facebook::logdevice
