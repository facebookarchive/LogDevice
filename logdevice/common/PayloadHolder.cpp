/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/PayloadHolder.h"

#include <cstdlib>
#include <event.h>

#include <event2/buffer.h>

#include "logdevice/common/EventLoop.h"
#include "logdevice/common/libevent/compat.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"
namespace facebook { namespace logdevice {
PayloadHolder::PayloadHolder(take_ownership_t,
                             void* buf,
                             size_t size,
                             bool ignore_size_limit)
    : iobuf_(folly::IOBuf::TAKE_OWNERSHIP, buf, size) {
  ld_check(!iobuf_.isChained());
  ld_check(iobuf_.data() == nullptr || iobuf_.isManaged());
  if (!ignore_size_limit) {
    ld_check(size == 0 || buf != nullptr);
    ld_check(size < Message::MAX_LEN);
  }
}

PayloadHolder::PayloadHolder(folly::IOBuf&& iobuf) : iobuf_(std::move(iobuf)) {
  ld_check(!iobuf_.isChained());
  ld_check(iobuf_.data() == nullptr || iobuf_.isManaged());
  if (folly::kIsDebug) {
    ld_check(size() < Message::MAX_LEN);
  }
}

PayloadHolder::PayloadHolder(copy_buffer_t, const Payload& payload)
    : iobuf_(folly::IOBuf::COPY_BUFFER, payload.data(), payload.size()) {
  ld_check(!iobuf_.isChained());
  ld_check(iobuf_.data() == nullptr || iobuf_.isManaged());
}
PayloadHolder::PayloadHolder(copy_buffer_t, const void* buf, size_t size)
    : iobuf_(folly::IOBuf::COPY_BUFFER, buf, size) {
  ld_check(!iobuf_.isChained());
  ld_check(iobuf_.data() == nullptr || iobuf_.isManaged());
}

void PayloadHolder::reset() {
  // Note that we can't use IOBuf::clear() here because it doesn't free/unlink
  // the memory, only adjusts the data pointer and length.
  iobuf_ = folly::IOBuf();
  ld_check(!iobuf_.isChained());
  ld_check(iobuf_.data() == nullptr || iobuf_.isManaged());
}

size_t PayloadHolder::size() const {
  return iobuf_.computeChainDataLength();
}

void PayloadHolder::serialize(ProtocolWriter& writer) const {
  ld_check(size() < Message::MAX_LEN); // must have been checked
                                       // by upper layers
  if (size() != 0) {
    writer.writeWithoutCopy(&iobuf_);
  }
}

/* static */
PayloadHolder PayloadHolder::deserialize(ProtocolReader& reader,
                                         size_t payload_size) {
  if (reader.error()) {
    return PayloadHolder();
  }

  if (payload_size == 0) {
    return PayloadHolder();
  }

  ld_check(payload_size > 0);
  PayloadHolder p;
  reader.readIOBuf(&p.iobuf_, payload_size);
  ld_check_eq(p.size(), reader.error() ? 0ul : payload_size);

  return p;
}

void PayloadHolder::TEST_corruptPayload() {
  if (size() == 0) {
    *this = copyString("a");
    return;
  }

  *this = copyPayload(getPayload());

  // Flip the last bit, as the user defined payload is last
  *(iobuf_.writableTail() - 1) ^= 1;
}

Payload PayloadHolder::getPayload() const {
  ld_check(!iobuf_.isChained());
  ld_check(iobuf_.data() == nullptr || iobuf_.isManaged());
  size_t payload_size = iobuf_.length();
  return payload_size == 0 ? Payload() : Payload(iobuf_.data(), payload_size);
}

std::string PayloadHolder::toString() const {
  return getPayload().toString();
}

}} // namespace facebook::logdevice
