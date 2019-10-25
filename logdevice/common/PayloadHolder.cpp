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
PayloadHolder::PayloadHolder(const void* buf,
                             size_t size,
                             bool ignore_size_limit) {
  if (!ignore_size_limit) {
    ld_check(size == 0 || buf != nullptr);
    ld_check(size < Message::MAX_LEN);
  }

  if (size > 0) {
    iobuf_ = folly::IOBuf::takeOwnership(const_cast<void*>(buf), size);
  } else {
    iobuf_ = folly::IOBuf::create(size);
  }
  ld_check(iobuf_);
}

PayloadHolder ::PayloadHolder(std::unique_ptr<folly::IOBuf> iobuf)
    : iobuf_(std::move(iobuf)) {
  if (folly::kIsDebug) {
    ld_check(size() < Message::MAX_LEN);
  }
}

PayloadHolder::PayloadHolder(const Payload& payload, unowned_t) {
  iobuf_ = folly::IOBuf::wrapBuffer(payload.data(), payload.size());
}

PayloadHolder& PayloadHolder::operator=(PayloadHolder&& other) noexcept {
  if (this != &other) {
    iobuf_ = std::move(other.iobuf_);
    other.reset();
  }
  return *this;
}

void PayloadHolder::reset() {
  iobuf_.reset();
  ld_check(!valid());
}

size_t PayloadHolder::size() const {
  ld_check(valid());
  return iobuf_->computeChainDataLength();
}

void PayloadHolder::serialize(ProtocolWriter& writer) const {
  ld_check(size() < Message::MAX_LEN); // must have been checked
                                       // by upper layers
  if (owner()) {
    writer.writeWithoutCopy(iobuf_.get());
  } else {
    Payload payload = getFlatPayload();
    writer.write(payload.data(), payload.size());
  }
}

/* static */
PayloadHolder PayloadHolder::deserialize(ProtocolReader& reader,
                                         size_t payload_size) {
  if (reader.error()) {
    return PayloadHolder();
  }

  if (payload_size == 0) {
    return PayloadHolder{nullptr, 0};
  }

  ld_check(payload_size > 0);
  auto iobuf = reader.readIntoIOBuf(payload_size);
  if (!reader.error()) {
    return PayloadHolder(std::move(iobuf));
  }

  ld_check(reader.error());
  return PayloadHolder();
}

void PayloadHolder::TEST_corruptPayload() {
  if (!folly::kIsDebug) {
    RATELIMIT_CRITICAL(std::chrono::seconds(60),
                       1,
                       "Please disable flag to corrupt STORE payloads");
    return;
  }

  Payload corrupted = getPayload().dup();
  // Flip the last bit, as the user defined payload is last
  *((unsigned char*)corrupted.data() + corrupted.size() - 1) ^= 1;
  *this = PayloadHolder(corrupted.data(), corrupted.size());
}

Payload PayloadHolder::getPayload() const {
  size_t payload_size = 0;
  if (!valid() || (payload_size = size()) == 0) {
    return Payload();
  }
  return Payload(iobuf_->data(), payload_size);
}

Payload PayloadHolder::getFlatPayload() const {
  ld_check(valid());
  size_t payload_size = size();
  if (payload_size == 0) {
    return Payload();
  }
  iobuf_->coalesce();
  ld_check_eq(iobuf_->length(), payload_size);
  return Payload(iobuf_->data(), payload_size);
}

std::string PayloadHolder::toString() const {
  return getPayload().toString();
}

}} // namespace facebook::logdevice
