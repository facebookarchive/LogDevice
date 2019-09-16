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

PayloadHolder::PayloadHolder(std::unique_ptr<folly::IOBuf> iobuf)
    : payload_flat_(Payload(iobuf->data(), iobuf->length())),
      iobuf_(std::move(iobuf)) {
  if (folly::kIsDebug) {
    ld_check(iobuf_->length() < Message::MAX_LEN);
  }
}

void PayloadHolder::reset() {
  if (iobuf_) {
    iobuf_.reset();
  } else {
    if (owned_) {
      free(const_cast<void*>(payload_flat_.data()));
    }
  }
  payload_flat_ = Payload(nullptr, 1);

  owned_ = false;
  ld_check(!valid());
}

size_t PayloadHolder::size() const {
  ld_check(valid());
  return payload_flat_.size();
}

void PayloadHolder::serialize(ProtocolWriter& writer) const {
  ld_check(payload_flat_.size() < Message::MAX_LEN); // must have been checked
                                                     // by upper layers
  writer.write(payload_flat_.data(), payload_flat_.size());
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
  return payload_flat_;
}

Payload PayloadHolder::getFlatPayload() const {
  ld_check(valid());
  return payload_flat_;
}

std::string PayloadHolder::toString() const {
  return getPayload().toString();
}

}} // namespace facebook::logdevice
