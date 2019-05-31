/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/PayloadHolder.h"

#include <cstdlib>

#include <event2/buffer.h>

#include "logdevice/common/EventLoop.h"
#include "logdevice/common/libevent/compat.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"
namespace facebook { namespace logdevice {

PayloadHolder::PayloadHolder(struct evbuffer* payload)
    : payload_flat_(Payload(nullptr, 1)),
      payload_evbuffer_(
          ZeroCopyPayload::create(EventLoop::onThisThread(), payload)) {
  if (folly::kIsDebug) {
    ld_check(payload);
    size_t payload_size = LD_EV(evbuffer_get_length)(payload);
    ld_check(payload_size < Message::MAX_LEN);
  }

  size_t size = payload_evbuffer_->length();
  payload_flat_ =
      Payload(LD_EV(evbuffer_pullup)(payload_evbuffer_->get(), size), size);

  // This check may not be necessary, but we maintain the semantics previously.
  ld_check(ThreadID::isWorker());
}

void PayloadHolder::reset() {
  if (payload_evbuffer_) {
    payload_evbuffer_ = nullptr;
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
                                         size_t payload_size,
                                         bool zero_copy) {
  if (reader.error()) {
    return PayloadHolder();
  }

  if (zero_copy) {
    // shouldn't use zero_copy with zero payload size
    ld_check(payload_size > 0);
    // Payload is large.  Zero-copy the payload into an evbuffer.
    struct evbuffer* payload_evbuf = LD_EV(evbuffer_new)();
    if (!payload_evbuf) { // unlikely
      throw std::bad_alloc();
    }
    reader.readEvbuffer(payload_evbuf, payload_size);
    if (!reader.error()) {
      return PayloadHolder(payload_evbuf);
    }
  } else if (payload_size == 0) {
    return PayloadHolder{nullptr, 0};
  } else {
    void* payload_flat = malloc(payload_size);
    if (payload_flat == nullptr) {
      throw std::bad_alloc();
    }
    reader.read(payload_flat, payload_size);
    if (!reader.error()) {
      return PayloadHolder{payload_flat, payload_size};
    }
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

Payload PayloadHolder::getPayload() {
  return payload_flat_;
}

Payload PayloadHolder::getFlatPayload() const {
  ld_check(valid());
  ld_check(payload_evbuffer_ == nullptr);
  return payload_flat_;
}

std::string PayloadHolder::toString() {
  return getPayload().toString();
}

}} // namespace facebook::logdevice
