/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "ProtocolWriter.h"

#include "event2/buffer.h"
#include "event2/event.h"
#include "logdevice/common/Checksum.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/libevent/compat.h"
#include "logdevice/common/protocol/MessageTypeNames.h"

namespace facebook { namespace logdevice {

namespace {
class EvbufferDestination : public ProtocolWriter::Destination {
 public:
  int write(const void* src, size_t nbytes, size_t /*nwritten*/) override {
    // caller should check and shouldn't call this when dest isNull
    ld_check(!isNull());
    int rv = LD_EV(evbuffer_add)(cksum_evbuf_, src, nbytes);
    if (rv != 0) {
      err = E::INTERNAL;
      ld_check(false);
    }
    return rv;
  }

  int writeWithoutCopy(const void* src,
                       size_t nbytes,
                       size_t /*nwritten*/) override {
    ld_check(!isNull());
    int rv = LD_EV(evbuffer_add_reference)(
        cksum_evbuf_, src, nbytes, nullptr, nullptr);
    if (rv != 0) {
      err = E::INTERNAL;
      ld_check(false);
    }
    return rv;
  }

  int writeEvbuffer(evbuffer* src) override {
    ld_check(!isNull());
#if LIBEVENT_VERSION_NUMBER >= 0x02010100
    // In libevent >= 2.1 we can add the evbuffer by reference which should be
    // cheaper
    int rv = LD_EV(evbuffer_add_buffer_reference)(cksum_evbuf_, src);
#else
    int rv = LD_EV(evbuffer_add_buffer)(cksum_evbuf_, src);
#endif
    if (rv != 0) {
      err = E::INTERNAL;
      ld_check(false);
    }
    return rv;
  }

  void endSerialization() override {
    if (dest_evbuf_ && cksum_evbuf_) {
      // This moves all data b/w evbuffers without memory copy
      int rv = LD_EV(evbuffer_add_buffer)(dest_evbuf_, cksum_evbuf_);
      ld_check(rv == 0);
    }
  }

  uint64_t computeChecksum() override {
    uint64_t checksum = 0;
    ld_check(cksum_evbuf_);

    const size_t len = LD_EV(evbuffer_get_length)(cksum_evbuf_);
    std::string data(len, 0);
    ev_ssize_t nbytes = LD_EV(evbuffer_copyout)(cksum_evbuf_, &data[0], len);
    ld_check(nbytes == len);
    Slice slice(&data[0], nbytes);
    checksum_bytes(slice, 64, (char*)&checksum);

    return checksum;
  }

  const char* identify() const override {
    return "evbuffer destination";
  }

  bool isNull() const override {
    return dest_evbuf_ == nullptr;
  }

  explicit EvbufferDestination(evbuffer* dest) : dest_evbuf_(dest) {
    if (dest_evbuf_) {
      cksum_evbuf_ = LD_EV(evbuffer_new)();
    }
  }

  ~EvbufferDestination() {
    if (cksum_evbuf_) {
      LD_EV(evbuffer_free)(cksum_evbuf_);
      cksum_evbuf_ = nullptr;
    }
  }

 private:
  struct evbuffer* const dest_evbuf_;
  struct evbuffer* cksum_evbuf_ = nullptr;
};

class LinearBufferDestination : public ProtocolWriter::Destination {
 public:
  int write(const void* src, size_t nbytes, size_t nwritten) override {
    ld_check(!isNull());
    if (nwritten > dest_.size || dest_.size - nwritten < nbytes) {
      err = E::NOBUFS;
      return -1;
    }
    // do not perform copy if source is empty, e.g., Slice(nullptr, 0)
    if (nbytes != 0) {
      // must from a valid source
      ld_check(src != nullptr);
      memcpy((char*)dest_.data + nwritten, src, nbytes);
    }
    return 0;
  }

  int writeWithoutCopy(const void* src,
                       size_t nbytes,
                       size_t nwritten) override {
    // currently zero copy is *not* support in linear buffer destination
    // fallback to copy
    return write(src, nbytes, nwritten);
  }

  int writeEvbuffer(evbuffer* /*src*/) override {
    err = E::NOTSUPPORTED;
    return -1;
  }

  const char* identify() const override {
    return "linear buffer destination";
  }

  bool isNull() const override {
    return dest_.data == nullptr;
  }

  /* unused */
  uint64_t computeChecksum() override {
    return 0;
  }

  explicit LinearBufferDestination(Slice dest) : dest_(dest) {}

 private:
  const Slice dest_;
};
} // anonymous namespace

ProtocolWriter::ProtocolWriter(std::unique_ptr<Destination> dest,
                               std::string context,
                               uint16_t proto)
    : dest_(std::move(dest)),
      context_(std::move(context)),
      proto_(std::move(proto)) {
  ld_assert(dest_);
}

ProtocolWriter::ProtocolWriter(MessageType type,
                               struct evbuffer* dest,
                               uint16_t proto)
    : ProtocolWriter(std::make_unique<EvbufferDestination>(dest),
                     messageTypeNames[type].c_str(),
                     proto) {}

ProtocolWriter::ProtocolWriter(Slice dest, std::string context, uint16_t proto)
    : ProtocolWriter(std::make_unique<LinearBufferDestination>(dest),
                     std::move(context),
                     proto) {}

template <typename Fn>
void ProtocolWriter::writeImplCb(Fn&& fn) {
  int rv = fn();
  if (rv != 0) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "error writing %s message to %s : %s, bytes written so "
                    "far: %lu.",
                    context_.c_str(),
                    dest_->identify(),
                    error_description(err),
                    nwritten_);
    ld_check(err != E::OK);
    status_ = err;
    return;
  }
}

void ProtocolWriter::writeImpl(const void* data, size_t nbytes) {
  writeImplCb([&] { return dest_->write(data, nbytes, nwritten_); });
}

void ProtocolWriter::writeEvbuffer(evbuffer* data) {
  if (proto_ < proto_gate_) {
    return;
  }

  size_t nbytes = LD_EV(evbuffer_get_length)(data);
  if (!dest_->isNull() && ok()) {
    writeImplCb([&] { return dest_->writeEvbuffer(data); });
  }
  nwritten_ += nbytes;
}

void ProtocolWriter::writeWithoutCopy(const void* data, size_t nbytes) {
  if (proto_ < proto_gate_) {
    return;
  }
  if (!dest_->isNull() && ok()) {
    writeImplCb(
        [&] { return dest_->writeWithoutCopy(data, nbytes, nwritten_); });
  }
  nwritten_ += nbytes;
}

}} // namespace facebook::logdevice
