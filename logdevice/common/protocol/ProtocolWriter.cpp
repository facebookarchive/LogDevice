/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/ProtocolWriter.h"

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
    int rv = LD_EV(evbuffer_add)(dest_evbuf_, src, nbytes);
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
        dest_evbuf_, src, nbytes, nullptr, nullptr);
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
    int rv = LD_EV(evbuffer_add_buffer_reference)(dest_evbuf_, src);
#else
    int rv = LD_EV(evbuffer_add_buffer)(dest_evbuf_, src);
#endif
    if (rv != 0) {
      err = E::INTERNAL;
      ld_check(false);
    }
    return rv;
  }

  uint64_t computeChecksum() override {
    uint64_t checksum = 0;
    ld_check(dest_evbuf_);

    const size_t len = LD_EV(evbuffer_get_length)(dest_evbuf_);
    std::string data(len, 0);
    ev_ssize_t nbytes = LD_EV(evbuffer_copyout)(dest_evbuf_, &data[0], len);
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

  explicit EvbufferDestination(evbuffer* dest) : dest_evbuf_(dest) {}

  ~EvbufferDestination() override {}

 private:
  struct evbuffer* const dest_evbuf_;
};

class LinearBufferDestinationBase : public ProtocolWriter::Destination {
 public:
  static int writeBuffer(const void* src,
                         size_t nbytes,
                         size_t nwritten,
                         Slice dest) {
    if (!checkBufferSize(dest.size, nbytes, nwritten)) {
      err = E::NOBUFS;
      return -1;
    }
    // do not perform copy if source is empty, e.g., Slice(nullptr, 0)
    if (nbytes != 0) {
      // must from a valid source
      ld_check(src != nullptr);
      memcpy((char*)dest.data + nwritten, src, nbytes);
    }
    return 0;
  }

  static bool checkBufferSize(size_t max_size,
                              size_t to_write,
                              size_t written) {
    return written <= max_size && max_size - written >= to_write;
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

  /* unused */
  uint64_t computeChecksum() override {
    return 0;
  }

  explicit LinearBufferDestinationBase() = default;
};

class LinearBufferDestination : public LinearBufferDestinationBase {
 public:
  int write(const void* src, size_t nbytes, size_t nwritten) override {
    ld_check(!isNull());
    return writeBuffer(src, nbytes, nwritten, dest_);
  }

  const char* identify() const override {
    return "linear buffer destination";
  }

  bool isNull() const override {
    return dest_.data == nullptr;
  }

  explicit LinearBufferDestination(Slice dest)
      : LinearBufferDestinationBase(), dest_(dest) {}

 private:
  const Slice dest_;
};

class StringBufferDestination : public LinearBufferDestinationBase {
 public:
  int write(const void* src, size_t nbytes, size_t nwritten) override {
    ld_check(!isNull());
    if (!checkBufferSize(max_size_, nbytes, nwritten)) {
      err = E::NOBUFS;
      return -1;
    }

    dest_->resize(nbytes + nwritten);
    return writeBuffer(
        src, nbytes, nwritten, Slice(dest_->data(), dest_->size()));
  }

  const char* identify() const override {
    return "string buffer destination";
  }

  bool isNull() const override {
    return dest_ == nullptr;
  }

  explicit StringBufferDestination(
      std::string* dest,
      size_t max_size = std::numeric_limits<size_t>::max())
      : LinearBufferDestinationBase(), dest_(dest), max_size_(max_size) {}

 private:
  std::string* const dest_;
  const size_t max_size_;
};

} // anonymous namespace

ProtocolWriter::ProtocolWriter(Destination* dest,
                               const char* context,
                               folly::Optional<uint16_t> proto)
    : dest_(dest), context_(context), proto_(std::move(proto)) {
  ld_assert(dest_);
}

ProtocolWriter::ProtocolWriter(MessageType type,
                               struct evbuffer* dest,
                               folly::Optional<uint16_t> proto)
    : dest_owned_(true),
      dest_(new (dest_space_) EvbufferDestination(dest)),
      context_(messageTypeNames()[type].c_str()),
      proto_(proto) {
  static_assert(sizeof(dest_space_) >= sizeof(EvbufferDestination), "");
}

ProtocolWriter::ProtocolWriter(Slice dest,
                               std::string context,
                               folly::Optional<uint16_t> proto)
    : dest_owned_(true),
      context_owned_(std::move(context)),
      dest_(new (dest_space_) LinearBufferDestination(dest)),
      context_(context_owned_.c_str()),
      proto_(proto) {
  static_assert(sizeof(dest_space_) >= sizeof(LinearBufferDestination), "");
}

ProtocolWriter::ProtocolWriter(std::string* dest,
                               std::string context,
                               folly::Optional<uint16_t> proto,
                               folly::Optional<size_t> max_size)
    : dest_owned_(true),
      context_owned_(std::move(context)),
      dest_(new (dest_space_) StringBufferDestination(
          dest,
          max_size.value_or(std::numeric_limits<size_t>::max()))),
      context_(context_owned_.c_str()),
      proto_(proto) {
  static_assert(sizeof(dest_space_) >= sizeof(StringBufferDestination), "");
}

ProtocolWriter::~ProtocolWriter() {
  if (dest_owned_) {
    dest_->~Destination();
  }
}

template <typename Fn>
void ProtocolWriter::writeImplCb(Fn&& fn) {
  int rv = fn();
  if (rv != 0) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "error writing %s message to %s : %s, bytes written so "
                    "far: %lu.",
                    context_,
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
  if (!isProtoVersionAllowed()) {
    return;
  }

  size_t nbytes = LD_EV(evbuffer_get_length)(data);
  if (!dest_->isNull() && ok()) {
    writeImplCb([&] { return dest_->writeEvbuffer(data); });
  }
  nwritten_ += nbytes;
}

void ProtocolWriter::writeWithoutCopy(const void* data, size_t nbytes) {
  if (!isProtoVersionAllowed()) {
    return;
  }
  if (!dest_->isNull() && ok()) {
    writeImplCb(
        [&] { return dest_->writeWithoutCopy(data, nbytes, nwritten_); });
  }
  nwritten_ += nbytes;
}

}} // namespace facebook::logdevice
