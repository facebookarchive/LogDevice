/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/ProtocolReader.h"

#include "event2/buffer.h"
#include "logdevice/common/Checksum.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/libevent/compat.h"
#include "logdevice/common/protocol/MessageTypeNames.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

namespace {
class EvbufferSource : public ProtocolReader::Source {
 public:
  int read(void* dest, size_t to_read, size_t nread) override {
    // must be checked by caller
    ld_check(nread + to_read <= len_);
    int rv = LD_EV(evbuffer_remove)(src_, dest, to_read);
    if (rv < 0 || rv != to_read) {
      err = E::INTERNAL;
      ld_check(false);
    }
    return rv;
  }

  int readEvbuffer(evbuffer* dest, size_t to_read, size_t nread) override {
    // must be checked by caller
    ld_check(nread + to_read <= len_);
    int rv = LD_EV(evbuffer_remove_buffer)(src_, dest, to_read);
    if (rv < 0 || rv != to_read) {
      err = E::INTERNAL;
      ld_check(false);
    }
    return rv;
  }

  int drain(size_t to_drain, size_t nread) override {
    // must be checked by caller
    ld_check(nread + to_drain <= len_);
    int rv = LD_EV(evbuffer_drain)(src_, to_drain);
    if (rv != 0) {
      err = E::INTERNAL;
      ld_check(false);
    }
    return rv;
  }

  size_t getLength() const override {
    return len_;
  }

  // This assumes caller is sure about presence of checksum
  // field in header
  uint64_t computeChecksum(size_t msg_len) override {
    uint64_t checksum = 0;
    size_t len = LD_EV(evbuffer_get_length)(src_);

    if (len < msg_len) {
      RATELIMIT_WARNING(
          std::chrono::seconds(1),
          2,
          "evbuffer doesn't have enough bytes, len:%zu, msg_len:%zu",
          len,
          msg_len);
    }
    ld_check(msg_len <= len);

    len = std::min(len, msg_len);
    std::string data(len, 0);
    ev_ssize_t nbytes = LD_EV(evbuffer_copyout)(src_, &data[0], len);
    ld_check(nbytes == len);

    Slice slice(&data[0], len);
    checksum_bytes(slice, 64, (char*)&checksum);
    return checksum;
  }

  const char* identify() const override {
    return "evbuffer source";
  }

  std::string hexDump(size_t /*unused*/) const override {
    // currently not supported for evbuffer
    return "N/A";
  }

  explicit EvbufferSource(evbuffer* src, size_t len) : src_(src), len_(len) {
    // source must be valid
    ld_check(src != nullptr);
  }

 private:
  struct evbuffer* const src_;
  const size_t len_;
};

class LinearBufferSource : public ProtocolReader::Source {
 public:
  int read(void* dest, size_t to_read, size_t nread) override {
    // must be checked by caller
    ld_check(nread + to_read <= getLength());
    memcpy(dest, (char*)src_.data + nread, to_read);
    return to_read;
  }

  int readEvbuffer(evbuffer* /*dest*/,
                   size_t /*to_read*/,
                   size_t /*nread*/) override {
    err = E::NOTSUPPORTED;
    return -1;
  }

  int drain(size_t to_drain, size_t nread) override {
    // must be checked by caller
    ld_check(nread + to_drain <= getLength());
    // no explicit drain needed
    return 0;
  }

  size_t getLength() const override {
    return src_.size;
  }

  const char* identify() const override {
    return "linear buffer source";
  }

  /* unused */
  uint64_t computeChecksum(size_t /*unused*/) override {
    return 0;
  }

  std::string hexDump(size_t max_output_size) const override {
    return hexdump_buf(src_.data, src_.size, max_output_size);
  }

  explicit LinearBufferSource(Slice src) : src_(src) {
    // source must be valid
    ld_check(src.data != nullptr);
  }

 private:
  const Slice src_;
};
} // anonymous namespace

ProtocolReader::ProtocolReader(Source* src,
                               const char* context,
                               folly::Optional<uint16_t> proto)
    : src_(src),
      context_(context),
      proto_(std::move(proto)),
      src_left_(src_->getLength()) {
  ld_check(src_ != nullptr);
}

ProtocolReader::ProtocolReader(MessageType type,
                               struct evbuffer* src,
                               size_t to_read,
                               folly::Optional<uint16_t> proto)
    : src_owned_(true),
      src_(new (src_space_) EvbufferSource(src, to_read)),
      context_(messageTypeNames()[type].c_str()),
      proto_(proto),
      src_left_(src_->getLength()) {
  static_assert(sizeof(src_space_) >= sizeof(EvbufferSource));
}

ProtocolReader::ProtocolReader(Slice src,
                               std::string context,
                               folly::Optional<uint16_t> proto)
    : src_owned_(true),
      context_owned_(std::move(context)),
      src_(new (src_space_) LinearBufferSource(src)),
      context_(context_owned_.c_str()),
      proto_(proto),
      src_left_(src_->getLength()) {
  static_assert(sizeof(src_space_) >= sizeof(LinearBufferSource));
}

ProtocolReader::~ProtocolReader() {
  if (src_owned_) {
    src_->~Source();
  }
}

template <typename Fn>
void ProtocolReader::readImplCb(size_t to_read, Fn&& fn) {
  checkReadableBytes(to_read);
  if (error()) {
    return;
  }

  int nread = fn();
  if (nread < 0 || nread != to_read) {
    RATELIMIT_CRITICAL(std::chrono::seconds(1),
                       10,
                       "INTERNAL ERROR: input %s returned %d bytes "
                       "for a %s message. Expected %zu bytes.",
                       src_->identify(),
                       nread,
                       context_,
                       to_read);
    ld_check(err != E::OK);
    status_ = err;
    return;
  }

  nread_ += nread;
  src_left_ -= nread;
}

void ProtocolReader::readImpl(void* out, size_t to_read) {
  readImplCb(to_read, [&] { return src_->read(out, to_read, nread_); });
}

void ProtocolReader::readEvbuffer(evbuffer* out, size_t to_read) {
  if (ok() && isProtoVersionAllowed()) {
    readImplCb(
        to_read, [&] { return src_->readEvbuffer(out, to_read, nread_); });
  }
}

void ProtocolReader::checkReadableBytes(size_t bytes_to_read) {
  if (bytes_to_read > src_left_) {
    ld_error(
        "PROTOCOL ERROR: Ran out of bytes while reading %s message from %s. "
        "After reading %zu bytes, tried to read %zu more but only %zu "
        "left.",
        context_,
        src_->identify(),
        nread_,
        bytes_to_read,
        src_left_);
    status_ = E::BADMSG;
  }
}

void ProtocolReader::handleTrailingBytes(size_t bytes_trailing) {
  checkReadableBytes(bytes_trailing);
  if (error() || bytes_trailing == 0) {
    return;
  }

  // Drain the extraneous bytes so that the next object in byte stream can be
  // parsed.
  int rv = src_->drain(bytes_trailing, nread_);
  if (rv != 0) {
    ld_check(err != E::OK);
    status_ = err;
    return;
  }

  nread_ += bytes_trailing;
  src_left_ -= bytes_trailing;

  RATELIMIT_LEVEL(
      allow_trailing_bytes_ ? dbg::Level::WARNING : dbg::Level::ERROR,
      std::chrono::seconds(5),
      5,
      "Got a %s message from %s with extra data at the end (%zu bytes left "
      "after reading %zu).  %s.",
      context_,
      src_->identify(),
      bytes_trailing,
      nread_ - bytes_trailing,
      allow_trailing_bytes_ ? "Ignoring the trailing bytes"
                            : "Reporting TOOBIG");

  if (!allow_trailing_bytes_) {
    status_ = E::TOOBIG;
  }
}

}} // namespace facebook::logdevice
