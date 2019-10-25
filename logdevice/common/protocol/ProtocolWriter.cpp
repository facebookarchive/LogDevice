/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/ProtocolWriter.h"

#include <folly/io/IOBuf.h>

#include "event2/buffer.h"
#include "event2/event.h"
#include "logdevice/common/Checksum.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/libevent/compat.h"
#include "logdevice/common/protocol/MessageTypeNames.h"

namespace facebook { namespace logdevice {

namespace {
// Write into folly::IOBuf to create a chain. First buffer is allocated outside
// and passed in, to create this object. On write, bytes are copied into the
// current active iobuf. If the buffer becomes full, another buffer is prepended
// as IOBuf chains are circular hence the last added buffer or tail of chain is
// just before the link pointed by iobuf_ . Hence the current active buffer is
// always prev of iobuf_. In presence of just one element iobuf_->prev points
// to itself which is correctly the current active iobuf. writeWithoutCopy API
// just wraps the given buffer with IOBuf without taking ownership. Entity
// passing in the buffer needs to make sure that the buffer exists till the
// IOBuf gets deleted.
class IOBufDestination : public ProtocolWriter::Destination {
 public:
  int write(const void* src, size_t nbytes, size_t /*nwritten*/) override {
    const uint8_t* ptr = static_cast<const uint8_t*>(src);
    auto add_to_iobuf = [this](const void* ptr, size_t nbytes) {
      // iobuf_ points to the head of the IOBuf chain. IOBuf chain is circular
      // hence the iobuf_->prev is the tail of the chain.
      auto prev_buf = iobuf_->prev();
      ld_check(prev_buf->tailroom() >= nbytes);
      memcpy(static_cast<void*>(prev_buf->writableTail()), ptr, nbytes);
      prev_buf->append(nbytes);
    };

    ld_check(nbytes > 0);
    auto avail = iobuf_->prev()->tailroom();
    auto nadd = std::min(nbytes, avail);
    if (avail == 0) {
      auto new_iobuf =
          folly::IOBuf::create(std::max(nbytes, IOBUF_ALLOCATION_UNIT));
      // IOBuf chain is circular. Add the newly created IOBuf to the tail of
      // IOBuf chain by prepending it to the first IOBuf.
      iobuf_->prependChain(std::move(new_iobuf));
      nadd = nbytes;
    }

    add_to_iobuf(ptr, nadd);
    ptr += nadd;
    nbytes -= nadd;

    if (nbytes > 0) {
      auto new_iobuf =
          folly::IOBuf::create(std::max(nbytes, IOBUF_ALLOCATION_UNIT));
      // IOBuf chain is circular. Add the newly created IOBuf to the tail of
      // IOBuf chain by prepending it to the first IOBuf.
      iobuf_->prependChain(std::move(new_iobuf));
    }

    add_to_iobuf(ptr, nbytes);

    return 0;
  }

  int writeWithoutCopy(const void* src,
                       size_t nbytes,
                       size_t /*nwritten*/) override {
    ld_check(nbytes > 0);
    auto new_iobuf =
        folly::IOBuf::wrapBuffer(static_cast<const void*>(src), nbytes);
    iobuf_->prependChain(std::move(new_iobuf));
    return 0;
  }

  int writeWithoutCopy(folly::IOBuf* const buffer,
                       size_t /* nwritten */) override {
    ld_check(buffer->length());
    auto clone = buffer->clone();
    iobuf_->prependChain(std::move(clone));
    return 0;
  }

  uint64_t computeChecksum() override {
    uint64_t checksum = 0;
    const size_t len = iobuf_->computeChainDataLength();
    std::string data(len, 0);
    auto it = data.begin();
    for (auto& buf : *iobuf_) {
      std::copy(buf.begin(), buf.end(), it);
      it += buf.size();
    }
    Slice slice(&data[0], len);
    checksum_bytes(slice, 64, (char*)&checksum);
    return checksum;
  }
  const char* identify() const override {
    return "iobuf destination";
  }

  bool isNull() const override {
    return iobuf_ == nullptr;
  }

  explicit IOBufDestination(folly::IOBuf* dest) : iobuf_(dest) {}

  ~IOBufDestination() override {}

 private:
  folly::IOBuf* const iobuf_;
};

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

  int writeWithoutCopy(folly::IOBuf* const buffer, size_t nwritten) override {
    return writeWithoutCopy(buffer->data(), buffer->length(), nwritten);
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

  int writeWithoutCopy(folly::IOBuf* const buffer, size_t nwritten) override {
    return writeWithoutCopy(buffer->data(), buffer->length(), nwritten);
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

ProtocolWriter::ProtocolWriter(MessageType type,
                               folly::IOBuf* iobuf,
                               folly::Optional<uint16_t> proto)
    : dest_(new (dest_space_) IOBufDestination(iobuf)),
      context_(messageTypeNames()[type].c_str()),
      proto_(proto) {
  static_assert(sizeof(dest_space_) >= sizeof(IOBufDestination), "");
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

void ProtocolWriter::writeWithoutCopy(folly::IOBuf* const buffer) {
  if (!isProtoVersionAllowed()) {
    return;
  }
  if (!dest_->isNull() && ok()) {
    writeImplCb([&] { return dest_->writeWithoutCopy(buffer, nwritten_); });
  }
  nwritten_ += buffer->length();
}

}} // namespace facebook::logdevice
