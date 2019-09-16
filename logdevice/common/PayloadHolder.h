/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once
#include <folly/io/IOBuf.h>

#include "logdevice/common/WorkerType.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/include/Record.h"

namespace facebook { namespace logdevice {

/**
 * PayloadHolder encapsulates the different types of buffers we use
 * for record payload: flat buffers and folly::IOBufs. It serves as a kind of
 * smart pointer for record payloads stored in a buffer of one of those
 * types. This is not a thread-safe data structure hence care should be taken
 * when using non-const API's.
 */

class ProtocolWriter;
class EventLoop;

class PayloadHolder {
 public:
  /**
   * Assumes ownership of the given buffer.  It must have been malloc'd and
   * will be free'd by the destructor.
   */
  PayloadHolder(const void* buf, size_t size, bool ignore_size_limit = false)
      : payload_flat_(buf, size) {
    if (!ignore_size_limit) {
      ld_check(payload_flat_.size() == 0 || payload_flat_.data() != nullptr);
      ld_check(payload_flat_.size() < Message::MAX_LEN);
    }
  }

  /**
   *  Assumes ownership of the given folly::IOBuf.
   */
  explicit PayloadHolder(std::unique_ptr<folly::IOBuf> iobuf);

  enum unowned_t { UNOWNED };
  /**
   * Wraps the given `Payload' without assuming ownership of the buffer.  You
   * must pass `UNOWNED' to make this explicit at the callsite.
   */
  explicit PayloadHolder(const Payload& payload, unowned_t)
      : PayloadHolder(payload.data(), payload.size()) {
    owned_ = false;
  }

  /**
   * Creates an invalid PayloadHolder.
   */
  explicit PayloadHolder() : payload_flat_(Payload(nullptr, 1)) {}

  /**
   * @return true iff PayloadHolder references a payload
   */
  bool valid() const {
    return payload_flat_.data() || payload_flat_.size() == 0 ||
        iobuf_ != nullptr;
  }

  // If _other_ owned the buffer, the ownership is transferred to *this. If
  // _other_ was a weak reference, *this becomes another such weak reference.
  PayloadHolder(PayloadHolder&& other) noexcept : PayloadHolder() {
    *this = std::move(other);
  }

  PayloadHolder& operator=(PayloadHolder&& other) noexcept {
    if (this != &other) {
      std::swap(payload_flat_, other.payload_flat_);
      std::swap(iobuf_, other.iobuf_);
      std::swap(owned_, other.owned_);
      other.reset();
    }
    return *this;
  }

  PayloadHolder(const PayloadHolder& other) = delete;
  PayloadHolder& operator=(const PayloadHolder& other) = delete;

  ~PayloadHolder() {
    reset();
  }

  bool owner() const {
    return valid() && owned_;
  }

  /**
   * If we were the owner of iobuf, free it and relinquish
   * ownership.
   */
  void reset();

  /**
   * Size of contained payload.
   */
  size_t size() const;

  /**
   * Serializes the data/payload owned by holder into the writer.
   *
   * @param writer     ProtocolWriter that encapsulates the buffer into which
   *                   serialized data will be written.
   *
   * @return  nothing is returned. But if there is an error on serialization,
   *          @param writer should enter error state (i.e., writer.error()
   *          == true).
   */
  void serialize(ProtocolWriter& writer) const;

  /**
   * Construct a PayloadHolder by deserialize data held in the buffer owned by
   * ProtocolReader. After this call bytes held in ProtocolReader buffer are
   * moved into the newly created instance of PayloadHolder.
   *
   * @param reader              ProtocolReader object that encapsulates the
   *                            buffer to read serialized data from
   * @param payload_size        number of bytes to read
   *
   * @return  constructed PayloadHolder object, the object is invalid if
   *          deserializtion failed (i.e., reader enters error state).
   */
  static PayloadHolder deserialize(ProtocolReader& reader, size_t payload_size);
  /**
   * Corrupts a copy of the payload, runs reset(), and sets the corrupted copy
   * as the new payload. Used for testing to simulate bad hardware that flips
   * bits.
   */
  void TEST_corruptPayload();

  /**
   * Returns the owned Payload held by instance of PayloadHolder
   */
  Payload getPayload() const;

  /**
   * Returns the flat payload.
   */
  Payload getFlatPayload() const;

  /**
   * returns true if payload is backed by IOBuf
   */
  bool isIOBuffer() const {
    return iobuf_ != nullptr;
  }

  /**
   * Returns the owned Payload copied to std::string
   */
  std::string toString() const;

 private:
  // A client-supplied payload, or a small payload that was read from
  // ProtocolReader buffer.
  //
  // Depending on the value of `owned_', upon destruction the object may
  // delete the payload referenced by payload_flat_.
  Payload payload_flat_;

  // IOBuf contains the payload read from socket or getting sent over to socket.
  std::unique_ptr<folly::IOBuf> iobuf_;

  // If true (default), the PayloadHolder owns the payload and will free it in
  // the destructor.
  bool owned_ = true;
};

}} // namespace facebook::logdevice
