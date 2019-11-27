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
 * PayloadHolder is a convenience wrapper around a folly::IOBuf that owns
 * a payload. Multiple PayloadHolder-s can point to the same underlying memory
 * slice because IOBuf uses reference counting. Chained or unmanaged IOBuf-s
 * are not supported (and not used) at the moment.
 * This is not a thread-safe data structure hence care should be taken when
 * using non-const API's.
 */

class ProtocolWriter;

class PayloadHolder {
 public:
  enum take_ownership_t { TAKE_OWNERSHIP };
  enum copy_buffer_t { COPY_BUFFER };

  /**
   * Creates a PayloadHolder holding an empty payload.
   */
  PayloadHolder() = default;

  /**
   *  Assumes ownership of the given folly::IOBuf.
   */
  explicit PayloadHolder(folly::IOBuf&& iobuf, bool ignore_size_limit = false);

  /**
   * Assumes ownership of the given buffer.  It must have been malloc'd and
   * will be free'd by the destructor.
   */
  PayloadHolder(take_ownership_t,
                void* buf,
                size_t size,
                bool ignore_size_limit = false);

  /**
   * Makes a copy of the payload.
   */
  PayloadHolder(copy_buffer_t, const Payload& payload);
  PayloadHolder(copy_buffer_t, const void* buf, size_t size);

  // Movable and copyable.
  // Copying uses folly::IOBuf::cloneAsValue(), which copies the pointers and
  // increments refcount, but doesn't copy the data.

  PayloadHolder(PayloadHolder&& other) noexcept = default;
  PayloadHolder& operator=(PayloadHolder&& other) noexcept = default;

  PayloadHolder(const PayloadHolder& other) = default;
  PayloadHolder& operator=(const PayloadHolder& other) = default;

  // Some convenience wrappers.
  static PayloadHolder copyString(const char* s) {
    return PayloadHolder(COPY_BUFFER, s, strlen(s));
  }
  static PayloadHolder copyString(const std::string& s) {
    return PayloadHolder(COPY_BUFFER, s.data(), s.size());
  }
  static PayloadHolder copyPayload(const Payload& p) {
    return PayloadHolder(COPY_BUFFER, p);
  }
  static PayloadHolder copyBuffer(const void* data, size_t size) {
    return PayloadHolder(COPY_BUFFER, data, size);
  }
  static PayloadHolder takeOwnership(void* buf,
                                     size_t size,
                                     bool ignore_size_limit = false) {
    return PayloadHolder(TAKE_OWNERSHIP, buf, size, ignore_size_limit);
  }

  // Same as copy constructor.
  PayloadHolder clone() const {
    return PayloadHolder(*this);
  }

  /**
   * @return true iff the held payload has length zero.
   */
  bool empty() const {
    return iobuf_.empty();
  }

  ~PayloadHolder() = default;

  folly::IOBuf& iobuf() {
    return iobuf_;
  }

  const folly::IOBuf& iobuf() const {
    return iobuf_;
  }

  /**
   * Destroy/unreference the held payload.
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
   * Returns the owned Payload copied to std::string
   */
  std::string toString() const;

 private:
  // IOBuf contains the payload read from socket or getting sent over to socket.
  folly::IOBuf iobuf_;
};

}} // namespace facebook::logdevice
