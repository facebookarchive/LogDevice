/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/WorkerType.h"
#include "logdevice/common/ZeroCopyPayload.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/include/Record.h"

struct evbuffer;

namespace facebook { namespace logdevice {

/**
 * PayloadHolder encapsulates the two different types of buffers we use
 * for record payload: flat buffers and evbuffers. It serves as a kind of
 * smart pointer for record payloads stored in a buffer of one of those
 * types.
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
      : payload_flat_(buf, size), payload_evbuffer_(nullptr) {
    if (!ignore_size_limit) {
      ld_check(payload_flat_.size() == 0 || payload_flat_.data() != nullptr);
      ld_check(payload_flat_.size() < Message::MAX_LEN);
    }
  }

  /**
   * The object constructed will own the evbuffer, and will free the
   * evbuffer upon destruction, provided that it does not pass on
   * the ownership through a move constructor.
   *
   * Must run on a worker thread and, additionally, most subsequent operations
   * on the instance (including destruction) should run on the same worker
   * thread.
   *
   * @param payload    a non-empty evbuffer containing a record payload
   *                   possibly zero-copied from a Socket's input evbuffer.
   */
  explicit PayloadHolder(struct evbuffer* payload);

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
  explicit PayloadHolder()
      : payload_flat_(Payload(nullptr, 1)), payload_evbuffer_(nullptr) {}

  /**
   * @return true iff PayloadHolder references a payload
   */
  bool valid() const {
    return payload_flat_.data() || payload_flat_.size() == 0 ||
        payload_evbuffer_;
  }

  // If _other_ owned the buffer, the ownership is transferred to *this. If
  // _other_ was a weak reference, *this becomes another such weak reference.
  PayloadHolder(PayloadHolder&& other) noexcept : PayloadHolder() {
    *this = std::move(other);
  }

  PayloadHolder& operator=(PayloadHolder&& other) noexcept {
    if (this != &other) {
      std::swap(payload_flat_, other.payload_flat_);
      std::swap(payload_evbuffer_, other.payload_evbuffer_);
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
   * If we were the owner of payload_evbuffer_, free it and relinquish
   * ownership.
   */
  void reset();

  /**
   * Size of contained payload.
   */
  size_t size() const;

  /**
   * Serializes the payload into evbuffer.
   *
   * @param writer     ProtocolWriter that encapsulates the evbuffer to
   *                   write into
   *
   * @return  nothing is returned. But if there is an error on serialization,
   *          @param writer should enter error state (i.e., writer.error()
   *          == true).
   */
  void serialize(ProtocolWriter& writer) const;

  /**
   * Deserialze by constructing a PayloadHolder object from evbuffer. Note that
   * evbuffer might get changed after the call.
   *
   * @param reader              ProtocolReader object that encapsulates the
   *                            evbuffer to read from
   * @param payload_size        number of bytes to read
   *
   * @param zero_copy           if false, allocate a flat buffer and copy the
   *                            content from evbuffer; otherwise, perform
   *                            zero-copy read by creating an evbuffer and
   *                            read into it using evbuffer_remove_buffer().
   *                            (See also ProtocolReader::readEvbuffer).
   *
   * @return  constructed PayloadHolder object, the object is invalid if
   *          deserializtion failed (i.e., reader enters error state).
   */
  static PayloadHolder deserialize(ProtocolReader& reader,
                                   size_t payload_size,
                                   bool zero_copy);
  /**
   * Corrupts a copy of the payload, runs reset(), and sets the corrupted copy
   * as the new payload. Used for testing to simulate bad hardware that flips
   * bits.
   */
  void TEST_corruptPayload();

  /**
   * Returns the owned Payload.  If backed by an evbuffer, linearizes the
   * evbuffer so that the data is contiguous in memory.
   *
   * Note that, since this may modify state (when backed by an evbuffer), care
   * needs to be taken when ownership of the PayloadHolder is shared across
   * threads.  Ideally, this should be called on the same thread that created
   * the PayloadHolder, soon after creation.
   */
  Payload getPayload();

  /**
   * Returns the flat payload. Must be called when payload is valid and
   * backed by flat buffer (not backed by evbuffer).
   */
  Payload getFlatPayload() const;

  /**
   * returns true if payload is backed by evbuffer
   */
  bool isEvbuffer() const {
    return payload_evbuffer_ != nullptr;
  }

  /**
   * Returns the owned Payload copied to std::string
   */
  std::string toString();

 private:
  // A client-supplied payload, or a small payload that was read from
  // an evbuffer into a flat buffer.  Whenever this field is in use,
  // payload_evbuffer_ is set to nullptr.
  //
  // Depending on the value of `owned_', upon destruction the object may
  // delete the payload referenced by payload_flat_.
  Payload payload_flat_;

  // if this payload was read from a Socket, or copy-constructed from a
  //  message read from a Socket, and the message is big enough to make
  // zero-copy worthwhile, this ZeroCopyPayload ptr contains the record's
  // payload, zero-copied from the input evbuffer of the Socket. Otherwise this
  // is nullptr.
  std::shared_ptr<ZeroCopyPayload> payload_evbuffer_;

  // If true (default), the PayloadHolder owns the payload and will free it in
  // the destructor.
  bool owned_ = true;
};

}} // namespace facebook::logdevice
