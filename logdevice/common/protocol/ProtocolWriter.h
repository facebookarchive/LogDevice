/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>
#include <type_traits>
#include <vector>

#include "logdevice/common/SerializableData.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/MessageType.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"

struct evbuffer;

namespace facebook { namespace logdevice {

/**
 * @file Utility class for serializing object into destination buffer (e.g.,
 * serializing Message subclasses onto the network evbuffer). Simplifies of a
 * lot of boilerplate like handling protocol errors, protocol versions etc.
 * The idea is to allow the callsite (message serializer) to repeatedly call
 * write() without checking for errors every time.
 *
 * If ProtocolWriter encounters an internal error, it moves into an error
 * state which makes subsequent write() calls no-ops.  The error is reported
 * to the socket later, when result*() is called.
 */

class ProtocolWriter {
 public:
  /**
   * Abstract class for the destination buffer.
   */
  class Destination {
   public:
    /**
     * Write the data from a linear buffer starting at @param src of @param
     * nbytes with @param nwritten bytes already written to destination.
     *
     * @return 0 on success -1 on failure. Note that err must be set on failure.
     */
    virtual int write(const void* src, size_t nbytes, size_t nwritten) = 0;

    /**
     * Same as write() but use best effort to avoid copy the data.
     */
    virtual int writeWithoutCopy(const void* src,
                                 size_t nbytes,
                                 size_t nwritten) = 0;

    /**
     * Write from an evbuffer in @param src into destination.
     * @return   0 on success, -1 on failure and err will be set. Specifically,
     *           for destinations that not support evbuffer source, err is set
     *           to E::NOTSUPPORTED
     */
    virtual int writeEvbuffer(evbuffer* src) = 0;

    /**
     * @return  a string to identify the buffer destination
     */
    virtual const char* identify() const = 0;

    /**
     * @return    true if the destination is null buffer, in such case
     *            ProtocolWriter will perform a dry run
     */
    virtual bool isNull() const = 0;

    /**
     * @return    compute checksum of payload held in the Destination buffer.
     */
    virtual uint64_t computeChecksum() = 0;

    virtual ~Destination() {}
  };

  /**
   * Writes sizeof(T) bytes.
   *
   * No-op if the writer has previously encountered an error.
   */
  template <typename T>
  void write(const T& val) {
    // Ideally this would check `std::is_pod' however a lot of message headers
    // contain NodeID which has a non-trivial default constructor.  For now,
    // rule out a few types that are easy to pass in by mistake.
    static_assert(!is_std_vector<T>::value,
                  "Templated ProtocolWriter::write() must not be called on "
                  "vector; use writeVector() instead");
    static_assert(!std::is_same<T, std::string>::value,
                  "Templated ProtocolWriter::write() must not be called on "
                  "string; use writeVector() instead");
    static_assert(!std::is_pointer<T>::value,
                  "Templated ProtocolWriter::write() must not be called on "
                  "pointer");
    write(&val, sizeof(T));
  }

  /**
   * Sets protocol version (or checks if set proto version is correct) and
   * proceeds with write() if everything checks out.
   */
  template <typename T>
  void writeVersion(const T& val) {
    static_assert(
        std::is_integral<T>::value,
        "Field that is meant to be a version must be of an integral type.");

    if (proto_.hasValue() && proto_.value() != val) {
      setError(E::PROTO);
    } else if (val < 0 || val > std::numeric_limits<uint16_t>::max()) {
      setError(E::PROTO);
    } else {
      proto_ = val;
      write(val);
    }
  }

  /**
   * Writes the specified range into the socket.
   *
   * No-op if the writer has previously encountered an error.
   */
  void write(const void* data, size_t nbytes) {
    if (!isProtoVersionAllowed()) {
      return;
    }

    if (!dest_->isNull() && ok() && nbytes > 0) {
      writeImpl(data, nbytes);
    }
    nwritten_ += nbytes;
  }

  /**
   * Writes data from an evbuffer (for zero-copy writes of larger amounts of
   * data).
   *
   * Wraps evbuffer_add_buffer_reference() from libevent.
   */
  void writeEvbuffer(evbuffer* data);

  /**
   * Writes data without necessarily copying it.  The Message subclass should
   * ensure the region of memory is valid as long as it exists.
   *
   * Wraps evbuffer_add_reference() from libevent.
   */
  void writeWithoutCopy(const void* data, size_t size);

  /**
   * Writes a vector.  std::string also welcome as it is sufficiently
   * vector-like.
   *
   * NOTE: The length of the vector is not included, assumed to be known to
   * the reader (from a header field for example).  See also
   * writeLengthPrefixedVector().
   */
  template <typename Vector>
  void writeVector(const Vector& v) {
    write(v.data(), v.size() * sizeof(typename Vector::value_type));
  }

  /**
   * Writes a vector of SerializableData.
   *
   * NOTE: The length of the vector is not included, assumed to be known to
   * the reader (from a header field for example).
   */
  template <typename Vector>
  void writeVectorOfSerializable(const Vector& v) {
    if (!isProtoVersionAllowed()) {
      return;
    }
    for (const auto& e : v) {
      e.serialize(*this);
    }
  }

  /**
   * Writes a vector prefixed by its size.
   */
  template <typename Vector>
  void writeLengthPrefixedVector(const Vector& v) {
    int64_t nbytes = v.size() * sizeof(typename Vector::value_type);
    write(nbytes);
    writeVector(v);
  }

  /**
   * Gate all subsequent write*() calls to protocol `proto' or newer.  If the
   * socket protocol is older, subsequent write*() calls will be no-ops.
   */
  void protoGate(uint16_t proto) {
    // It would be very strange to allow a lower protocol after previously
    // gating on a higher one
    ld_check(proto > proto_gate_);
    proto_gate_ = proto;
  }

  /**
   * Manually move the ProtocolWriter into an error state.  This should be
   * rare.
   */
  void setError(Status e) {
    ld_check(e != E::OK);
    if (status_ == E::OK) { // avoid swallowing a previous error
      status_ = e;
    }
  }

  /**
   * Called by the messaging layer after msg.serialize() to learn if
   * serialization succeded and how long the message was in bytes.
   */
  ssize_t result() {
    return status_ == E::OK ? nwritten_ : -1;
  }

  /**
   * Returns `true` if this ProtocolWriter doesn't care about the data being
   * written. This is used for optimizing message size calculations
   */
  bool isBlackHole() {
    return dest_->isNull();
  }

  uint16_t proto() const {
    ld_assert(proto_.hasValue());
    return proto_.value();
  }

  bool isProtoVersionAllowed() {
    if (!proto_.hasValue()) {
      setError(E::PROTO); // protocol version must be set
      return false;
    } else if (proto_.value() < proto_gate_) {
      return false;
    }
    return true;
  }

  Status status() const {
    return status_;
  }
  bool ok() const {
    return status_ == E::OK;
  }
  bool error() const {
    return status_ != E::OK;
  }
  uint64_t computeChecksum() {
    return dest_->computeChecksum();
  }

  // Use a custom destination. Both dest and context are owned by the caller and
  // must outlive the ProtocolWriter.
  ProtocolWriter(Destination* dest,
                 const char* context,
                 folly::Optional<uint16_t> proto = folly::none);

  ProtocolWriter(MessageType type,
                 struct evbuffer* dest, // may be null
                 folly::Optional<uint16_t> proto = folly::none);

  ProtocolWriter(Slice dest,
                 std::string context,
                 folly::Optional<uint16_t> proto = folly::none);

  // Use a std::string (@param dest) as destination. ProtocolWriter::write()
  // will resize the string with potential allocations. Users can also
  // optionally specify the max_size parameter to limit the maximum size the
  // string can grow. Note: caller must ensure that the dest string outlives the
  // ProtocolWriter
  ProtocolWriter(std::string* dest,
                 std::string context,
                 folly::Optional<uint16_t> proto = folly::none,
                 folly::Optional<size_t> max_size = folly::none);

  ProtocolWriter(const ProtocolWriter&) = delete;
  ProtocolWriter& operator=(const ProtocolWriter&) = delete;

  ~ProtocolWriter();

 private:
  // To avoid allocating Destination on the heap, we'll placement-new it in this
  // buffer. We could instead require the user of ProtocolWriter to always own
  // the Destination, but that would be slightly less convenient.
  alignas(void*) char dest_space_[24];
  bool dest_owned_ = false;

  std::string context_owned_;

  // Points either to dest_space or to an unowned Destination.
  Destination* dest_;
  // Points to either context_owned_ or to an unowned string.
  const char* context_;

  size_t nwritten_ = 0;
  // Socket protocol
  folly::Optional<uint16_t> proto_;
  // Protocol gate; write calls are ignored if `proto_' < `proto_gate_'
  uint16_t proto_gate_ = 0;
  Status status_ = E::OK;

  void writeImpl(const void* data, size_t nbytes);
  template <typename Fn>
  void writeImplCb(Fn&& fn);

  // http://stackoverflow.com/questions/21512678/check-at-compile-time-is-a-template-type-a-vector
  template <typename T>
  struct is_std_vector : public std::false_type {};
  template <typename T, typename A>
  struct is_std_vector<std::vector<T, A>> : public std::true_type {};

  // Let Message.cpp access `dest_' and `proto_' to ease the transition to new
  // serializer interface
  friend struct Message;
};

}} // namespace facebook::logdevice
