/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>
#include <type_traits>
#include <vector>

#include "logdevice/common/debug.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/common/protocol/MessageType.h"
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

    /**
     * Moves the temp evbuffer contents into Destination evbuffer.
     * Frees the temp evbuffer.
     */
    virtual void endSerialization() {}

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
   * Writes the specified range into the socket.
   *
   * No-op if the writer has previously encountered an error.
   */
  void write(const void* data, size_t nbytes) {
    if (proto_ < proto_gate_) {
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
    return proto_;
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
  void endSerialization() {
    dest_->endSerialization();
  }

  ProtocolWriter(std::unique_ptr<Destination> dest,
                 std::string context,
                 uint16_t proto);

  ProtocolWriter(MessageType type,
                 struct evbuffer* dest, // may be null
                 uint16_t proto);

  ProtocolWriter(Slice dest, std::string context, uint16_t proto);

 private:
  std::unique_ptr<Destination> dest_;
  const std::string context_;

  size_t nwritten_ = 0;
  // Socket protocol
  uint16_t proto_;
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
