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
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/protocol/MessageReadResult.h"
#include "logdevice/common/protocol/MessageType.h"
#include "logdevice/include/Err.h"

struct evbuffer;

namespace facebook { namespace logdevice {

/**
 * @file Utility class for reading bytes of an object (e.g., message) bytes
 * off the a buffer source (e.g., network evbuffer). For messages it also
 * support parsing them to form Message subclasses.  Simplifies of a lot of
 * boilerplate like handling protocol errors, protocol versions etc.
 *
 * The basic idea is to allow the callsite (e.g., message deserializer) to
 * repeatedly call read() without checking for errors every time, and result()
 * when it is ready to construct a message.
 *
 * If ProtocolReader encounters an error (such as running out of bytes in the
 * input evbuffer), it moves into an error state which makes subsequent read()
 * calls no-ops.  The error is reported to the socket later, when result*() is
 * called.
 *
 * Note that the error-swallowing/deferring comes with caveats.  For example,
 * after read(&x), `x' may not contain a valid value (if an error was
 * encountered).  Conditionally calling another read() based on the value of
 * `x' is safe since, in the error case, subsequent read() calls become
 * no-ops.  Doing arbitrary conditional logic is not recommended in
 * deserializers, however if needed, it should be guarded with calls to ok()
 * which indicates that there have been no errors; all previous reads were
 * successful and their output is valid.
 */

class ProtocolReader {
 public:
  /**
   * Abstract class for the source buffer to read from.
   */
  class Source {
   public:
    /**
     * Read the data from this source to the linear destination buffer. Caller
     * must ensure that the destination buffer has at least @param to_read bytes
     * of capacity.
     *
     * @param dest       starting address of the destination buffer
     * @param to_read    amount of data to read
     * @param nread      bytes read so far by ProtocolReader
     *
     * @return           num of bytes read on success, -1 on failure,
     *                   and err must be set
     */
    virtual int read(void* dest, size_t to_read, size_t nread) = 0;

    /**
     * Similar to read() except the destination is an evbuffer so that copy
     * could be avoided in certain source types. For sources that do not support
     * such type of reading, return -1 with err set to E::NOTSUPPORTED.
     */
    virtual int readEvbuffer(evbuffer* dest, size_t to_read, size_t nread) = 0;

    /**
     * Remove @param to_drain number of bytes from the beginning of the source
     * buffer. Caller must ensure that source buffer still has enough remaining
     * bytes.
     */
    virtual int drain(size_t to_drain, size_t nread) = 0;

    /**
     * @return   the total length in bytes for the source buffer. Used by
     *           ProtocolReader to determine if there are enough bytes.
     */
    virtual size_t getLength() const = 0;

    /**
     * @return  a string to identify the source buffer
     */
    virtual const char* identify() const = 0;

    virtual std::string hexDump(
        size_t max_output_size = std::numeric_limits<size_t>::max()) const = 0;

    virtual uint64_t computeChecksum(size_t msg_len) = 0;

    virtual ~Source() {}
  };

  /**
   * Reads sizeof(T) bytes into `out'.
   *
   * No-op if the reader has previously encountered an error.
   */
  template <typename T>
  void read(T* out) {
    // Ideally this would check `std::is_pod' however a lot of message headers
    // contain NodeID which has a non-trivial default constructor.  For now,
    // rule out a few types that are easy to pass in by mistake.
    static_assert(!is_std_vector<T>::value,
                  "Templated ProtocolReader::read() must not be called on "
                  "vector; use readVector() instead");
    static_assert(!std::is_same<T, std::string>::value,
                  "Templated ProtocolReader::read() must not be called on "
                  "string; use readVector() instead");
    read(out, sizeof(T));
  }

  /**
   * Reads protocol version from field and sets version.
   */
  template <typename T>
  void readVersion(T* val) {
    static_assert(
        std::is_integral<T>::value,
        "Field that is meant to be a version must be of an integral type.");

    if (proto_.hasValue()) {
      setError(E::PROTO);
      return;
    }

    proto_ = 0;
    read(val);

    if (!error()) {
      if (*val < 0 || *val > std::numeric_limits<uint16_t>::max()) {
        setError(E::PROTO);
      } else {
        proto_ = *val;
      }
    }
  }

  /**
   * Reads the specified number of bytes into `out'.
   *
   * No-op if the reader has previously encountered an error.
   */
  void read(void* out, size_t to_read) {
    if (!isProtoVersionAllowed()) {
      // No zeroing out in this case, as caller may have set an alternate
      // (non-zero) default for old protocols
    } else if (error()) {
      // In the (unlikely) case of a previous protocol or internal error, zero
      // out the memory to make patterns like this safe:
      //   size_t n;         // uninitialized
      //   reader.read(&n);  // may fail
      //   vector<int> v(n); // how much are we allocating?
      memset(out, 0, to_read);
    } else {
      readImpl(out, to_read);
      if (error()) {
        memset(out, 0, to_read);
      }
    }
  }

  /**
   * Reads into an evbuffer (for zero-copy reads of larger amounts of data).
   * Wraps evbuffer_remove_buffer() from libevent.
   */
  void readEvbuffer(evbuffer* out, size_t to_read);

  uint64_t computeChecksum(size_t msglen) {
    return src_ ? src_->computeChecksum(msglen) : 0;
  }

  /**
   * Reads a vector (pre-sized, elements get overwritten).  std::string also
   * welcome as it is sufficiently vector-like.
   */
  template <typename Vector>
  void readVector(Vector* out) {
    using value_type = typename Vector::value_type;
    // const_cast for std::string in which a non-const char* data() only
    // exists since C++17
    read(
        const_cast<value_type*>(out->data()), out->size() * sizeof(value_type));
  }

  /**
   * Reads a vector, resizing it to `n' elements.
   */
  template <typename Vector>
  void readVector(Vector* out, size_t n) {
    // Pre-check `proto_gate_' to avoid resize() causing a side effect on an
    // old protocol
    if (isProtoVersionAllowed()) {
      checkReadableBytes(n * sizeof(typename Vector::value_type));
      if (ok() && n > 0) {
        out->resize(n);
        readVector(out);
      } else {
        // Same rationale as zeroing out in read(void*, size_t)
        out->clear();
      }
    }
  }

  /**
   * Reads a vector of SerializableData, resizing it to `n' elements.
   */
  template <typename Vector>
  void readVectorOfSerializable(Vector* out, size_t n) {
    if (isProtoVersionAllowed()) {
      out->resize(n);
      for (auto& o : *out) {
        o.deserialize(*this, false);
      }
    }
  }

  /**
   * Reads a length-prefixed vector.
   */
  template <typename Vector>
  void readLengthPrefixedVector(Vector* out) {
    int64_t nbytes = 0;
    read(&nbytes);
    if (nbytes % sizeof(typename Vector::value_type) != 0) {
      // The length of the vector isn't divisible by the type size, something is
      // very wrong
      ld_error("PROTOCOL ERROR: Stated size of vector (%lu bytes) not "
               "divisible by the size of the value type (%lu bytes) while "
               "reading %s message from %s. Bytes read: %lu",
               nbytes,
               sizeof(typename Vector::value_type),
               context_,
               src_->identify(),
               nread_);
      status_ = E::BADMSG;
      // NOTE: falling through to readVector() anyway to ensure the vector is
      // appropriately modified (or not, depending on the proto version)
    }
    readVector(out, nbytes / sizeof(typename Vector::value_type));
  }

  /**
   * Gate all subsequent read*() calls to protocol `proto' or newer.  If the
   * socket protocol is older, subsequent read*() calls will be no-ops, with
   * zero side effects.
   */
  void protoGate(uint16_t proto) {
    // It would be very strange to allow a lower protocol after previously
    // gating on a higher one
    ld_check(proto > proto_gate_);
    proto_gate_ = proto;
  }

  /**
   * Drain specific number of bytes from the buffer source.
   * @param bytes_remaining     amount of bytes to drain
   *
   * @return   nothing is returned. However, the ProtocolReader enters
   *           the error state if 1) draining failed, or 2) there is no enough
   *           bytes remaining to drain, or 3) trailing bytes is not allowed.
   */
  void handleTrailingBytes(size_t bytes_trailing);

  /**
   * Manually move the ProtocolReader into an error state.
   */
  void setError(Status e) {
    ld_check(e != E::OK);
    if (status_ == E::OK) { // avoid swallowing a previous error
      status_ = e;
    }
  }

  /**
   * Creates a MessageReadResult for the socket layer.
   *
   * If the reader has not encountered an error, invokes the given callback to
   * create a Message subclass.  The callback must return a Message*,
   * DerivedMessage*, std::unique_ptr<Message> or
   * std::unique_ptr<DerivedMessage>.
   *
   * If there was an error during message reading, the callback is not
   * invoked and the error is propagated to the socket layer through `err'.
   * Possible errors:
   *      BADMSG    message is too small (ran out of bytes on the wire)
   *      INTERNAL  `src' had fewer than `to_read' bytes available (see ctor)
   *      TOOBIG    message size is too large
   */
  template <typename NewMessageCb>
  MessageReadResult result(NewMessageCb&& cb) {
    handleTrailingBytes(src_left_);
    if (error()) {
      err = status_;
      return MessageReadResult(nullptr);
    }
    return MessageReadResult(std::unique_ptr<Message>(cb()));
  }

  /**
   * Convenience overload of `result()' for when the Message object is already
   * constructed.
   */
  MessageReadResult resultMsg(std::unique_ptr<Message> msg) {
    return result([&]() { return std::move(msg); });
  }

  /**
   * Constructs an error result when the reader is known to already be in an
   * error state.
   */
  MessageReadResult errorResult() {
    ld_check(status_ != E::OK);
    err = status_;
    return MessageReadResult(nullptr);
  }

  /**
   * Constructs a custom error result provided by the caller.
   */
  MessageReadResult errorResult(Status code) {
    if (status_ == E::OK) { // avoid swallowing a previous error
      status_ = code;
    }
    return errorResult();
  }

  bool isProtoSet() const {
    return proto_.hasValue();
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
  size_t bytesRemaining() const {
    return ok() ? src_left_ : 0;
  }
  size_t bytesRead() const {
    return nread_;
  }

  /**
   * By default, `result()' fails with E::TOOBIG if there are bytes left
   * unconsumed in the evbuffer.  If this is called, the extra bytes will be
   * warned about and discarded, but message construction will be allowed to
   * succeed.
   */
  void allowTrailingBytes() {
    allow_trailing_bytes_ = true;
  }

  void disallowTrailingBytes() {
    allow_trailing_bytes_ = false;
  }

  const Source& getSource() const {
    return *src_;
  }

  // Use a custom source. Both src and context are owned by the caller and
  // must outlive the ProtocolReader.
  ProtocolReader(Source* src,
                 const char* context,
                 folly::Optional<uint16_t> proto = folly::none);

  ProtocolReader(MessageType type,
                 struct evbuffer* src,
                 size_t to_read,
                 folly::Optional<uint16_t> proto = folly::none);

  ProtocolReader(Slice src,
                 std::string context,
                 folly::Optional<uint16_t> proto = folly::none);

  ProtocolReader(const ProtocolReader&) = delete;
  ProtocolReader& operator=(const ProtocolReader&) = delete;

  ~ProtocolReader();

 private:
  // This is similar to ProtocolWriter, see ProtocolWriter.h
  alignas(void*) char src_space_[24];
  bool src_owned_ = false;

  std::string context_owned_;

  // Points either to dest_space or to an unowned Destination.
  Source* src_;
  // Points to either context_owned_ or to an unowned string.
  const char* context_;

  // Socket protocol
  folly::Optional<uint16_t> proto_;

  size_t src_left_;
  size_t nread_ = 0;
  // Protocol gate; read calls are ignored if `proto_' < `proto_gate_'
  uint16_t proto_gate_ = 0;
  Status status_ = E::OK;
  bool allow_trailing_bytes_ = false;

  void readImpl(void* out, size_t to_read);
  template <typename Fn>
  void readImplCb(size_t to_read, Fn&& fn);

  // Checks that there are at least `bytes_to_read` readable bytes remaining.
  // If there aren't, sets status_ to E::BADMSG and logs a protocol violation
  void checkReadableBytes(size_t bytes_to_read);

  // http://stackoverflow.com/questions/21512678/check-at-compile-time-is-a-template-type-a-vector
  template <typename T>
  struct is_std_vector : public std::false_type {};
  template <typename T, typename A>
  struct is_std_vector<std::vector<T, A>> : public std::true_type {};
};

}} // namespace facebook::logdevice
