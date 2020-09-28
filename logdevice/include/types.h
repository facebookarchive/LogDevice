/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cassert>
#include <climits>
#include <cstdint>
#include <functional>
#include <limits>
#include <string>
#include <unordered_map>
#include <utility>

#include <folly/Optional.h>
#include <folly/Range.h>
#include <folly/io/IOBuf.h>

#include "logdevice/include/strong_typedef.h"

namespace facebook { namespace logdevice {

// Logs are uniquely identified by 64-bit unsigned ints
LOGDEVICE_STRONG_TYPEDEF(uint64_t, logid_t);

// Used by VersionedConfigStore
LOGDEVICE_STRONG_TYPEDEF(uint64_t, vcs_config_version_t);

constexpr logid_t LOGID_INVALID(0);
constexpr logid_t LOGID_INVALID2(~0);

// maximum number of bits in a log id
constexpr size_t LOGID_BITS(62);

// max valid data logid value. This accounts for internal logs.
// Not to be confused with numeric_limits<>::max().
constexpr logid_t LOGID_MAX((1ull << LOGID_BITS) - 1);

// max valid user data logid value.
constexpr logid_t USER_LOGID_MAX(LOGID_MAX.val_ - 1000);

// inclusive on both ends
typedef std::pair<logid_t, logid_t> logid_range_t;

// Log sequence numbers are 64-bit unsigned ints.
typedef uint64_t lsn_t;

// 0 is not a valid LSN.
const lsn_t LSN_INVALID = 0;

// Guaranteed to be less than or equal to the smallest valid LSN possible.
// Use this to seek to the oldest record in a log.
const lsn_t LSN_OLDEST = 1;

// Greatest valid LSN possible plus one.
const lsn_t LSN_MAX = ULLONG_MAX;

const uint64_t BYTE_OFFSET_INVALID = std::numeric_limits<uint64_t>::max();

// maximum size of CSID (Client Session ID)
const size_t MAX_CSID_SIZE = 256;

// Counter types to get offsets of OffsetMap
typedef uint8_t counter_type_t;

// counter type used in OffsetMap. It refers to amount of data in bytes of
// compressed data written to the log after sequencer batching logic
const counter_type_t BYTE_OFFSET = 246;

enum class Compression { NONE = 0x00, ZSTD = 0x01, LZ4 = 0x04, LZ4_HC = 0x05 };

/**
 * Returns "none", "zstd", "lz4" or "lz4_hc".
 */
std::string compressionToString(Compression c);

/*
 * Returns 0 on success, -1 on error.
 */
int parseCompression(const char* str, Compression* out_c);

/**
 * This const struct represents an opaque payload that a client may wish to
 * store in a log record on a LogDevice.
 *
 * Note that we can have one with a nullptr but size > 0.  This is used in
 * calculating how big something will be, without creating that something.
 * However, we don't allow the opposite: if the data is non-null, it must have
 * size > 0.
 */
struct Payload {
  Payload() noexcept : data_(nullptr), size_(0) {}

  Payload(const void* dt, size_t sz) noexcept : data_(dt), size_(sz) {
    if (data_) {
      assert(size_ != 0);
    }
  }

  Payload(const Payload& rhs) noexcept : data_(rhs.data_), size_(rhs.size_) {}

  Payload(Payload&& rhs) noexcept : data_(rhs.data_), size_(rhs.size_) {
    rhs.data_ = nullptr;
    rhs.size_ = 0;
  }

  Payload& operator=(Payload&& rhs) noexcept {
    if (this != &rhs) {
      data_ = rhs.data_;
      size_ = rhs.size_;
      rhs.data_ = nullptr;
      rhs.size_ = 0;
    }
    return *this;
  }

  Payload& operator=(const Payload& rhs) noexcept {
    data_ = rhs.data_;
    size_ = rhs.size_;
    return *this;
  }

  static Payload fromStringLiteral(const char* s) {
    return Payload(s, strlen(s));
  }

  Payload dup() const {
    void* data_copy = nullptr;
    if (data_) {
      assert(size_ != 0);
      data_copy = malloc(size_);
      if (!data_copy) {
        throw std::bad_alloc();
      }
      memcpy(data_copy, data_, size_);
    }
    return Payload(data_copy, size_);
  }

  const void* data() const noexcept {
    return data_;
  }

  size_t size() const noexcept {
    return size_;
  }

  // This makes a copy of the data.
  std::string toString() const {
    return std::string{static_cast<const char*>(data_), size_};
  }

  folly::StringPiece toStringPiece() const noexcept {
    return folly::StringPiece{static_cast<const char*>(data_), size_};
  }

  // returns maximum payload size supported by this implementation of
  // LogDevice client.
  // DEPRECATED! use Client::getMaxPayloadSize() instead. Appends that exceed
  // Client::getMaxPayloadSize() will fail.
  static constexpr size_t maxSize() {
    return 32 * 1024 * 1024;
  }

 private:
  const void* data_;
  size_t size_;
};

// Key used to uniquely identify payload in PayloadGroup.
typedef int32_t PayloadKey;

/**
 * An group of payloads that can be stored in a record and read from LogDevice.
 * Each payload in a group is uniquely identified by a key.
 * When using buffered writes payloads with same key from multiple records are
 * compressed together.
 * Users must not modify IOBufs once group is submitted for any operations.
 */
typedef std::unordered_map<PayloadKey, folly::IOBuf> PayloadGroup;

/**
 * Describes single payload in CompressedPayloads.
 */
struct PayloadDescriptor {
  /** Size of original (uncompressed) payload in bytes. */
  size_t uncompressed_size;
};

/**
 * Represents compressed payloads of a single key of a collection of
 * PayloadGroups. See CompressedPayloadGroups for details.
 */
struct CompressedPayloads {
  /** Compression algorithm, used to compress payloads. */
  Compression compression;

  /**
   * Descriptors of payloads. Descriptor only present if i-th PayloadGroup
   * contains the key.
   */
  std::vector<folly::Optional<PayloadDescriptor>> descriptors;

  /**
   * Concatenation of all payloads of one key, compressed using the algorithm
   * specified in compression: compress(payload1 + payload2 + ...).
   * To extract individual payloads, it must be uncompressed first, and then
   * split into chunks as specified in uncompressed_size of the descriptor.
   */
  folly::IOBuf payload;
};

/**
 * Represents restructured compressed collection of PayloadGroups.
 * Suppose the following collection of payload groups:
 * index   group ({key: payload})
 *   1     {k1: p11}, {k2: p12}
 *   2     {k1: p21}
 *   3     {k1: p31}, {k2: p32}
 *
 * It's restructured in the following way:
 * key   payload                    descriptors (uncompressed_size)
 *  k1   compress(p11 + p21 + p31)  {p11.size(), p21.size(), p31.size()}
 *  k2   compress(p12 + p32)        {p12.size(), none, p32.size()}
 *
 * Note that compression method can be different for different keys.
 * For example, if compressing the payload for k1 does not result in reduced
 * size campared to non-compressed payload, then Compression::NONE is used.
 */
using CompressedPayloadGroups =
    std::unordered_map<PayloadKey, CompressedPayloads>;

/**
 * Convert lsn into a string in the format e<epoch>n<esn>.
 */
std::string lsn_to_string(lsn_t lsn);

enum class Severity : unsigned {
  CRITICAL,
  ERROR,
  WARNING,
  NOTICE,
  INFO,
  DEBUG
};

std::string toString(const Severity&);

}} // namespace facebook::logdevice

// Specialization of std::hash for logid_t
namespace std {
template <>
struct hash<facebook::logdevice::logid_t>
    : public facebook::logdevice::logid_t::Hash {};
} // namespace std
