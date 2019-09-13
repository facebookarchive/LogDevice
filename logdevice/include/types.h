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
#include <utility>

#include <folly/Range.h>

#include "logdevice/include/strong_typedef.h"

namespace facebook { namespace logdevice {

// Logs are uniquely identified by 64-bit unsigned ints
LOGDEVICE_STRONG_TYPEDEF(uint64_t, logid_t);

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

enum class Compression { NONE = 0x00, ZSTD = 0x01, LZ4 = 0x04, LZ4_HC = 0x05 };

/**
 * Returns "none", "zstd", "lz4" or "lz4_hc".
 */
std::string compressionToString(Compression c);

/*
 * Returns 0 on success, -1 on error.
 */
int parseCompression(const char* str, Compression* out_c);

}} // namespace facebook::logdevice

// Specialization of std::hash for logid_t
namespace std {
template <>
struct hash<facebook::logdevice::logid_t>
    : public facebook::logdevice::logid_t::Hash {};
} // namespace std
