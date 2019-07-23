/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <cstdint>
#include <limits>
#include <string>
#include <utility>

#include <folly/hash/Hash.h>

#include "logdevice/common/NodeID.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * Max payload size that we allow throughout the system.  In the API we
 * enforce the public-visible Client::getMaxPayloadSize(), but internally we
 * need to allow a bit more to accommodate BufferedWriter overhead.
 * NOTE: if you change the MAX_PAYLOAD_SIZE_PUBLIC, make sure to change the
 * Payload::maxSize() as well.
 */
constexpr size_t MAX_PAYLOAD_SIZE_PUBLIC = 32 * 1024 * 1024;
constexpr size_t MAX_PAYLOAD_EXTRA_SIZE = 32;
constexpr size_t MAX_PAYLOAD_SIZE_INTERNAL =
    (MAX_PAYLOAD_SIZE_PUBLIC + MAX_PAYLOAD_EXTRA_SIZE);

// Max payload size where copying into an evbuffer for an outbound
// socket is faster than queing by reference.
// See common/test/LibeventEvbufferBenchmark.
constexpr size_t MAX_COPY_TO_EVBUFFER_PAYLOAD_SIZE = 512;

// when e2e tracing in on we should have a tracing context encoded in the
// message, so we have to put a limit to that information size
constexpr size_t MAX_E2E_TRACING_CONTEXT_SIZE = 64;

// maximum number of bits in a log id internally, we use all bits in the
// uint64_t integer
constexpr size_t LOGID_BITS_INTERNAL = 64;

// Max valid logid for all logs (data logs, internal logs, and metadata logs).
// Not to be confused with numeric_limits<>::max() and with LOGID_MAX.
// the most significant bit is used to indicate a metadata log (see
// common/MetaDataLog.h) Note on LOGID_MAX: LOGID_MAX (defined in
// include/types.h) does not use the 62nd bit for data log ids. The reason is
// related to implementation details such as to avoid overflow in logid
// arithmetics and to leave room for reserved keys in folly::AtomicHashMap.
constexpr logid_t LOGID_MAX_INTERNAL(LOGID_MAX.val_ |
                                     1ull << (LOGID_BITS_INTERNAL - 1));

static_assert(LOGID_MAX_INTERNAL.val_ <
                  std::numeric_limits<logid_t::raw_type>::max() - 2,
              "logid range contains reserved keys in folly::AtomicHashMap");

LOGDEVICE_STRONG_TYPEDEF(uint32_t, epoch_t); // epoch numbers

constexpr epoch_t EPOCH_INVALID(0); // "invalid" epoch value
constexpr epoch_t EPOCH_MIN(1);     // smallest valid epoch value
constexpr epoch_t EPOCH_MAX(std::numeric_limits<uint32_t>::max() - 1);

inline epoch_t previous_epoch(epoch_t e) {
  ld_check(e != EPOCH_INVALID);
  return epoch_t(e.val_ - 1);
}

static_assert(EPOCH_INVALID.val_ < EPOCH_MIN.val_,
              "EPOCH_INVALID must be smaller than EPOCH_MIN");
static_assert(EPOCH_MAX.val_ <= std::numeric_limits<epoch_t::raw_type>::max(),
              "EPOCH_MAX must not exceed the largest value representable by "
              "epoch_t");

constexpr bool epoch_valid(epoch_t e) {
  return e >= EPOCH_MIN && e <= EPOCH_MAX;
}

constexpr bool epoch_valid_or_unset(epoch_t e) {
  return e <= EPOCH_MAX;
}

LOGDEVICE_STRONG_TYPEDEF(uint32_t, esn_t); // sequence numbers within an epoch

constexpr esn_t ESN_INVALID(0); // "invalid " ESN value
constexpr esn_t ESN_MIN(1);     // smallest valid ESN value
constexpr esn_t ESN_MAX(std::numeric_limits<uint32_t>::max());

static_assert(ESN_MAX.val_ <= std::numeric_limits<esn_t::raw_type>::max(),
              "ESN_MAX must not exceed the largest value representable by "
              "esn_t");

/**
 * Converts a pair (epoch,esn) to lsn_t, which is currently uint64_t.
 * If/when we go to 128-bit externally visible lsn_t, this function
 * will be replaced with its constructor.
 */

constexpr lsn_t compose_lsn(epoch_t epoch, esn_t esn) {
  return ((lsn_t)epoch.val_ << 32) | (lsn_t)esn.val_;
}

constexpr epoch_t lsn_to_epoch(lsn_t lsn) {
  return epoch_t(lsn >> 32);
}

constexpr esn_t lsn_to_esn(lsn_t lsn) {
  return esn_t(lsn & 0xffffffff);
}

constexpr bool same_epoch(lsn_t lsn1, lsn_t lsn2) {
  return lsn_to_epoch(lsn1) == lsn_to_epoch(lsn2);
}

static_assert(sizeof(esn_t::raw_type) == 4,
              "lsn to esn conversion functions require 4 byte esn_t");

static_assert(!epoch_valid(lsn_to_epoch(LSN_MAX)),
              "Epoch for LSN_MAX should not be a valid epoch.");

/**
 * Values of type copyset_size_t contain the size of a record copyset.
 * A copyset is a set of nodes on which all copies of a particular log
 * record end up stored. copyset_off_t is a signed version of copyset_size_t
 * used to represent offsets into copysets.
 */
typedef uint8_t copyset_size_t;
typedef int8_t copyset_off_t;

// maximum number of nodes in a record copyset
const size_t COPYSET_SIZE_MAX = 127;

static_assert(COPYSET_SIZE_MAX - 1 <= std::numeric_limits<copyset_off_t>::max(),
              "COPYSET_SIZE_MAX and copyset_off_t are incompatible");

// Maximum number of nodes in a log's nodeset. Currently it can't be greater
// than COPYSET_SIZE_MAX because recovery sometimes wants to use the entire
// nodeset as a copyset.
const nodeset_size_t NODESET_SIZE_MAX = COPYSET_SIZE_MAX;

/**
 * Type used to identify a NodesetState object
 */
LOGDEVICE_STRONG_TYPEDEF(uint64_t, nodeset_state_id_t);

/**
 * Type of identifier of a ClientReadStream (a ClientReadStream represents
 * state for a Client reading a log, likely from multiple storage nodes).  It
 * is assigned by a worker thread when a Client starts reading a log, and
 * passed back to the Client.  The identifier can later be used to stop
 * reading.
 *
 * The identifier is also passed around over the network to identify the read
 * stream.  Servers can use (ClientID, read_stream_id_t) to keep separate
 * states when a Client reads the same log more than once.
 *
 * Example usage:
 * - Application starts reading the same log twice via the same Client, with
 *   two different record callbacks.
 * - C1 sends START(log_id=1, start_lsn=100, read_stream_id=1) to N1
 * - C1 sends START(log_id=1, start_lsn=200, read_stream_id=2) to N1
 * - N1 sends RECORD(log_id=1, read_stream_id=1, lsn=100) to C1.  Based on
 *   read_stream_id=1, the client library can call the correct (first) callback.
 * - N1 sends RECORD(log_id=1, read_stream_id=2, lsn=200) to C1.  Based on
 *   read_stream_id=2, the client library can call the second callback.
 * - C1 sends STOP(log_id=1, read_stream_id=1) to N1.  The server knows to
 *   stop the first read stream but keep sending records for the other.
 *
 * 0 is not a valid identifier; ld_check(id != READ_STREAM_ID_INVALID) can be
 * used to verify that a reasonable value was passed in.
 */
LOGDEVICE_STRONG_TYPEDEF(uint64_t, read_stream_id_t);

constexpr read_stream_id_t READ_STREAM_ID_INVALID(0);

/**
 * Type of identifiers of individual Workers in a Processor.
 * Note that a worker_id_t alone does not identify a worker uniquely.
 * The worker identifier is a pair (WorkerType, worker_id_t).
 */
LOGDEVICE_STRONG_TYPEDEF(int, worker_id_t);

constexpr worker_id_t WORKER_ID_INVALID(-1);

/**
 * Type used to identify EpochRecovery machines.
 */
LOGDEVICE_STRONG_TYPEDEF(uint64_t, recovery_id_t);

/**
 * Type used to identify a logdevice request
 */
LOGDEVICE_STRONG_TYPEDEF(uint64_t, request_id_t);
constexpr request_id_t REQUEST_ID_INVALID(0);

/**
 * Type used to identify a logdevice stream request. A stream request id is a
 * 128 bit unsigned integer, composed of a stream id and sequence number. The
 * stream id must be positive.
 */
LOGDEVICE_STRONG_TYPEDEF(uint64_t, write_stream_id_t);
LOGDEVICE_STRONG_TYPEDEF(uint64_t, write_stream_seq_num_t);
struct write_stream_request_id_t {
  write_stream_id_t id;
  write_stream_seq_num_t seq_num;
};
constexpr write_stream_id_t WRITE_STREAM_ID_INVALID(0UL);
constexpr write_stream_request_id_t WRITE_STREAM_REQUEST_ID_INVALID = {
    WRITE_STREAM_ID_INVALID,
    write_stream_seq_num_t(0UL)};
bool write_stream_request_id_valid(write_stream_request_id_t req_id);

/**
 * Type used to identify a run of LogRebuilding state machine.
 * TODO (#24665001): In rebuilding v2 it's used to identiy chunks rather than
 * logs. Rename to chunk_rebuilding_id_t.
 */
LOGDEVICE_STRONG_TYPEDEF(uint64_t, log_rebuilding_id_t);

constexpr log_rebuilding_id_t LOG_REBUILDING_ID_INVALID(0);

/**
 * Filter version used in START messages. This is bumped each time a new START
 * message is sent with a different filtering policy. The storage nodes use this
 * to differentiate between the case when a START message it receives is a
 * duplicate of another START message and the stream should not rewind and the
 * case when the START message is sent because the client wants to change the
 * filtering mode and the stream should rewind.
 */
LOGDEVICE_STRONG_TYPEDEF(uint64_t, filter_version_t);

/**
 * Type of handle provided by StartReadingRequest, used to stop reading and
 * other cases where the client library needs to talk to the worker thread
 * that owns the read stream.
 */
struct ReadingHandle {
  worker_id_t worker_id;
  read_stream_id_t read_stream_id;

  bool operator<(ReadingHandle rhs) const {
    return std::tie(worker_id, read_stream_id) <
        std::tie(rhs.worker_id, rhs.read_stream_id);
  }
};

// max number of workers any Processor can have
// Note: RWTicketSpinLockT type in LogStorageState, EpochRecordCache and
// PartitionedRocksDBStore have to be modified if MAX_WORKERS > 255
const size_t MAX_WORKERS = 128;

// Max number of shards that a logdevice storage node can support.
const size_t MAX_SHARDS = 128;

typedef int16_t shard_index_t; // Offset of a shard on a node, zero-based.
typedef int16_t shard_size_t;  // Number of shards;
static_assert(MAX_SHARDS <= std::numeric_limits<shard_index_t>::max(), "");

/**
 * A general-purpose pointer+size pair.
 */
struct Slice {
  Slice(const void* dt, size_t sz) noexcept : data(dt), size(sz) {}

  Slice() noexcept : data(nullptr), size(0) {}

  explicit Slice(const Payload& payload) noexcept
      : data(payload.data()), size(payload.size()) {}

  static Slice fromString(const std::string& s) {
    return Slice(s.data(), s.size());
  }

  const char* ptr() const {
    return reinterpret_cast<const char*>(data);
  }

  const void* data;
  size_t size;
};

/**
 * An identifier used to associate LocalLogStore writes with the in-core
 * state that must be flushed before the write can be considered
 * committed to stable storage. A write is known to be retired when
 * LocalLogStore::flushedUpTo() returns a FlushToken that is greater
 * than or equal to the FlushToken associated with a give write.
 */
using FlushToken = uint64_t;
constexpr FlushToken FlushToken_INVALID = 0;
constexpr FlushToken FlushToken_MIN = 1;
constexpr FlushToken FlushToken_MAX = std::numeric_limits<FlushToken>::max();

using ServerInstanceId = uint64_t;
constexpr ServerInstanceId ServerInstanceId_INVALID = 0;

/**
 * Partition id in partitioned log store.
 */
typedef uint64_t partition_id_t;

constexpr partition_id_t PARTITION_INVALID(0);
constexpr partition_id_t
    PARTITION_MAX(std::numeric_limits<partition_id_t>::max());

/**
 * Integer identifier for BufferedWriterShard instances attached to a Client.
 */
LOGDEVICE_STRONG_TYPEDEF(uint64_t, buffered_writer_id_t);

/**
 * Hash function for 64 bit integer types using twang_mix64 from folly.
 * Has better distribution of output values compared with the hash method
 * defined in LOGDEVICE_STRONG_TYPEDEF
 */
template <class INTTYPE>
struct Hash64 {
  static_assert(std::is_integral<INTTYPE>::value, "Key must be integer.");
  static_assert(sizeof(INTTYPE) <= sizeof(uint64_t),
                "Key must be no longer than 64 bit.");
  uint64_t operator()(const INTTYPE key) const {
    return folly::hash::twang_mix64((uint64_t)key);
  }
};

/**
 * Hash function for an enum type
 */
template <typename EnumType>
struct HashEnum {
  static_assert(std::is_enum<EnumType>::value, "Key must be an enum type");

  using under_type = typename std::underlying_type<EnumType>::type;
  size_t operator()(const EnumType& key) const {
    return std::hash<under_type>()(static_cast<under_type>(key));
  }
};

const uint32_t WAVE_MAX = std::numeric_limits<uint32_t>::max();

typedef uint32_t SHARD_NEEDS_REBUILD_flags_t;
typedef uint32_t SHARD_IS_REBUILT_flags_t;

// Used for requesting the findTime index to be written in RocksDB.
constexpr char FIND_TIME_INDEX = 'f';
constexpr int FIND_TIME_KEY_SIZE = 8;

// Used for requesting the findKey indexes to be written in RocksDB.
constexpr char FIND_KEY_INDEX = 'k';

// Used by VersionedConfigStore
LOGDEVICE_STRONG_TYPEDEF(uint64_t, vcs_config_version_t);

/**
 * Config version used to detect stale configs.
 */
LOGDEVICE_STRONG_TYPEDEF(uint32_t, config_version_t);

constexpr config_version_t CONFIG_VERSION_INVALID(0);

// Number (or total weight) of allowed operations per unit of time.
// Zero denominator means unlimited. Zero numerator means limited to zero.
// It's illegal for both to be zero at the same time.
using rate_limit_t = std::pair<size_t, std::chrono::milliseconds>;
constexpr rate_limit_t RATE_UNLIMITED =
    rate_limit_t(1, std::chrono::milliseconds(0));

// the type of STORE_Header::flags
typedef uint32_t STORE_flags_t;

// Used in tests. See RocksDBKeyFormat.h
// TODO (#10357210): remove when all data is converted.
enum class DataKeyFormat {
  DEFAULT,
  OLD,
  NEW,
};

}} // namespace facebook::logdevice

// Specialization of std::hash for log_rebuilding_id_t
namespace std {
template <>
struct hash<facebook::logdevice::log_rebuilding_id_t>
    : public facebook::logdevice::log_rebuilding_id_t::Hash {};
} // namespace std
