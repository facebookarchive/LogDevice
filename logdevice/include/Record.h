/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <map>
#include <new>
#include <utility>

#include <folly/Optional.h>

#include "logdevice/include/RecordOffset.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * Types of gaps in the numbering sequence of a log. See GapRecord below.
 */
enum class GapType {
  UNKNOWN = 0,      // default gap type; used by storage nodes when they don't
                    // have enough information to determine gap type
  BRIDGE = 1,       // a "bridge" that completes an epoch. This is benign and
                    // could be a result of sequencer failover or log
                    // reconfiguration. There is no data loss.
  HOLE = 2,         // a hole in the numbering sequence that appeared due
                    // to a sequencer crash. No acknowledged records were lost.
  DATALOSS = 3,     // all records in the gap were permanently lost
  TRIM = 4,         // a gap caused by trimming the log
  ACCESS = 5,       // a gap sent when the client does not have the required
                    // permissions
  NOTINCONFIG = 6,  // a gap issued up to until_lsn if the log was removed from
                    // the config
  FILTERED_OUT = 7, // a gap issued when this record is filtered out by server
                    // side filtering feature
  MAX
};

std::string gapTypeToString(GapType type);

enum class KeyType : uint8_t {
  FINDKEY = 0,    // Old find key API use this key. Assume monotonically
                  // increasing.
  FILTERABLE = 1, // Filterable key used by server-side filtering, no order
                  // assumed.
  MAX,
  UNDEFINED
};

/**
 * All that is known about a LogRecord is which log it belongs to.
 */
struct LogRecord {
  LogRecord() {}
  explicit LogRecord(logid_t log_id) : logid(log_id) {}

  logid_t logid;
};

/**
 * This struct contains the basic attributes of data records.
 *
 * We can't add attributes without breaking binary compatibility. That's ok
 * since we do not expect data records to acquire new basic attributes.
 *
 * Should any non-essential attributes come up in the future, we can add
 * support for them with a DataRecord::getExtendedAttributes() or similar.
 */
struct DataRecordAttributes {
  DataRecordAttributes() {}
  DataRecordAttributes(lsn_t ls,
                       std::chrono::milliseconds ts,
                       int bo = 0,
                       RecordOffset offsets = RecordOffset())
      : lsn(ls), timestamp(ts), batch_offset(bo), offsets(std::move(offsets)) {}

  lsn_t lsn; // log sequence number (LSN) of this record

  // timestamp in milliseconds since epoch assigned to this record by a
  // LogDevice server
  std::chrono::milliseconds timestamp;

  // If the record is part of a batch written through BufferedWriter, this
  // contains the record's 0-based index within the batch.  (All records that
  // get batched into a single write by BufferedWriter have the same LSN.)
  int batch_offset;

  // RecordOffset object containing information on the amount of data written
  // to the log (to which this record belongs) up to this record.
  // Currently supports BYTE_OFFSET which represents the number of bytes
  // written. if counter is invalid it will be set as BYTE_OFFSET_INVALID.
  // BYTE_OFFSET will be invalid if this attribute was not requested by client
  // (see includeByteOffset() reader option) or if it is not available to
  // storage nodes.
  RecordOffset offsets;
};

/**
 * DataRecords are log records that contain data payload written to the log
 * by LogDevice clients. In addition to payload every data record has a fixed
 * set of basic attributes.
 */
struct DataRecord : public LogRecord {
  DataRecord() {}
  DataRecord(logid_t log_id,
             const Payload& pl,
             lsn_t lsn = LSN_INVALID,
             std::chrono::milliseconds timestamp = std::chrono::milliseconds{0},
             int batch_offset = 0,
             RecordOffset offsets = RecordOffset())
      : LogRecord(log_id),
        payload(pl),
        attrs(lsn, timestamp, batch_offset, std::move(offsets)) {}

  DataRecord(logid_t log_id,
             Payload&& pl,
             lsn_t lsn = LSN_INVALID,
             std::chrono::milliseconds timestamp = std::chrono::milliseconds{0},
             int batch_offset = 0,
             RecordOffset offsets = RecordOffset())
      : LogRecord(log_id),
        payload(std::move(pl)),
        attrs(lsn, timestamp, batch_offset, std::move(offsets)) {}

  Payload payload;            // payload of this record
  DataRecordAttributes attrs; // attributes of this record. Not const,
                              // can be set after the record instance
                              // is constructed. Log::append() will use this.

  // LogDevice library internals may at runtime slip DataRecord subclasses
  // that free the payload on destruction, so allow a virtual destructor
  virtual ~DataRecord() {}

  // Not movable or copyable because of memory management - a subclass may be
  // managing the payload's lifetime
  DataRecord(const DataRecord& other) = delete;
  DataRecord(DataRecord&& other) = delete;
  DataRecord& operator=(const DataRecord& other) = delete;
  DataRecord& operator=(DataRecord&& other) = delete;
};

/**
 * GapRecords represent gaps in the numbering sequence of a given log.
 */
struct GapRecord : public LogRecord {
  GapRecord() {}
  GapRecord(logid_t log_id, GapType gtype, lsn_t losn, lsn_t hisn)
      : LogRecord(log_id), type(gtype), lo(losn), hi(hisn) {}

  GapType type; // see definition above
  lsn_t lo;     // lowest LSN in this gap (inclusive)
  lsn_t hi;     // highest LSN in this gap (inclusive)
};

/** Struct used to provide additional attributes to append.
 *
 *  optional_keys: optional keys that provided by client. (No assumed order)
 *                Previous `folly::Optional<String> key` field can be inserted
 *                as KeyType::FINDKEY in optional_keys.
 *
 *  counters: Map of values that should be tracked and aggregated by appender.
 *            See admin command "stats custom counters" for details.
 */
struct AppendAttributes {
  std::map<KeyType, std::string> optional_keys;
  folly::Optional<std::map<uint8_t, int64_t>> counters;
};

}} // namespace facebook::logdevice
