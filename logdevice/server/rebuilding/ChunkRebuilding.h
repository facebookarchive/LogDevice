/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/RecordRebuildingBase.h"

namespace facebook { namespace logdevice {

struct ChunkAddress {
  logid_t log;
  lsn_t min_lsn;
  lsn_t max_lsn;

  bool operator<(const ChunkAddress& rhs) const;
};

// A sequence of records for the same log with the same copyset, as read from
// local log store.
// Note that a sticky copyset block may be split into multiple chunks,
// e.g. if the block crosses partition boundary.
struct ChunkData {
  struct RecordInfo {
    lsn_t lsn;
    // Offset of the end of this record in `buffer`.
    // Note that this can't be a pointer because `buffer`'s data can be
    // reallocated when new records are appended to it.
    size_t offset;
  };

  ChunkAddress address;
  // Used for seeding the rng for the copyset selector.
  size_t blockID;
  // Timestamp of the first record in the chunk.
  RecordTimestamp oldestTimestamp;
  // Information about records' epoch.
  std::shared_ptr<ReplicationScheme> replication;

  // All records concatenated together.
  std::string buffer;
  // Offset of end of each record in `buffer`.
  std::vector<RecordInfo> records;

  void addRecord(lsn_t lsn, Slice blob) {
    buffer.append(blob.ptr(), blob.size);
    records.push_back({lsn, buffer.size()});
  }

  size_t numRecords() const {
    return records.size();
  }

  // Size of all records, including header.
  size_t totalBytes() const {
    return buffer.size();
  }

  lsn_t getLSN(size_t idx) const {
    return records[idx].lsn;
  }

  // Serialized header+value combo, to be parsed
  // by LocalLogStoreRecordFormat::parse().
  Slice getRecordBlob(size_t idx) const {
    size_t off = idx ? records[idx - 1].offset : 0ul;
    return Slice(buffer.data() + off, records[idx].offset - off);
  }
};

}} // namespace facebook::logdevice
