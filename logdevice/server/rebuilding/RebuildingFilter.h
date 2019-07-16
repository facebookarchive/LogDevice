/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <unordered_map>

#include <folly/Memory.h>

#include "logdevice/common/RebuildingTypes.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/include/types.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"

/**
 * RebuildingReadFilter is used by rebuilding donors to filter records
 * read on disk according to the following criterias:
 *
 * * We filter records whose copyset does not intersect with the rebuilding set;
 * * We filter records for which SCD rules define that another node should be
 *   donor for their copyset.
 */

namespace facebook { namespace logdevice {

class RebuildingReadFilter : public LocalLogStoreReadFilter {
 public:
  enum class FilteredReason { SCD, NOT_DIRTY, DRAINED, TIMESTAMP };

  RebuildingReadFilter(std::shared_ptr<const RebuildingSet> rs, logid_t logid)
      : LocalLogStoreReadFilter(), logid_(logid), rebuildingSet(rs) {}
  bool operator()(logid_t log,
                  lsn_t lsn,
                  const ShardID* copyset,
                  const copyset_size_t copyset_size,
                  const csi_flags_t csi_flags,
                  RecordTimestamp min_ts,
                  RecordTimestamp max_ts) override;
  bool shouldProcessTimeRange(RecordTimestamp min,
                              RecordTimestamp max) override;

  /**
   * Update stats regarding skipped records.
   */
  void noteRecordFiltered(FilteredReason reason);

  const logid_t logid_; // for logging
  std::shared_ptr<const RebuildingSet> rebuildingSet;

  // Cached set of shards that are effectively not in the rebuilding set,
  // as long as the given time range is concerned.
  // This struct uses the fact that operator() is usually called many times
  // in a row with the same min_ts and max_ts.
  struct {
    RecordTimestamp minTs = RecordTimestamp::min();
    RecordTimestamp maxTs = RecordTimestamp::max();
    // ShardID+DataClass pairs whose dirty ranges have no intersection with
    // time range [minTs, maxTs].
    // The dirty ranges are rebuildingSet.shards[s].dc_dirty_ranges[dc].
    std::unordered_set<std::pair<ShardID, DataClass>> shardsOutsideTimeRange;

    bool valid(RecordTimestamp min_ts, RecordTimestamp max_ts) const {
      return min_ts == minTs && max_ts == maxTs;
    }

    void clear() {
      minTs = RecordTimestamp::min();
      maxTs = RecordTimestamp::max();
      shardsOutsideTimeRange.clear();
    }
  } timeRangeCache;

  size_t nRecordsLateFiltered{0};
  size_t nRecordsSCDFiltered{0};
  size_t nRecordsNotDirtyFiltered{0};
  size_t nRecordsDrainedFiltered{0};
  size_t nRecordsTimestampFiltered{0};
};

}} // namespace facebook::logdevice
