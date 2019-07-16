/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <folly/Optional.h>

#include "logdevice/common/Timestamp.h"
#include "logdevice/common/debug.h"
#include "logdevice/server/LogRebuilding.h"

namespace facebook { namespace logdevice {

bool RebuildingReadFilter::
operator()(logid_t log,
           lsn_t lsn,
           const ShardID* copyset,
           copyset_size_t copyset_size,
           LocalLogStoreRecordFormat::csi_flags_t flags,
           RecordTimestamp min_ts,
           RecordTimestamp max_ts) {
  required_in_copyset_.clear();
  scd_known_down_.clear();

  if (flags & LocalLogStoreRecordFormat::CSI_FLAG_DRAINED) {
    noteRecordFiltered(FilteredReason::DRAINED);
    return false;
  }

  auto filtered_reason = FilteredReason::NOT_DIRTY;
  // TODO(T43708398): in order to work around T43708398, we always look for
  // append dirty ranges.
  auto dc = DataClass::APPEND;

  for (copyset_off_t i = 0; i < copyset_size; ++i) {
    ShardID shard = copyset[i];
    auto node_kv = rebuildingSet->shards.find(shard);
    if (node_kv != rebuildingSet->shards.end()) {
      auto& node_info = node_kv->second;
      if (!node_info.dc_dirty_ranges.empty()) {
        // Node is only partially dirty (time range data is provided).
        ld_check(node_info.mode == RebuildingMode::RESTORE);

        // Exclude if DataClass/Timestamp do not match.
        auto dc_tr_kv = node_info.dc_dirty_ranges.find(dc);
        if (dc_tr_kv == node_info.dc_dirty_ranges.end() ||
            dc_tr_kv->second.empty()) {
          // DataClass isn't dirty.
          // We should never serialize an empty DataClass since it is
          // not dirty, but we tolerate it in production builds.
          ld_check(dc_tr_kv == node_info.dc_dirty_ranges.end());
          continue;
        }

        // Check if the record's timestamp intersects some of the
        // time ranges of this shard in the rebuilding set.
        const auto& time_ranges = dc_tr_kv->second;
        bool intersects;
        if (timeRangeCache.valid(min_ts, max_ts)) {
          // (a) Just like (d), but we already have a cached result for this
          //     time range.
          intersects = !timeRangeCache.shardsOutsideTimeRange.count(
              std::make_pair(shard, dc));
        } else if (min_ts == max_ts) {
          // (b) We know the exact timestamp of the record.
          intersects = time_ranges.find(min_ts) != time_ranges.end();
        } else if (min_ts > max_ts) {
          // (c) Invalid range. Be paranoid and assume that it intersects the
          //     rebuilding range.
          RATELIMIT_INFO(
              std::chrono::seconds(10),
              2,
              "operator() called with min_ts > max_ts: %s > %s. Log: %lu",
              min_ts.toString().c_str(),
              max_ts.toString().c_str(),
              logid_.val_);
          intersects = true;
        } else {
          // (d) We don't know the exact timestamp, but we know that it's
          //     somewhere in [min_ts, max_ts] range. Check if this range
          //     intersects any of the rebuilding time ranges for this shard.
          intersects = boost::icl::intersects(
              time_ranges, RecordTimeInterval(min_ts, max_ts));
          // At the time of writing, this should be unreachable with all
          // existing LocalLogStore::ReadIterator implementations.
          // If you see this message, it's likely that there's a bug.
          RATELIMIT_INFO(
              std::chrono::seconds(10),
              1,
              "Time range in operator() doesn't match time range in "
              "shouldProcessTimeRange(). Suspicious. Please check the code.");
        }

        if (!intersects) {
          // Record falls outside a dirty time range.
          filtered_reason = FilteredReason::TIMESTAMP;
          continue;
        }
      }

      // Records inside a dirty region may be lost, but some/all may
      // have been durably stored before we crashed. We only serve as a
      // donor for records we happen to find in a dirty region if some
      // other node's failure also impacts the record (i.e. if we get past
      // this point during a different iteration of this loop).
      ld_check(scd_my_shard_id_.isValid());
      if (shard == scd_my_shard_id_ &&
          node_kv->second.mode == RebuildingMode::RESTORE) {
        continue;
      }

      // Node's shard either needs to be fully rebuilt or is dirty in this
      // region and we can serve as a donor.
      required_in_copyset_.push_back(shard);

      // If the rebuilding node is rebuilding in RESTORE mode, it should not
      // participate as a donor. Add it to the known down list so that other
      // nodes will send the records for which it was the leader.
      //
      // Note: If this node is in the rebuilding set for a time-ranged
      //       rebuild, and that range overlaps with under-replication
      //       on another node, it is possible for our node id to be added
      //       here. However, the SCD filtering logic ignores known_down
      //       for the local node id and we will be considered a donor for
      //       the record. This can lead to overreplication, but also
      //       ensures that data that can be rebuilt isn't skipped.
      if (node_kv->second.mode == RebuildingMode::RESTORE) {
        scd_known_down_.push_back(shard);
      }
    }
  }

  // Perform SCD copyset filtering.
  bool result = !required_in_copyset_.empty();
  if (result) {
    filtered_reason = FilteredReason::SCD;
    result = LocalLogStoreReadFilter::operator()(
        log, lsn, copyset, copyset_size, flags, min_ts, max_ts);
  }
  if (!result) {
    noteRecordFiltered(filtered_reason);
  }
  return result;
}

bool RebuildingReadFilter::shouldProcessTimeRange(RecordTimestamp min,
                                                  RecordTimestamp max) {
  auto& cache = timeRangeCache;
  cache.clear();

  if (min > max) {
    RATELIMIT_INFO(
        std::chrono::seconds(10),
        2,
        "shouldProcessTimeRange() called with min > max: %s > %s. Log: %lu",
        min.toString().c_str(),
        max.toString().c_str(),
        logid_.val_);
    // Be conservative.
    return true;
  }

  cache.minTs = min;
  cache.maxTs = max;
  bool have_shards_intersecting_range = false;

  for (const auto& node_kv : rebuildingSet->shards) {
    ShardID shard = node_kv.first;
    auto& node_info = node_kv.second;
    if (node_info.dc_dirty_ranges.empty()) {
      // Empty dc_dirty_ranges means that the node is dirty for all time points.
      have_shards_intersecting_range = true;
      continue;
    }
    // Node is only partially dirty (time range data is provided).
    ld_check(node_info.mode == RebuildingMode::RESTORE);
    for (const auto& dc_tr_kv : node_info.dc_dirty_ranges) {
      if (dc_tr_kv.second.empty()) {
        ld_check(false);
        continue;
      }

      auto& time_ranges = dc_tr_kv.second;
      if (boost::icl::intersects(time_ranges, RecordTimeInterval(min, max))) {
        // The shard is dirty for some of the timestamps in [min, max].
        have_shards_intersecting_range = true;
      } else {
        // The shard is clean for all timestamps in [min, max].
        cache.shardsOutsideTimeRange.emplace(shard, dc_tr_kv.first);
      }
    }
  }

  return have_shards_intersecting_range;
}

/**
 * Update stats regarding skipped records.
 */
void RebuildingReadFilter::noteRecordFiltered(FilteredReason reason) {
  switch (reason) {
    case FilteredReason::SCD:
      ++nRecordsSCDFiltered;
      break;
    case FilteredReason::NOT_DIRTY:
      ++nRecordsNotDirtyFiltered;
      break;
    case FilteredReason::DRAINED:
      ++nRecordsDrainedFiltered;
      break;
    case FilteredReason::TIMESTAMP:
      // Timestamp data is only available after retrieving the full record.
      ++nRecordsLateFiltered;
      ++nRecordsTimestampFiltered;
      break;
  }
}

}} // namespace facebook::logdevice
