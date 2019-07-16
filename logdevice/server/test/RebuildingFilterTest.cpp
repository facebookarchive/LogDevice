/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/rebuilding/RebuildingFilter.h"

#include <vector>

#include <gtest/gtest.h>

#include "logdevice/common/debug.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/server/RecordRebuildingStore.h"

using namespace facebook::logdevice;

// Convenient shortcuts for writting ShardIDs.
#define N0S0 ShardID(0, 0)
#define N1S0 ShardID(1, 0)
#define N2S0 ShardID(2, 0)
#define N3S0 ShardID(3, 0)
#define N4S0 ShardID(4, 0)
#define N5S0 ShardID(5, 0)
#define N6S0 ShardID(6, 0)
#define N7S0 ShardID(7, 0)
#define N8S0 ShardID(8, 0)
#define N9S0 ShardID(9, 0)
#define N10S0 ShardID(10, 0)

namespace facebook { namespace logdevice {

// Verify the special copyset and record filter used by the LogRebuilding
// state machine.
TEST(RebuidingFilterTest, RebuildingFilter) {
  auto rebuilding_set = std::make_shared<RebuildingSet>();
  ShardID donor_shard_id = N0S0;

  // Mark a node as dirty.
  RecordTimeIntervals n1_dirty_ranges;
  n1_dirty_ranges.insert(
      RecordTimeInterval(RecordTimestamp(std::chrono::seconds(10)),
                         RecordTimestamp(std::chrono::seconds(20))));
  n1_dirty_ranges.insert(
      RecordTimeInterval(RecordTimestamp(std::chrono::seconds(40)),
                         RecordTimestamp(std::chrono::seconds(100))));
  n1_dirty_ranges.insert(
      RecordTimeInterval(RecordTimestamp(std::chrono::seconds(150)),
                         RecordTimestamp(std::chrono::seconds(200))));
  PerDataClassTimeRanges n1_dc_dirty_ranges{
      {DataClass::APPEND, n1_dirty_ranges}};
  RebuildingNodeInfo n1_info(n1_dc_dirty_ranges, RebuildingMode::RESTORE);
  rebuilding_set->shards.emplace(N1S0, n1_info);

  // Add another node to the rebuilding set. This node has a full
  // shard that needs to be rebuilt.
  RebuildingNodeInfo n2_info(RebuildingMode::RESTORE);
  rebuilding_set->shards.emplace(N2S0, n2_info);

  logid_t log(42);
  RebuildingReadFilter filter(rebuilding_set, log);
  filter.scd_my_shard_id_ = donor_shard_id;

  std::vector<ShardID> copyset_no_hit{N0S0, N3S0, N4S0};
  std::vector<ShardID> copyset_dirty_hit{N0S0, N1S0, N4S0};
  std::vector<ShardID> copyset_non_dirty_hit{N0S0, N2S0, N3S0};
  std::vector<ShardID> copyset_both_hit{N0S0, N1S0, N2S0};

  LocalLogStoreRecordFormat::csi_flags_t append_csi_flags = 0;
  LocalLogStoreRecordFormat::csi_flags_t rebuild_csi_flags =
      LocalLogStoreRecordFormat::CSI_FLAG_WRITTEN_BY_REBUILDING;

  RecordTimestamp ts_in_n1_dirty_ranges(std::chrono::seconds(41));
  RecordTimestamp ts_not_in_n1_dirty_ranges(std::chrono::seconds(25));

  std::pair<RecordTimestamp, RecordTimestamp> range_intersecting_n1(
      RecordTimestamp(std::chrono::seconds(90)),
      RecordTimestamp(std::chrono::seconds(110)));
  std::pair<RecordTimestamp, RecordTimestamp> range_not_intersecting_n1(
      RecordTimestamp(std::chrono::seconds(101)),
      RecordTimestamp(std::chrono::seconds(149)));

  // Neither N1 nor N2 are in the copyset. The record should be skipped.
  EXPECT_FALSE(filter(log,
                      10,
                      copyset_no_hit.data(),
                      copyset_no_hit.size(),
                      append_csi_flags,
                      RecordTimestamp::min(),
                      RecordTimestamp::max()));

  // Both N1 and N2 should hit for an append if we don't supply timestamp
  // data.
  EXPECT_TRUE(filter(log,
                     11,
                     copyset_dirty_hit.data(),
                     copyset_dirty_hit.size(),
                     append_csi_flags,
                     RecordTimestamp::min(),
                     RecordTimestamp::max()));
  EXPECT_TRUE(filter(log,
                     12,
                     copyset_non_dirty_hit.data(),
                     copyset_non_dirty_hit.size(),
                     append_csi_flags,
                     RecordTimestamp::min(),
                     RecordTimestamp::max()));
  EXPECT_TRUE(filter(log,
                     13,
                     copyset_both_hit.data(),
                     copyset_both_hit.size(),
                     append_csi_flags,
                     RecordTimestamp::min(),
                     RecordTimestamp::max()));

  // N2 should still hit for an append regardless of the timestamp supplied.
  EXPECT_TRUE(filter(log,
                     14,
                     copyset_non_dirty_hit.data(),
                     copyset_non_dirty_hit.size(),
                     append_csi_flags,
                     ts_in_n1_dirty_ranges,
                     ts_in_n1_dirty_ranges));
  EXPECT_TRUE(filter(log,
                     15,
                     copyset_non_dirty_hit.data(),
                     copyset_non_dirty_hit.size(),
                     append_csi_flags,
                     ts_not_in_n1_dirty_ranges,
                     ts_not_in_n1_dirty_ranges));

  // N1 should only hit if the timestamp intersects one of its dirty ranges.
  EXPECT_TRUE(filter(log,
                     16,
                     copyset_dirty_hit.data(),
                     copyset_dirty_hit.size(),
                     append_csi_flags,
                     ts_in_n1_dirty_ranges,
                     ts_in_n1_dirty_ranges));
  EXPECT_FALSE(filter(log,
                      17,
                      copyset_dirty_hit.data(),
                      copyset_dirty_hit.size(),
                      append_csi_flags,
                      ts_not_in_n1_dirty_ranges,
                      ts_not_in_n1_dirty_ranges));

  // T43708398: N1 should hit even though the record was written by rebuilding.
  EXPECT_TRUE(filter(log,
                     18,
                     copyset_dirty_hit.data(),
                     copyset_dirty_hit.size(),
                     rebuild_csi_flags,
                     ts_in_n1_dirty_ranges,
                     ts_in_n1_dirty_ranges));
  EXPECT_FALSE(filter(log,
                      19,
                      copyset_dirty_hit.data(),
                      copyset_dirty_hit.size(),
                      rebuild_csi_flags,
                      ts_not_in_n1_dirty_ranges,
                      ts_not_in_n1_dirty_ranges));

  // But N2 should hit on records written by rebuilding.
  EXPECT_TRUE(filter(log,
                     20,
                     copyset_non_dirty_hit.data(),
                     copyset_non_dirty_hit.size(),
                     rebuild_csi_flags,
                     ts_in_n1_dirty_ranges,
                     ts_in_n1_dirty_ranges));
  EXPECT_TRUE(filter(log,
                     21,
                     copyset_non_dirty_hit.data(),
                     copyset_non_dirty_hit.size(),
                     rebuild_csi_flags,
                     ts_not_in_n1_dirty_ranges,
                     ts_not_in_n1_dirty_ranges));

  // Without record timestamp, N1 should be filtered out if time range
  // doesn't intersect the dirty ranges.
  EXPECT_TRUE(filter.shouldProcessTimeRange(
      range_not_intersecting_n1.first, range_not_intersecting_n1.second));
  EXPECT_FALSE(filter(log,
                      22,
                      copyset_dirty_hit.data(),
                      copyset_dirty_hit.size(),
                      append_csi_flags,
                      range_not_intersecting_n1.first,
                      range_not_intersecting_n1.second));
  EXPECT_TRUE(filter(log,
                     23,
                     copyset_non_dirty_hit.data(),
                     copyset_non_dirty_hit.size(),
                     append_csi_flags,
                     range_not_intersecting_n1.first,
                     range_not_intersecting_n1.second));
  // Range should be ignored if exact timestamp is provided.
  EXPECT_TRUE(filter(log,
                     24,
                     copyset_dirty_hit.data(),
                     copyset_dirty_hit.size(),
                     append_csi_flags,
                     ts_in_n1_dirty_ranges,
                     ts_in_n1_dirty_ranges));
  // shouldProcessTimeRange() intersects dirty ranges, N1 passes the filter.
  EXPECT_TRUE(filter.shouldProcessTimeRange(
      range_intersecting_n1.first, range_intersecting_n1.second));
  EXPECT_TRUE(filter(log,
                     25,
                     copyset_dirty_hit.data(),
                     copyset_dirty_hit.size(),
                     append_csi_flags,
                     range_intersecting_n1.first,
                     range_intersecting_n1.second));
  // T43708398: N1 should hit even though the record was written by rebuilding.
  EXPECT_TRUE(filter(log,
                     26,
                     copyset_dirty_hit.data(),
                     copyset_dirty_hit.size(),
                     rebuild_csi_flags,
                     range_intersecting_n1.first,
                     range_intersecting_n1.second));
  // If range in operator() is different from range in shouldProcessTimeRange(),
  // the one from operator() takes precedence.
  EXPECT_FALSE(filter(log,
                      27,
                      copyset_dirty_hit.data(),
                      copyset_dirty_hit.size(),
                      append_csi_flags,
                      range_not_intersecting_n1.first,
                      range_not_intersecting_n1.second));

  // Now remove the fully dirty node from rebuilding set and check that filter
  // will reject whole time ranges.
  rebuilding_set->shards.erase(N2S0);
  RebuildingReadFilter filter2(rebuilding_set, log);
  filter2.scd_my_shard_id_ = donor_shard_id;
  // Range is disjoint with the dirty ranges, filter it out.
  EXPECT_FALSE(filter2.shouldProcessTimeRange(
      range_not_intersecting_n1.first, range_not_intersecting_n1.second));
  // operator() should still return reasonable results.
  EXPECT_FALSE(filter2(log,
                       28,
                       copyset_dirty_hit.data(),
                       copyset_dirty_hit.size(),
                       append_csi_flags,
                       range_not_intersecting_n1.first,
                       range_not_intersecting_n1.second));
  // Range intersects the dirty ranges, keep it.
  EXPECT_TRUE(filter2.shouldProcessTimeRange(
      range_intersecting_n1.first, range_intersecting_n1.second));
  EXPECT_TRUE(filter2(log,
                      29,
                      copyset_dirty_hit.data(),
                      copyset_dirty_hit.size(),
                      append_csi_flags,
                      range_intersecting_n1.first,
                      range_intersecting_n1.second));
  // Set invalid range. Expect filter to be paranoid and accept it.
  EXPECT_TRUE(filter2.shouldProcessTimeRange(
      RecordTimestamp::max(), RecordTimestamp::min()));
  // Expect filter to be paranoid and assume we're in a dirty range.
  EXPECT_TRUE(filter2(log,
                      30,
                      copyset_dirty_hit.data(),
                      copyset_dirty_hit.size(),
                      append_csi_flags,
                      RecordTimestamp::max(),
                      RecordTimestamp::min()));
  // Set full range, expect the range and record to pass the filter.
  EXPECT_FALSE(filter2.shouldProcessTimeRange(
      range_not_intersecting_n1.first, range_not_intersecting_n1.second));
  EXPECT_TRUE(filter2.shouldProcessTimeRange(
      RecordTimestamp::min(), RecordTimestamp::max()));
  // Expect filter to be paranoid and assume we're in a dirty range.
  EXPECT_TRUE(filter2(log,
                      31,
                      copyset_dirty_hit.data(),
                      copyset_dirty_hit.size(),
                      append_csi_flags,
                      RecordTimestamp::min(),
                      RecordTimestamp::max()));
}

// Verify the special copyset and record filter used by the LogRebuilding
// state machine.
TEST(RebuidingFilterTest, RebuildingFilterDonorDirty) {
  auto rebuilding_set = std::make_shared<RebuildingSet>();
  ShardID donor_shard_id = N0S0;

  // Mark the donor as dirty for some time ranges
  RecordTimeIntervals n0_dirty_ranges;
  n0_dirty_ranges.insert(
      RecordTimeInterval(RecordTimestamp(std::chrono::seconds(2)),
                         RecordTimestamp(std::chrono::seconds(5))));
  n0_dirty_ranges.insert(
      RecordTimeInterval(RecordTimestamp(std::chrono::seconds(35)),
                         RecordTimestamp(std::chrono::seconds(42))));
  PerDataClassTimeRanges n0_dc_dirty_ranges{
      {DataClass::APPEND, n0_dirty_ranges}};
  RebuildingNodeInfo n0_info(n0_dc_dirty_ranges, RebuildingMode::RESTORE);
  rebuilding_set->shards.emplace(N0S0, n0_info);

  // Mark another node as dirty. It has one range that overlaps with the
  // donor.
  RecordTimeIntervals n1_dirty_ranges;
  n1_dirty_ranges.insert(
      RecordTimeInterval(RecordTimestamp(std::chrono::seconds(10)),
                         RecordTimestamp(std::chrono::seconds(20))));
  n1_dirty_ranges.insert(
      RecordTimeInterval(RecordTimestamp(std::chrono::seconds(40)),
                         RecordTimestamp(std::chrono::seconds(100))));
  n1_dirty_ranges.insert(
      RecordTimeInterval(RecordTimestamp(std::chrono::seconds(150)),
                         RecordTimestamp(std::chrono::seconds(200))));
  PerDataClassTimeRanges n1_dc_dirty_ranges{
      {DataClass::APPEND, n1_dirty_ranges}};
  RebuildingNodeInfo n1_info(n1_dc_dirty_ranges, RebuildingMode::RESTORE);
  rebuilding_set->shards.emplace(N1S0, n1_info);

  // Add yet another node to the rebuilding set. This node has a full
  // shard that needs to be rebuilt.
  RebuildingNodeInfo n2_info(RebuildingMode::RESTORE);
  rebuilding_set->shards.emplace(N2S0, n2_info);

  logid_t log(42);
  RebuildingReadFilter filter(rebuilding_set, log);
  filter.scd_my_shard_id_ = donor_shard_id;

  std::vector<ShardID> copyset_no_hit{N0S0, N3S0, N4S0};
  std::vector<ShardID> copyset_dirty_hit{N0S0, N1S0, N4S0};
  std::vector<ShardID> copyset_non_dirty_hit{N0S0, N2S0, N3S0};
  std::vector<ShardID> copyset_both_hit{N0S0, N1S0, N2S0};

  LocalLogStoreRecordFormat::csi_flags_t append_csi_flags = 0;
  LocalLogStoreRecordFormat::csi_flags_t rebuild_csi_flags =
      LocalLogStoreRecordFormat::CSI_FLAG_WRITTEN_BY_REBUILDING;

  RecordTimestamp ts_in_n0_and_n1_dirty_ranges(std::chrono::seconds(41));
  RecordTimestamp ts_in_n1_dirty_ranges(std::chrono::seconds(46));
  RecordTimestamp ts_not_in_any_dirty_ranges(std::chrono::seconds(25));
  ;

  // Since N0 is the donor, its dirty range is ignored. Dirty data is
  // always rebuilt in RESTORE mode meaning someone else must serve
  // as the donor for these records.  Additionally, neither N1 nor N2
  // are in the copyset. The record should be skipped.
  ASSERT_FALSE(filter(log,
                      10,
                      copyset_no_hit.data(),
                      copyset_no_hit.size(),
                      append_csi_flags,
                      RecordTimestamp::min(),
                      RecordTimestamp::max()));

  // Both N1 and N2 should hit for an append if we don't supply timestamp
  // data.
  ASSERT_TRUE(filter(log,
                     11,
                     copyset_dirty_hit.data(),
                     copyset_dirty_hit.size(),
                     append_csi_flags,
                     RecordTimestamp::min(),
                     RecordTimestamp::max()));
  ASSERT_TRUE(filter(log,
                     12,
                     copyset_non_dirty_hit.data(),
                     copyset_non_dirty_hit.size(),
                     append_csi_flags,
                     RecordTimestamp::min(),
                     RecordTimestamp::max()));
  ASSERT_TRUE(filter(log,
                     13,
                     copyset_both_hit.data(),
                     copyset_both_hit.size(),
                     append_csi_flags,
                     RecordTimestamp::min(),
                     RecordTimestamp::max()));

  // N2 should still hit for an append regardless of the timestamp supplied.
  ASSERT_TRUE(filter(log,
                     14,
                     copyset_non_dirty_hit.data(),
                     copyset_non_dirty_hit.size(),
                     append_csi_flags,
                     ts_in_n0_and_n1_dirty_ranges,
                     ts_in_n0_and_n1_dirty_ranges));
  ASSERT_TRUE(filter(log,
                     15,
                     copyset_non_dirty_hit.data(),
                     copyset_non_dirty_hit.size(),
                     append_csi_flags,
                     ts_in_n1_dirty_ranges,
                     ts_in_n1_dirty_ranges));
  ASSERT_TRUE(filter(log,
                     16,
                     copyset_non_dirty_hit.data(),
                     copyset_non_dirty_hit.size(),
                     append_csi_flags,
                     ts_not_in_any_dirty_ranges,
                     ts_not_in_any_dirty_ranges));

  // N1 should only hit if the timestamp intersects one of its dirty ranges.
  ASSERT_TRUE(filter(log,
                     17,
                     copyset_dirty_hit.data(),
                     copyset_dirty_hit.size(),
                     append_csi_flags,
                     ts_in_n0_and_n1_dirty_ranges,
                     ts_in_n0_and_n1_dirty_ranges));
  ASSERT_TRUE(filter(log,
                     18,
                     copyset_dirty_hit.data(),
                     copyset_dirty_hit.size(),
                     append_csi_flags,
                     ts_in_n1_dirty_ranges,
                     ts_in_n1_dirty_ranges));
  ASSERT_FALSE(filter(log,
                      19,
                      copyset_dirty_hit.data(),
                      copyset_dirty_hit.size(),
                      append_csi_flags,
                      ts_not_in_any_dirty_ranges,
                      ts_not_in_any_dirty_ranges));

  // T43708398: N1 should hit even though the record was written by rebuilding.
  ASSERT_TRUE(filter(log,
                     20,
                     copyset_dirty_hit.data(),
                     copyset_dirty_hit.size(),
                     rebuild_csi_flags,
                     ts_in_n0_and_n1_dirty_ranges,
                     ts_in_n0_and_n1_dirty_ranges));
  // T43708398: N1 should hit even though the record was written by rebuilding.
  ASSERT_TRUE(filter(log,
                     21,
                     copyset_dirty_hit.data(),
                     copyset_dirty_hit.size(),
                     rebuild_csi_flags,
                     ts_in_n1_dirty_ranges,
                     ts_in_n1_dirty_ranges));
  ASSERT_FALSE(filter(log,
                      22,
                      copyset_dirty_hit.data(),
                      copyset_dirty_hit.size(),
                      rebuild_csi_flags,
                      ts_not_in_any_dirty_ranges,
                      ts_not_in_any_dirty_ranges));

  // But N2 should hit on records written by rebuilding.
  ASSERT_TRUE(filter(log,
                     23,
                     copyset_non_dirty_hit.data(),
                     copyset_non_dirty_hit.size(),
                     rebuild_csi_flags,
                     ts_in_n0_and_n1_dirty_ranges,
                     ts_in_n0_and_n1_dirty_ranges));
  ASSERT_TRUE(filter(log,
                     24,
                     copyset_non_dirty_hit.data(),
                     copyset_non_dirty_hit.size(),
                     rebuild_csi_flags,
                     ts_in_n1_dirty_ranges,
                     ts_in_n1_dirty_ranges));
  ASSERT_TRUE(filter(log,
                     25,
                     copyset_non_dirty_hit.data(),
                     copyset_non_dirty_hit.size(),
                     rebuild_csi_flags,
                     ts_not_in_any_dirty_ranges,
                     ts_not_in_any_dirty_ranges));
}

}} // namespace facebook::logdevice
