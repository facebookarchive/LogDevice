/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
/* can be included multiple times */

#ifndef TIME_SERIES_DEFINE
#error TIME_SERIES_DEFINE() macro not defined
#define TIME_SERIES_DEFINE(...)
#endif

// Fields:
// 1) name,
// 2) string names for querying via admin command,
// 3) time intervals over which to accumulate the stats
// 4) number of buckets for time series

// The choice of intervals and buckets per interval depends on the required
// granularity of reporting (expected queries) and desired memory usage.
//
// E.g., with {10, 60, 600} seconds and 2 buckets we have:
// - 5-second granularity at the 10-second level,
// - 30-second granularity at the 1-minute level, and
// - 5-minute granularity at the 10-minute level
// ... at a cost of 3 * 2 * 16B = 96B per (active) log group per worker.


// Append payload bytes received
TIME_SERIES_DEFINE(append_in_bytes,
                   std::set<std::string>({"appends_in", "appends"}),
                   std::vector<std::chrono::milliseconds>({
                       std::chrono::seconds(60),
                       std::chrono::seconds(300),
                       std::chrono::seconds(600)}),
                   2);
// Append bytes after sequencer batching
TIME_SERIES_DEFINE(append_out_bytes,
                   std::set<std::string>({"appends_out"}),
                   std::vector<std::chrono::milliseconds>({
                       std::chrono::seconds(60),
                       std::chrono::seconds(680)}),
                   2);
// Payload bytes sent out in records
TIME_SERIES_DEFINE(record_bytes,
                   std::set<std::string>({"reads"}),
                   std::vector<std::chrono::milliseconds>({
                       std::chrono::seconds(900),
                       std::chrono::seconds(1800),
                       std::chrono::seconds(3600)}),
                   2);

#undef TIME_SERIES_DEFINE
