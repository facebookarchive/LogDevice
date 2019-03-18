/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

include "common.thrift"

namespace cpp2 facebook.logdevice.thrift
namespace py3 logdevice.admin
namespace php LogDevice
namespace wiki LogDevice.LogTree


// Response of getReplicationInfo()
struct LogTreeInfo {
  /**
   * The log tree version, version is u64 so we convert that to string because
   * thrift does not support u64.
   */
  1: string version,
  2: i64 num_logs,
  3: i64 max_backlog_seconds,
  4: bool is_fully_loaded,
}

struct TolerableFailureDomain {
  1: string domain,
  2: i32 count,
}

// Response of getReplicationInfo()
struct ReplicationInfo {
  /**
   * The log tree version, version is u64 so we convert that to string because
   * thrift does not support u64.
   */
  1: string version,
  /**
   * What is the most restrictive replication policy in
   * The entire LogTree
   */
  2: map<string, i32> narrowest_replication,
  /**
   * What is the smallest replication for a record in the
   * entire LogTree
   */
  3: i32 smallest_replication_factor,
  /**
   * How many of failure domain (domain) we can lose
   * in theory without losing read/write availability.
   */
  4: TolerableFailureDomain tolerable_failure_domains,
}

/**
 * Log group operations for throughput gathering
 */
enum LogGroupOperation {
  APPENDS = 0,
  READS = 1,
}

struct LogGroupThroughput {
  /**
   * appends or reads
   */
  1: LogGroupOperation operation,
  /**
   * B/s per time interval
   */
  2: list<i64> results,
}

struct LogGroupThroughputRequest {
   /**
    * appends or reads (by default: appends)
    */
   1: optional LogGroupOperation operation,
   /**
    * time period in seconds. Throughput is calculated for the given
    * time periods, for instance, 1 min (60 sec), 5 min (300 sec) and so on.
    * By default: 60 sec
    */
   2: optional list<i32> time_period,
   /**
    * log group name filtering
    */
   3: optional string log_group_name,
}

struct LogGroupThroughputResponse {
  /**
   * per-log-group append/read in B/s
   */
  1: map<string, LogGroupThroughput> throughput;
}
