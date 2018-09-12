/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <array>
#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include <folly/Optional.h>
#include <folly/dynamic.h>
#include "logdevice/common/SecurityInformation.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/LogAttributes.h"
#include "logdevice/include/NodeLocationScope.h"
#include "logdevice/include/types.h"

/**
 * @file Config reading and parsing.
 */

namespace folly {
struct dynamic;
}

namespace facebook { namespace logdevice { namespace logsconfig {
class LogAttributes;
}}} // namespace facebook::logdevice::logsconfig

namespace facebook { namespace logdevice { namespace configuration {
struct Log {
  using PermissionMap =
      std::unordered_map<std::string,
                         std::array<bool, static_cast<size_t>(ACTION::MAX)>>;

  using ACLList = std::vector<std::string>;

  /**
   * Symbolic name of the range to which this log belongs, or an empty
   * string if the range was not given a name.
   */
  std::string rangeName{};

  /**
   * Number of nodes on which to persist a record ('r' in the design doc).
   */
  folly::Optional<int> replicationFactor;

  /**
   * Number of extra copies the sequencer sends out to storage nodes
   * ('x' in the design doc).  If x > 0, this is done to improve
   * latency and availability; the sequencer will try to delete extra
   * copies after the write is finalized.
   */
  int extraCopies = 0;

  /**
   * The number of copies that must be acknowledged by storage nodes
   * as synced to disk before the record is acknowledged to client as
   * fully appended. Can be 0. Capped at replicationFactor.
   */
  int syncedCopies = 0;

  /**
   * The largest number of records not released for delivery that the
   * sequencer allows to be outstanding ('z' in the design doc).
   */
  int maxWritesInFlight = 2;

  /**
   * Does LogDevice assume that there is a single writer for the log?
   */
  bool singleWriter = false;

  /**
   * The location scope to enforce failure domain properties, by default
   * the scope is in the individual node level.
   */
  folly::Optional<NodeLocationScope> syncReplicationScope;

  /**
   * See LogAttributes.h
   */
  std::vector<std::pair<NodeLocationScope, int>> replicateAcross;

  /**
   * Duration that a record can exist in the log before it expires and
   * gets deleted. Valid value must be at least 1 second.
   */
  folly::Optional<std::chrono::seconds> backlogDuration;

  /**
   * Size of the nodeset for the log. Optional. If value is not specified,
   * the nodeset for the log is considered to be all storage nodes in the
   * config.
   */
  folly::Optional<int> nodeSetSize;

  /**
   * Maximum amount of time to artificially delay delivery of newly written
   * records (increases delivery latency but improves server and client
   * performance).
   */
  folly::Optional<std::chrono::milliseconds> deliveryLatency;

  /**
   * Indicate whether or not the Single Copy Delivery optimization should be
   * used.
   */
  bool scdEnabled = false;

  /**
   * Indicates whether or not to use Local Single Copy Delivery. This is ignored
   * if scdEnabled is false.
   */
  bool localScdEnabled = false;

  /**
   * If this is nonempty, writes to the log group are only allowed if
   * Client::addWriteToken() was called with this string.
   */
  folly::Optional<std::string> writeToken;

  /**
   * Maps a principal to a set of permissions. Used by ConfigPermissionChecker
   * and is populated when the permission_checker_type in the config file is
   * set to 'config'
   */
  PermissionMap permissions;

  /**
   * List of ACLs (Access Control List) names.
   * Used by permission checkers which rely on external systems.
   */
  ACLList acls;

  /**
   * List of ACLs (Access Control List) names.
   * Used by permission checkers which rely on external systems.
   * This list is used for logging only (shadow, darklaunch),
   * permissions are not enforced.
   */
  ACLList aclsShadow;

  /**
   * True if copysets on this log should be "sticky". See docblock in
   * StickyCopySetManager.h
   */
  bool stickyCopySets = false;

  /**
   * If true, write mutable per-epoch log metadata along with every data record.
   */
  bool mutablePerEpochLogMetadataEnabled = true;

  /**
   * The location affinity of the sequencer. Sequencer routing will try to
   * find a sequencer in the given location first before looking elsewhere.
   */
  folly::Optional<std::string> sequencerAffinity;

  /**
   * Enables or disables batching on sequencer.
   */
  folly::Optional<bool> sequencerBatching;

  /**
   * Buffered writes for a log will be flushed when
   * the oldest of them has been buffered for this amount of time.
   */
  folly::Optional<std::chrono::milliseconds> sequencerBatchingTimeTrigger;

  /**
   * Buffered writes for a log will be flushed as soon as this many payload
   * bytes are buffered.
   */
  folly::Optional<ssize_t> sequencerBatchingSizeTrigger;

  /**
   * Compression codec.
   */
  folly::Optional<Compression> sequencerBatchingCompression;

  /**
   * Writes with payload size greater than this value will not be batched.
   */
  folly::Optional<ssize_t> sequencerBatchingPassthruThreshold;

  /**
   * If true, reading the tail of the log will be significantly more efficient.
   * The trade-off is more memory usage depending on the record size.
   */
  bool tailOptimized = false;

  /**
   * Arbitrary fields that logdevice does not recognize
   */
  folly::dynamic customFields;

  Log();

  bool operator==(const Log& other) const;

  /*
   * Constructs a Log object out of a name and LogAttributes
   */
  static std::unique_ptr<Log> fromLogAttributes(
      const std::string& rangeName,
      const facebook::logdevice::logsconfig::LogAttributes& attrs);

  /*
   * Converts a Log object to LogAttributes, note that the LogAttributes object
   * doesn't have the rangeName property in it.
   */
  void
  toLogAttributes(facebook::logdevice::logsconfig::LogAttributes* out) const;
};

}}} // namespace facebook::logdevice::configuration
