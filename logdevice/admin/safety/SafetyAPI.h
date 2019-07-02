/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <map>

#include "logdevice/common/AuthoritativeStatus.h"
#include "logdevice/common/ClusterState.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/configuration/Node.h"
#include "logdevice/common/configuration/ReplicationProperty.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/NodeLocationScope.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

using SafetyMargin = std::map<NodeLocationScope, int>;

struct Impact {
  // Empty constructor sets everything as if the operation is safe.
  Impact();
  /**
   * A data structure that holds extra information about the storage set that
   * resemble the status at the time of the safety check.
   */
  struct ShardMetadata {
    AuthoritativeStatus auth_status;
    bool has_dirty_ranges;
    bool is_alive;
    configuration::StorageState storage_state;
    folly::Optional<NodeLocation> location;
    bool operator==(ShardMetadata const& x) const {
      return x.auth_status == auth_status && x.is_alive == is_alive &&
          x.storage_state == storage_state && x.location == location &&
          has_dirty_ranges == x.has_dirty_ranges;
    }
    bool operator!=(ShardMetadata const& x) const {
      return !(*this == x);
    }
  };

  // For each index in the storage set, what was the cluster state and
  using StorageSetMetadata = std::vector<Impact::ShardMetadata>;

  /**
   * A data structure that holds the operation impact on a specific epoch in a
   * log.
   */
  struct ImpactOnEpoch {
    logid_t log_id = LOGID_INVALID;
    epoch_t epoch = EPOCH_INVALID;
    StorageSet storage_set;
    StorageSetMetadata storage_set_metadata;
    ReplicationProperty replication;
    int impact_result = ImpactResult::INVALID;
    // Ctor used when there is no impact on log
    explicit ImpactOnEpoch(logid_t log_id)
        : log_id(log_id), impact_result(ImpactResult::NONE) {}
    ImpactOnEpoch(logid_t log_id,
                  epoch_t epoch,
                  StorageSet storage_set,
                  StorageSetMetadata storage_set_metadata,
                  ReplicationProperty replication,
                  int impact_result)
        : log_id(log_id),
          epoch(epoch),
          storage_set(std::move(storage_set)),
          storage_set_metadata(std::move(storage_set_metadata)),
          replication(std::move(replication)),
          impact_result(impact_result) {}
    // Needed for the python bindings
    bool operator==(const ImpactOnEpoch& x) const {
      return x.log_id == log_id && x.epoch == epoch &&
          x.storage_set == storage_set &&
          x.storage_set_metadata == storage_set_metadata &&
          x.replication == replication && x.impact_result == impact_result;
    }
    bool operator!=(const ImpactOnEpoch& x) const {
      return !(*this == x);
    }
  };

  enum ImpactResult {
    NONE = 0,
    // operation could lead to rebuilding stall, as full rebuilding
    // of all logs a is not possible due to historical nodesets
    REBUILDING_STALL = (1u << 1),
    // operation could lead to loss of write availability.
    WRITE_AVAILABILITY_LOSS = (1u << 2),
    // operation could lead to loss of read availability,
    // as there is no f-majority for certain logs
    READ_AVAILABILITY_LOSS = (1u << 3),
    // Impact Could not be established due to an error.
    INVALID = (1u << 30),
  };

  // bit set of ImpactResult
  int32_t result;

  // Set of data logs affected.
  std::vector<ImpactOnEpoch> logs_affected;

  // Whether metadata logs are also affected (ie the operations have impact on
  // the metadata nodeset or the internal logs).
  bool internal_logs_affected = false;
  // The total number of logs checked during this operation
  size_t total_logs_checked{0};
  // The total duration in seconds during this operation
  std::chrono::seconds total_duration{0};

  explicit Impact(
      int result,
      std::vector<ImpactOnEpoch> logs_affected = {},
      bool internal_logs_affected = false,
      size_t total_logs_checked = 0,
      std::chrono::seconds total_duration = std::chrono::seconds(0));

  bool operator==(const Impact& x) const {
    // We don't validate very field here as we care only about the semantic of
    // equality rather than being identical.
    return x.logs_affected == logs_affected &&
        x.internal_logs_affected == internal_logs_affected;
  }

  /**
   * Combines takes an impact object and combines its result with this one.
   * Note that this doesn't combine the total_time.
   * It will also take into account the error_sample_size given to ensure we
   * are not pushing so many samples.
   *
   * If error_sample_size is < 0 we will push all samples.
   */
  static Impact merge(Impact i1, const Impact& i2, size_t error_sample_size);

  static folly::Expected<Impact, Status>
  merge(folly::Expected<Impact, Status> i,
        const folly::Expected<Impact, Status>& i2,
        size_t error_sample_size);

  std::string toString() const;
  static std::string toStringImpactResult(int);
};

/**
 * @param descriptor A descriptor describing one safety margin or a set of .
                     safety marings. Safety marging is simular to replication
                     property - it is list of <domain>:<number> pairs.
 *                   For instance, \"rack:1\",\"node:2\".
 * @param out        Populated set of NodeLocationScope, int  pairs.
 *
 * @return           0 on success, or -1 on error
 */
int parseSafetyMargin(const std::vector<std::string>& descriptors,
                      SafetyMargin& out);

// Converts a ReplicationProperty object into SafetyMargin
// TODO: We should replace SafetyMargin type with ReplicationProperty
SafetyMargin
safetyMarginFromReplication(const ReplicationProperty& replication);

/**
 * @param descriptors A list of descriptors, @see parseSafetyMargin.
 * @param out        Populated set of NodeLocationScope, int  pairs.
 *
 * @return           0 on success, or -1 on error
 *
 */
int parseSafetyMargin(const std::string& descriptor, SafetyMargin& out);
}} // namespace facebook::logdevice
