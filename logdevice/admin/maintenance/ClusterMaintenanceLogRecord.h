/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <unordered_map>

#include <folly/Conv.h>
#include <folly/Format.h>
#include <folly/String.h>

#include "logdevice/admin/maintenance/ClusterMaintenanceLogRecord_generated.h"
#include "logdevice/admin/maintenance/ClusterMaintenanceState_generated.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/membership/types.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

enum class MaintenanceRecordType : uint16_t {
  INVALID = 0,
  APPLY_MAINTENANCE,
  REMOVE_MAINTENANCE,
  MAX
};

typedef uint32_t MAINTENANCE_LOG_HEADER_flags_t;

std::string toString(const MaintenanceRecordType& type);

class MaintenanceLogDeltaHeader {
 public:
  explicit MaintenanceLogDeltaHeader()
      : type_(MaintenanceRecordType::INVALID) {}

  std::string user_id_;
  std::string reason_;
  MaintenanceRecordType type_;
  membership::MaintenanceID::Type id_;
  SystemTimestamp created_on_;
  MAINTENANCE_LOG_HEADER_flags_t flags_{0};

  // Flags

  static constexpr MAINTENANCE_LOG_HEADER_flags_t SKIP_SAFETY_CHECK =
      (unsigned)1 << 0;

  std::string describe() const {
    return folly::format(
               "Header[user:{}, reason:{}, type:{}, id{}, created on:{}",
               user_id_,
               reason_,
               toString(type_),
               folly::to<std::string>(id_.val_),
               toString(created_on_))
        .str();
  }

  bool operator==(const MaintenanceLogDeltaHeader& other) const {
    return user_id_ == other.user_id_ && reason_ == other.reason_ &&
        type_ == other.type_ && id_ == other.id_ && flags_ == other.flags_;
  }
};

class MaintenanceLogDeltaRecord {
 public:
  virtual MaintenanceRecordType getType() const = 0;
  virtual std::string describe() const = 0;
  virtual ~MaintenanceLogDeltaRecord() {}
  explicit MaintenanceLogDeltaRecord(MaintenanceLogDeltaHeader header)
      : header_(header) {}

  MaintenanceLogDeltaHeader header_;
};

using ShardOperationalState =
    cluster_maintenance_state::fbuffers::ShardOperationalState;
using SequencingState = cluster_maintenance_state::fbuffers::SequencingState;
using ShardMaintenanceMap =
    std::unordered_map<ShardID, ShardOperationalState, ShardID::Hash>;
using SequencerMaintenanceMap =
    std::unordered_map<node_index_t, SequencingState>;

class APPLY_MAINTENANCE_Record : public MaintenanceLogDeltaRecord {
 public:
  explicit APPLY_MAINTENANCE_Record(
      MaintenanceLogDeltaHeader header,
      ShardMaintenanceMap shard_maintenance_map,
      SequencerMaintenanceMap sequencer_maintenance_map)
      : MaintenanceLogDeltaRecord(header),
        shard_maintenance_map_(std::move(shard_maintenance_map)),
        sequencer_maintenance_map_(std::move(sequencer_maintenance_map)) {}

  MaintenanceRecordType getType() const override {
    return MaintenanceRecordType::APPLY_MAINTENANCE;
  }

  bool operator==(const APPLY_MAINTENANCE_Record& other) const;
  std::string describe() const override {
    return header_.describe();
  }

  ShardMaintenanceMap shard_maintenance_map_;
  SequencerMaintenanceMap sequencer_maintenance_map_;
}; // APPLY_MAINTENANCE_Record

class REMOVE_MAINTENANCE_Record : public MaintenanceLogDeltaRecord {
 public:
  explicit REMOVE_MAINTENANCE_Record(MaintenanceLogDeltaHeader header,
                                     std::set<ShardID> shards,
                                     std::set<node_index_t> nodes)
      : MaintenanceLogDeltaRecord(header),
        shard_maintenances_(std::move(shards)),
        sequencer_maintenances_(std::move(nodes)) {}

  MaintenanceRecordType getType() const override {
    return MaintenanceRecordType::REMOVE_MAINTENANCE;
  }

  bool operator==(const REMOVE_MAINTENANCE_Record& other) const;
  std::string describe() const override {
    return header_.describe();
  }

  std::set<ShardID> shard_maintenances_;
  std::set<node_index_t> sequencer_maintenances_;
}; // REMOVE_MAINTENANCE_Record

}} // namespace facebook::logdevice
