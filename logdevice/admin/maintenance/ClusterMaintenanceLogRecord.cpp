/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "ClusterMaintenanceLogRecord.h"

namespace facebook { namespace logdevice {

std::string toString(const MaintenanceRecordType& type) {
  switch (type) {
    case MaintenanceRecordType::MAX:
    case MaintenanceRecordType::INVALID:
      break;
    case MaintenanceRecordType::APPLY_MAINTENANCE:
      return "APPLY_MAINTENANCE";
    case MaintenanceRecordType::REMOVE_MAINTENANCE:
      return "REMOVE_MAINTENANCE";
  }
  return "INVALID";
}

bool APPLY_MAINTENANCE_Record::
operator==(const APPLY_MAINTENANCE_Record& other) const {
  return header_ == other.header_ &&
      shard_maintenance_map_ == other.shard_maintenance_map_ &&
      sequencer_maintenance_map_ == other.sequencer_maintenance_map_;
}

bool REMOVE_MAINTENANCE_Record::
operator==(const REMOVE_MAINTENANCE_Record& other) const {
  return header_ == other.header_ &&
      shard_maintenances_ == other.shard_maintenances_ &&
      sequencer_maintenances_ == other.sequencer_maintenances_;
}

}} // namespace facebook::logdevice
