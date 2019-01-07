/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>

namespace facebook { namespace logdevice {

enum class MaintenanceTransitionStatus : uint16_t {
  /*
   * The MaintenanceManager has not started this maintenance transition yet.
   */
  NOT_STARTED = 0,
  STARTED = 1,
  /*
   * MaintenanceManager is waiting for a response from the NodesConfigManager
   * after requesting to apply changes
   */
  AWAITING_STORAGE_STATE_CHANGES = 2,
  /*
   * MaintenanceManager is performing a safety verification to ensure that the
   * operation is safe.
   */
  AWAITING_SAFETY_CHECK_RESULTS = 3,
  /*
   * The internal safety checker deemed this maintenance operation unsafe. The
   * maintenance will remain to be blocked until the next retry of safety check
   * succeeds.
   */
  BLOCKED_UNTIL_SAFE = 4,
  /*
   * MaintenanceManager is waiting for data migration/rebuilding to complete.
   * operation is safe.
   */
  AWAITING_DATA_REBUILDING = 5,
  /*
   * Data migration is blocked because it would lead to data loss if unblocked.
   * If this is required, use the unblockRebuilding to skip lost records and
   * and unblock readers waiting for the permanently lost records to be
   * recovered.
   */
  REBUILDING_IS_BLOCKED = 6,
  /*
   * Maintenance is expecting the node to join the cluster so we can finialize
   * this maintenance transition. MaintenanceManager will keep waiting until the
   * node becomes alive.
   */
  AWAITING_NODE_TO_JOIN = 7,
};

std::string toString(const MaintenanceTransitionStatus& st);

}} // namespace facebook::logdevice
