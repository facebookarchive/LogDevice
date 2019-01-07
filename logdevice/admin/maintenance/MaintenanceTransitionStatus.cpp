/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/admin/maintenance/MaintenanceTransitionStatus.h"

namespace facebook { namespace logdevice {

std::string toString(const MaintenanceTransitionStatus& st) {
  switch (st) {
    case MaintenanceTransitionStatus::NOT_STARTED:
      return "NOT_STARTED";
    case MaintenanceTransitionStatus::STARTED:
      return "STARTED";
    case MaintenanceTransitionStatus::AWAITING_STORAGE_STATE_CHANGES:
      return "AWAITING_STORAGE_STATE_CHANGES";
    case MaintenanceTransitionStatus::AWAITING_SAFETY_CHECK_RESULTS:
      return "AWAITING_SAFETY_CHECK_RESULTS";
    case MaintenanceTransitionStatus::BLOCKED_UNTIL_SAFE:
      return "BLOCKED_UNTIL_SAFE";
    case MaintenanceTransitionStatus::AWAITING_DATA_REBUILDING:
      return "AWAITING_DATA_REBUILDING";
    case MaintenanceTransitionStatus::REBUILDING_IS_BLOCKED:
      return "REBUILDING_IS_BLOCKED";
    case MaintenanceTransitionStatus::AWAITING_NODE_TO_JOIN:
      return "AWAITING_NODE_TO_JOIN";
    default:
      return "INVALID";
  }
}

}} // namespace facebook::logdevice
