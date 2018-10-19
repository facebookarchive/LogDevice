/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/rebuilding/ShardRebuildingV2.h"

namespace facebook { namespace logdevice {

ShardRebuildingV2::~ShardRebuildingV2() {}

void ShardRebuildingV2::start(
    std::unordered_map<logid_t, std::unique_ptr<RebuildingPlan>>) {
  // TODO (#T24665001): implement.
}
void ShardRebuildingV2::advanceGlobalWindow(RecordTimestamp) {
  // TODO (#T24665001): implement.
}
void ShardRebuildingV2::noteConfigurationChanged() {
  // TODO (#T24665001): implement.
}
void ShardRebuildingV2::getDebugInfo(InfoShardsRebuildingTable&) const {
  // TODO (#T24665001): implement.
}

void ShardRebuildingV2::onChunkRebuildingDone(log_rebuilding_id_t,
                                              RecordTimestamp) {
  // TODO (#T24665001): implement.
}

}} // namespace facebook::logdevice
