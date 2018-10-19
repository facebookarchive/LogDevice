/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/RebuildingTypes.h"
#include "logdevice/server/rebuilding/RebuildingReadStorageTaskV2.h"

namespace facebook { namespace logdevice {

class ShardRebuildingV2 : public ShardRebuildingInterface {
 public:
  ~ShardRebuildingV2() override;

  void start(std::unordered_map<logid_t, std::unique_ptr<RebuildingPlan>> plan)
      override;
  void advanceGlobalWindow(RecordTimestamp new_window_end) override;
  void noteConfigurationChanged() override;
  void getDebugInfo(InfoShardsRebuildingTable& table) const override;

  void onChunkRebuildingDone(log_rebuilding_id_t chunk_id,
                             RecordTimestamp oldest_timestamp);

 protected:
  WorkerCallbackHelper<ShardRebuildingV2> callbackHelper_;
};

}} // namespace facebook::logdevice
