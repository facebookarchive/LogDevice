/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "logdevice/common/replicated_state_machine/RsmSnapshotStoreFactory.h"

namespace facebook { namespace logdevice {

class RsmServerSnapshotStoreFactory {
 public:
  static std::unique_ptr<RSMSnapshotStore>
  create(Processor* processor,
         SnapshotStoreType snapshot_store_type,
         bool /* unused */,
         std::string key) {
    ld_info("Attempting to create snapshot store (type:%d, key:%s)",
            static_cast<int>(snapshot_store_type),
            key.c_str());
    if (snapshot_store_type == SnapshotStoreType::LOCAL_STORE) {
      /* TODO will be supported with Local store Implementation */
      return nullptr;
    } else {
      return RsmSnapshotStoreFactory::create(
          processor, snapshot_store_type, key, true /* server */);
    }
  }
};
}} // namespace facebook::logdevice
